package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/mattermost/morph/drivers"
)

// Mutex is similar to sync.Mutex, except usable by morph to lock the db.
//
// Pick a unique name for each mutex your plugin requires.
//
// A Mutex must not be copied after first use.
type Mutex struct {
	noCopy // nolint:unused
	key    string

	// lock guards the variables used to manage the refresh task, and is not itself related to
	// the db lock.
	lock        sync.Mutex
	stopRefresh chan bool
	refreshDone chan bool
	conn        *sql.Conn

	logger drivers.Logger
}

// NewMutex creates a mutex with the given key name.
//
// returns error if key is empty.
func (pg *Postgres) NewMutex(key string, logger drivers.Logger) (drivers.Locker, error) {
	key, err := drivers.MakeLockKey(key)
	if err != nil {
		return nil, fmt.Errorf("make lock key: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), drivers.TTL)
	defer cancel()

	conn, err := pg.db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("get db conn: %w", err)
	}

	createTableIfNotExistsQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id varchar(64) PRIMARY KEY, expireat bigint);", drivers.MutexTableName)
	for attempt := 0; ; attempt++ {
		if _, err = conn.ExecContext(ctx, createTableIfNotExistsQuery); err == nil {
			break
		}

		if isRetryableDDLConflict(err) && attempt < 7 {
			time.Sleep(time.Duration(attempt+1) * 150 * time.Millisecond)
			logger.Println("Morph: retrying DDL conflict (create db_lock)")
			continue
		}

		return nil, fmt.Errorf("create table db_lock: %w", err)
	}

	return &Mutex{
		key:    key,
		conn:   conn,
		logger: logger,
	}, nil
}

func isRetryableDDLConflict(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		if strings.HasPrefix(string(pqErr.Code), "40") {
			return true
		}
	}

	var stateErr interface {
		SQLState() string
	}
	if errors.As(err, &stateErr) {
		if strings.HasPrefix(stateErr.SQLState(), "40") {
			return true
		}
	}

	return strings.Contains(strings.ToLower(err.Error()), "tuple concurrently updated")
}

// lock makes a single attempt to lock the mutex, returning true only if successful.
func (m *Mutex) tryLock(ctx context.Context) (bool, error) {
	m.logger.Println("Morph: trying to acquire lock")

	now := time.Now()

	tx, err := m.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer m.finalizeTx(tx)

	query := fmt.Sprintf("INSERT INTO %s (id, expireat) VALUES ($1, $2)", drivers.MutexTableName)

	if _, err := tx.Exec(query, m.key, now.Add(drivers.TTL).Unix()); err != nil {
		m.finalizeTx(tx)

		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			m.logger.Println("Morph: DB is locked, going to try acquire the lock if it is expired.")
		}

		err2 := m.releaseLock(ctx, now)
		if err2 == nil { // lock has been released due to expiration
			m.logger.Println("Morph: lock has been released due to expiration")
			return true, nil
		} else {
			m.logger.Printf("Morph: failed to release lock: %v", err2)
		}

		return false, fmt.Errorf("failed to lock mutex: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}

	return true, nil
}

func (m *Mutex) releaseLock(ctx context.Context, t time.Time) error {
	tx, err := m.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer m.finalizeTx(tx)

	e, err := m.getExpireAt(tx)
	if err != nil {
		return fmt.Errorf("fetch expireat: %w", err)
	}

	if t.Unix() < e {
		return errors.New("could not release the lock")
	}

	query := fmt.Sprintf("UPDATE %s SET expireat = $1 WHERE id = $2", drivers.MutexTableName)
	if err = executeTx(tx, query, t.Add(drivers.TTL).Unix(), m.key); err != nil {
		return fmt.Errorf("unable to set new expireat for mutex: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("unable to set new expireat for mutex: %w", err)
	}

	m.logger.Println("Morph: lock released successfully")

	return nil
}

func (m *Mutex) getExpireAt(tx *sql.Tx) (int64, error) {
	var expireAt int64

	query := fmt.Sprintf("SELECT expireat FROM %s WHERE id = $1 FOR UPDATE", drivers.MutexTableName)

	if err := tx.QueryRow(query, m.key).Scan(&expireAt); err != nil {
		return -1, fmt.Errorf("failed to fetch mutex from db: %w", err)
	}

	return expireAt, nil
}

// refreshLock rewrites the lock key value with a new expiry, returning nil only if successful.
func (m *Mutex) refreshLock(ctx context.Context) error {
	tx, err := m.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer m.finalizeTx(tx)

	e, err := m.getExpireAt(tx)
	if err != nil {
		return fmt.Errorf("fetch expireat: %w", err)
	}

	tmp := time.Unix(e, 0)

	query := fmt.Sprintf("UPDATE %s SET expireat = $1 WHERE id = $2", drivers.MutexTableName)

	if err = executeTx(tx, query, tmp.Add(drivers.TTL).Unix(), m.key); err != nil {
		return fmt.Errorf("unable to refresh expireat for mutex: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("unable to refresh expireat for mutex: %w", err)
	}

	m.logger.Println("Morph: lock refreshed successfully")

	return nil
}

// Lock locks m unless the context is canceled. If the mutex is already locked by any other
// instance, including the current one, the calling goroutine blocks until the mutex can be locked,
// or the context is canceled.
//
// The mutex is locked only if a nil error is returned.
func (m *Mutex) Lock(ctx context.Context) error {
	var waitInterval time.Duration

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitInterval):
			m.logger.Println("Morph: wait interval")
		}

		ok, err := m.tryLock(ctx)
		if err != nil || !ok {
			m.logger.Printf("Morph: Failed to acquire lock. Trying again: %v\n", err)
			waitInterval = drivers.NextWaitInterval(waitInterval, err)
			continue
		}

		break
	}

	stop := make(chan bool)
	done := make(chan bool)

	go func() {
		defer close(done)
		t := time.NewTicker(drivers.RefreshInterval)
		for {
			select {
			case <-t.C:
				if err := m.refreshLock(ctx); err != nil {
					m.logger.Println("Morph: refresh lock failed: ", err.Error())
					return
				}
			case <-stop:
				return
			}
		}
	}()

	m.lock.Lock()
	m.stopRefresh = stop
	m.refreshDone = done
	m.lock.Unlock()

	m.logger.Println("Morph: got DB lock")

	return nil
}

// Unlock unlocks m. It is a run-time error if m is not locked on entry to Unlock.
//
// Just like sync.Mutex, a locked Lock is not associated with a particular goroutine or a process.
func (m *Mutex) Unlock() error {
	m.lock.Lock()
	if m.stopRefresh == nil {
		m.lock.Unlock()
		panic("mutex has not been acquired")
	}

	close(m.stopRefresh)
	m.stopRefresh = nil
	<-m.refreshDone
	m.lock.Unlock()

	defer m.conn.Close()

	// If an error occurs deleting, the mutex will still expire, allowing later retry.
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", drivers.MutexTableName)

	if _, err := m.conn.ExecContext(context.Background(), query, m.key); err != nil {
		return fmt.Errorf("unable to delete mutex: %w", err)
	}

	return nil
}

func executeTx(tx *sql.Tx, query string, args ...interface{}) error {
	_, err := tx.Exec(query, args...)
	return err
}

func (m *Mutex) finalizeTx(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
		m.logger.Printf("Morph: failed to rollback transaction: %s", err)
	}
}

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{} // nolint:unused

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {} // nolint:unused
