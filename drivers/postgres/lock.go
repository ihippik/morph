package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

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
func (pg *Postgres) NewMutex(key string, logger drivers.Logger) (*Mutex, error) {
	key, err := drivers.MakeLockKey(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), drivers.TTL)
	defer cancel()

	conn, err := pg.db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	createTableIfNotExistsQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id varchar(64) PRIMARY KEY, expireat bigint);",
		drivers.MutexTableName,
	)
	if _, err = conn.ExecContext(ctx, createTableIfNotExistsQuery); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create mutex table: %w", err)
	}

	return &Mutex{
		key:    key,
		conn:   conn,
		logger: logger,
	}, nil
}

// tryLock makes a single attempt to lock the mutex, returning true only if successful.
func (m *Mutex) tryLock(ctx context.Context) (bool, error) {
	now := time.Now().Unix()
	expireAt := time.Now().Add(drivers.TTL).Unix()

	query := fmt.Sprintf(`
		INSERT INTO %s (id, expireat)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE
		SET expireat = EXCLUDED.expireat
		WHERE %s.expireat < $3
	`, drivers.MutexTableName, drivers.MutexTableName)

	res, err := m.conn.ExecContext(ctx, query, m.key, expireAt, now)
	if err != nil {
		return false, fmt.Errorf("failed to lock mutex: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to inspect lock result: %w", err)
	}

	m.logger.Println("Morph: attempt to lock mutex rows=", rows)

	return rows == 1, nil
}

// refreshLock rewrites the lock key value with a new expiry, returning nil only if successful.
func (m *Mutex) refreshLock(ctx context.Context) error {
	now := time.Now().Unix()
	newExpireAt := time.Now().Add(drivers.TTL).Unix()

	query := fmt.Sprintf(`
		UPDATE %s
		SET expireat = $1
		WHERE id = $2 AND expireat >= $3
	`, drivers.MutexTableName)

	res, err := m.conn.ExecContext(ctx, query, newExpireAt, m.key, now)
	if err != nil {
		return fmt.Errorf("unable to refresh expireat for mutex: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to inspect refresh result: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("lock is no longer held or already expired")
	}

	m.logger.Println("lock was refreshed id=", m.key)

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
		}

		ok, err := m.tryLock(ctx)
		if err != nil || !ok {
			m.logger.Printf("Morph: failed to acquire lock. Trying again: %v\n", err)
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
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if err := m.refreshLock(ctx); err != nil {
					m.logger.Printf("Morph: Failed to refresh lock: %v", err)
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

	return nil
}

// Unlock unlocks m. It is a run-time error if m is not locked on entry to Unlock.
//
// Just like sync.Mutex, a locked Lock is not associated with a particular goroutine or a process.
func (m *Mutex) Unlock() error {
	m.lock.Lock()
	if m.stopRefresh == nil {
		m.lock.Unlock()
		panic("morph: mutex has not been acquired")
	}

	close(m.stopRefresh)
	m.stopRefresh = nil
	<-m.refreshDone
	m.lock.Unlock()

	defer m.conn.Close()

	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", drivers.MutexTableName)
	_, err := m.conn.ExecContext(context.Background(), query, m.key)
	return err
}

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{} // nolint:unused

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {} // nolint:unused
