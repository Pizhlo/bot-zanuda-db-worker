package transaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/logrusadapter"
	"github.com/sirupsen/logrus"

	_ "github.com/lib/pq" // postgres driver
)

// Repo сохраняет результаты выполнения транзакции в базу данных.
type Repo struct {
	addr       string
	db         *sql.DB
	instanceID int

	insertTimeout int
	readTimeout   int

	transaction struct {
		mu sync.Mutex
		tx map[string]*sql.Tx
	}
}

// RepoOption определяет опции для репозитория.
type RepoOption func(*Repo)

// WithAddr устанавливает адрес базы данных.
func WithAddr(addr string) RepoOption {
	return func(r *Repo) {
		r.addr = addr
	}
}

// WithInsertTimeout устанавливает время ожидания вставки.
func WithInsertTimeout(insertTimeout int) RepoOption {
	return func(c *Repo) {
		c.insertTimeout = insertTimeout
	}
}

// WithReadTimeout устанавливает время ожидания чтения.
func WithReadTimeout(readTimeout int) RepoOption {
	return func(c *Repo) {
		c.readTimeout = readTimeout
	}
}

// WithInstanceID устанавливает идентификатор экземпляра.
func WithInstanceID(instanceID int) RepoOption {
	return func(c *Repo) {
		c.instanceID = instanceID
	}
}

// New создает новый репозиторий.
func New(ctx context.Context, opts ...RepoOption) (*Repo, error) {
	r := &Repo{}

	for _, opt := range opts {
		opt(r)
	}

	if r.insertTimeout == 0 {
		return nil, fmt.Errorf("insert timeout is required")
	}

	if r.readTimeout == 0 {
		return nil, fmt.Errorf("read timeout is required")
	}

	if r.addr == "" {
		return nil, errors.New("addr is required")
	}

	if r.instanceID == 0 {
		return nil, fmt.Errorf("instance id is required")
	}

	db, err := sql.Open("postgres", r.addr)
	if err != nil {
		return nil, fmt.Errorf("connect open a db driver: %w", err)
	}

	logger := logrus.New()
	logger.Level = logrus.DebugLevel           // miminum level
	logger.Formatter = &logrus.JSONFormatter{} // logrus automatically add time field

	db = sqldblogger.OpenDriver(r.addr, db.Driver(), logrusadapter.New(logger))

	err = db.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to a db: %w", err)
	} // to check connectivity and DSN correctness

	r.transaction = struct {
		mu sync.Mutex
		tx map[string]*sql.Tx
	}{mu: sync.Mutex{}, tx: make(map[string]*sql.Tx)}

	r.db = db

	return r, nil
}

// Close закрывает репозиторий.
func (db *Repo) Close() {
	if err := db.db.Close(); err != nil {
		logrus.Errorf("error on closing space repo: %v", err)
	}
}

// BeginTx начинает транзакцию.
func (db *Repo) BeginTx(ctx context.Context, id string) error {
	if _, err := db.getTx(id); err == nil {
		return nil
	}

	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error on begin transaction: %w", err)
	}

	db.transaction.tx[id] = tx

	return nil
}

// func (db *Repo) getOrCreateTx(ctx context.Context, id string) (*sql.Tx, error) {
// 	tx, err := db.getTx(id)
// 	if err == nil {
// 		return tx, nil
// 	}

// 	tx, err = db.db.BeginTx(ctx, nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("error on begin transaction: %w", err)
// 	}

// 	db.transaction.tx[id] = tx

// 	return tx, nil
// }

func (db *Repo) getTx(id string) (*sql.Tx, error) {
	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	tx, ok := db.transaction.tx[id]
	if !ok {
		return nil, fmt.Errorf("transaction not found")
	}

	return tx, nil
}

// Commit коммитит транзакцию.
func (db *Repo) Commit(ctx context.Context, id string) error {
	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	err := db.transaction.tx[id].Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	delete(db.transaction.tx, id)

	return nil
}

// Rollback откатывает транзакцию.
func (db *Repo) Rollback(ctx context.Context, id string) error {
	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	err := db.transaction.tx[id].Rollback()
	if err != nil {
		return fmt.Errorf("error rolling back transaction: %w", err)
	}

	delete(db.transaction.tx, id)

	return nil
}
