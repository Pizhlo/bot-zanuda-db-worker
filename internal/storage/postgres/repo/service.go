package postgres

import (
	"context"
	"database/sql"
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
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
	addr string
	db   *sql.DB
	name string

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

// WithName устанавливает имя репозитория.
func WithName(name string) RepoOption {
	return func(r *Repo) {
		r.name = name
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

	if r.name == "" {
		return nil, fmt.Errorf("name is required")
	}

	if r.addr == "" {
		return nil, errors.New("addr is required")
	}

	db, err := sql.Open("postgres", r.addr)
	if err != nil {
		return nil, fmt.Errorf("connect open a db driver: %w", err)
	}

	logger := logrus.New()
	logger.Level = logrus.DebugLevel           // miminum level
	logger.Formatter = &logrus.JSONFormatter{} // logrus automatically add time field

	db = sqldblogger.OpenDriver(r.addr, db.Driver(), logrusadapter.New(logger) /*, using_default_options*/) // db is STILL *sql.DB

	r.transaction = struct {
		mu sync.Mutex
		tx map[string]*sql.Tx
	}{mu: sync.Mutex{}, tx: make(map[string]*sql.Tx)}

	r.db = db

	return r, nil
}

// Stop закрывает репозиторий.
func (db *Repo) Stop(_ context.Context) error {
	return db.db.Close()
}

// Run запускает репозиторий.
func (db *Repo) Run(ctx context.Context) error {
	if err := db.db.PingContext(ctx); err != nil {
		return fmt.Errorf("error pinging db: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"addr": db.addr,
		"name": db.name,
	}).Info("successfully connected postgres")

	return nil
}

// Begin начинает транзакцию.
func (db *Repo) Begin(ctx context.Context, id string) error {
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

	tx, ok := db.transaction.tx[id]
	if !ok {
		return fmt.Errorf("transaction not found")
	}

	err := tx.Commit()
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

// FinishTx завершает транзакцию без коммита (cleanup при неуспехе begin в другом драйвере).
func (db *Repo) FinishTx(ctx context.Context, id string) error {
	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	tx, ok := db.transaction.tx[id]
	if !ok {
		return nil // ничего не делаем — уже очищено (либо commit, либо rollback)
	}

	// Игнорируем ошибку Rollback — цель: гарантированно освободить ресурсы
	_ = tx.Rollback()
	delete(db.transaction.tx, id)

	return nil
}

// Exec выполняет запрос.
func (db *Repo) Exec(ctx context.Context, req *storage.Request, id string) error {
	tx, err := db.getTx(id)
	if err != nil {
		return fmt.Errorf("error getting transaction: %w", err)
	}

	sql, ok := req.Val.(string)
	if !ok {
		return fmt.Errorf("request value is not a string")
	}

	args, ok := req.Args.([]any)
	if !ok {
		return fmt.Errorf("request arguments are not a slice of any")
	}

	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}

// Name возвращает имя репозитория.
func (db *Repo) Name() string {
	return db.name
}

// Type возвращает тип хранилища (PostgreSQL).
func (db *Repo) Type() operation.StorageType {
	return operation.StorageTypePostgres
}

// Table возвращает имя таблицы.
func (db *Repo) Table() string {
	return ""
}

// Host возвращает адрес хоста.
func (db *Repo) Host() string {
	return db.addr
}

// Port возвращает порт.
func (db *Repo) Port() int {
	return 0
}

// User возвращает имя пользователя.
func (db *Repo) User() string {
	return ""
}

// Password возвращает пароль.
func (db *Repo) Password() string {
	return ""
}

// DBName возвращает имя базы данных.
func (db *Repo) DBName() string {
	return ""
}

// Queue возвращает имя очереди.
func (db *Repo) Queue() string {
	return ""
}

// RoutingKey возвращает ключ маршрутизации.
func (db *Repo) RoutingKey() string {
	return ""
}

// InsertTimeout возвращает время ожидания вставки.
func (db *Repo) InsertTimeout() int {
	return db.insertTimeout
}

// ReadTimeout возвращает время ожидания чтения.
func (db *Repo) ReadTimeout() int {
	return db.readTimeout
}
