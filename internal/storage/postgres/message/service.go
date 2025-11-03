package message

import (
	"context"
	"database/sql"
	"db-worker/internal/config"
	"db-worker/internal/config/operation"
	"errors"
	"fmt"
	"sync"

	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/logrusadapter"
	"github.com/sirupsen/logrus"

	_ "github.com/lib/pq" // postgres driver
)

// Repo сохраняет сообщения и результаты их обработки в базу данных.
type Repo struct {
	addr  string
	db    *sql.DB
	name  string
	table string

	cfg *config.Postgres

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

// WithCfg устанавливает конфигурацию базы данных.
func WithCfg(cfg *config.Postgres) RepoOption {
	return func(r *Repo) {
		r.cfg = cfg
	}
}

// WithTable устанавливает имя таблицы.
func WithTable(table string) RepoOption {
	return func(r *Repo) {
		r.table = table
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

	if r.cfg == nil {
		return nil, errors.New("config is required")
	}

	if r.table == "" {
		return nil, errors.New("table is required")
	}

	db, err := sql.Open("postgres", r.addr)
	if err != nil {
		return nil, fmt.Errorf("connect open a db driver: %w", err)
	}

	logger := logrus.New()
	logger.Level = logrus.DebugLevel           // miminum level
	logger.Formatter = &logrus.JSONFormatter{} // logrus automatically add time field

	drv := db.Driver()

	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("close raw db: %w", err)
	}

	db = sqldblogger.OpenDriver(r.addr, drv, logrusadapter.New(logger) /*, using_default_options*/) // db is STILL *sql.DB

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
		"host": db.cfg.Host,
		"port": db.cfg.Port,
		"name": db.name,
	}).Info("successfully connected postgres")

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
	return db.table
}

// Host возвращает хост базы данных.
func (db *Repo) Host() string {
	return db.cfg.Host
}

// User возвращает пользователя для подключения к базе данных.
func (db *Repo) User() string {
	return db.cfg.User
}

// Password возвращает пароль для подключения к базе данных.
func (db *Repo) Password() string {
	return db.cfg.Password
}

// DBName возвращает имя базы данных.
func (db *Repo) DBName() string {
	return db.cfg.DBName
}

// Queue возвращает очередь.
// Не реализовано в этом драйвере, возвращает пустую строку.
func (db *Repo) Queue() string {
	return "" // not implemented in this driver
}

// RoutingKey возвращает ключ маршрутизации.
// Не реализовано в этом драйвере, возвращает пустую строку.
func (db *Repo) RoutingKey() string {
	return "" // not implemented in this driver
}

// InsertTimeout возвращает время ожидания вставки.
func (db *Repo) InsertTimeout() int {
	return db.insertTimeout
}

// ReadTimeout возвращает время ожидания чтения.
func (db *Repo) ReadTimeout() int {
	return db.readTimeout
}

// Port возвращает порт базы данных.
func (db *Repo) Port() int {
	return db.cfg.Port
}
