package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/simukti/sqldb-logger/logadapter/logrusadapter"
	"github.com/sirupsen/logrus"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // postgres driver
	_ "github.com/golang-migrate/migrate/v4/source/file"       // for loading migrations from file
)

// Repo реализует загрузку системных миграций.
type Repo struct {
	addr string
	db   *sql.DB

	insertTimeout int
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

// New создает новый репозиторий.
func New(ctx context.Context, opts ...RepoOption) (*Repo, error) {
	r := &Repo{}

	for _, opt := range opts {
		opt(r)
	}

	if r.insertTimeout == 0 {
		return nil, fmt.Errorf("insert timeout is required")
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
	logger.Formatter = &logrus.TextFormatter{} // logrus automatically add time field

	drv := db.Driver()

	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("close raw db: %w", err)
	}

	db = sqldblogger.OpenDriver(r.addr, drv, logrusadapter.New(logger) /*, using_default_options*/) // db is STILL *sql.DB

	r.db = db

	return r, nil
}

// Stop закрывает репозиторий.
func (db *Repo) Stop(_ context.Context) error {
	return db.db.Close()
}

// Run запускает репозиторий.
func (db *Repo) Run(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)
	defer cancel()

	if err := db.db.PingContext(timeoutCtx); err != nil {
		return fmt.Errorf("error pinging db: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"addr": db.addr,
	}).Info("successfully connected postgres")

	return nil
}

// Load загружает миграции.
func (db *Repo) Load(ctx context.Context) error {
	m, err := migrate.New(
		"file://migration",
		db.addr)
	if err != nil {
		return fmt.Errorf("error creating migrate: %w", err)
	}

	defer func() {
		if _, err := m.Close(); err != nil {
			logrus.WithError(err).Error("error closing migrate instance")
		}
	}()

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			version, dirty, err := m.Version()
			if err != nil {
				return fmt.Errorf("error getting version: %w", err)
			}

			logrus.WithFields(logrus.Fields{
				"addr":    db.addr,
				"version": version,
				"dirty":   dirty,
			}).Info("migrations loaded: no change")

			return nil
		}

		return fmt.Errorf("error migrating up: %w", err)
	}

	version, dirty, err := m.Version()
	if err != nil {
		return fmt.Errorf("error getting version: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"addr":    db.addr,
		"version": version,
		"dirty":   dirty,
	}).Info("migrations loaded")

	return nil
}
