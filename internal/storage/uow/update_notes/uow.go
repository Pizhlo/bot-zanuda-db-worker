package uow

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"db-worker/internal/model"
	interfaces "db-worker/internal/service/message/interface"

	"github.com/sirupsen/logrus"
)

// noteRepo определяет интерфейс для работы с заметками
type noteRepo interface {
	repo
	UpdateNotes(ctx context.Context, id string, notes []interfaces.Message) error
}

type repo interface {
	BeginTx(ctx context.Context, id string) error
	Commit(ctx context.Context, id string) error
	Rollback(ctx context.Context, id string) error
	Close()
}

type transactionRepo interface {
	repo
	// CreateTx создает запись о транзакции в БД со статусом "in progress".
	CreateTx(ctx context.Context, id string, notes []interfaces.Message) error
	// SaveResult сохраняет результат транзакции в БД.
	SaveResult(ctx context.Context, id string, result model.Result) error
}

// UnitOfWork реализует интерфейс UnitOfWork.
type UnitOfWork struct {
	mu sync.Mutex

	Repos struct {
		TxRepo   transactionRepo
		Postgres noteRepo
	}

	transactions map[string]struct{}
}

type UnitOfWorkOption func(*UnitOfWork)

func WithPostgres(postgres noteRepo) UnitOfWorkOption {
	return func(uow *UnitOfWork) {
		uow.Repos.Postgres = postgres
	}
}

func WithTxRepo(txRepo transactionRepo) UnitOfWorkOption {
	return func(uow *UnitOfWork) {
		uow.Repos.TxRepo = txRepo
	}
}

// NewUnitOfWork создает новый экземпляр UnitOfWork.
func NewUnitOfWork(opts ...UnitOfWorkOption) (*UnitOfWork, error) {
	uow := &UnitOfWork{}

	for _, opt := range opts {
		opt(uow)
	}

	if uow.Repos.Postgres == nil {
		return nil, errors.New("postgres repo is required")
	}

	if uow.Repos.TxRepo == nil {
		return nil, errors.New("tx repo is required")
	}

	uow.transactions = make(map[string]struct{})

	return uow, nil
}

func (uow *UnitOfWork) Begin(ctx context.Context, id string) error {
	uow.mu.Lock()
	defer uow.mu.Unlock()

	logrus.Debugf("UOW: beginning transaction %s", id)

	if _, ok := uow.transactions[id]; ok {
		return errors.New("transaction already exists")
	}

	// не создаем транзкацию в TxSaver, т.к. у него свои, несвязанные транзакции

	err := uow.Repos.Postgres.BeginTx(ctx, id)
	if err != nil {
		return fmt.Errorf("UOW: failed to begin transaction: %w", err)
	}

	uow.transactions[id] = struct{}{}

	return nil
}

func (uow *UnitOfWork) Commit(ctx context.Context, id string) error {
	uow.mu.Lock()
	defer uow.mu.Unlock()

	logrus.Debugf("UOW: committing transaction %s", id)

	if _, ok := uow.transactions[id]; !ok {
		return errors.New("transaction not found")
	}

	err := uow.Repos.Postgres.Commit(ctx, id)
	if err != nil {
		return fmt.Errorf("UOW: failed to commit postgres: %w", err)
	}

	// Сохраняем результат транзакции в БД
	if err := uow.Repos.TxRepo.SaveResult(ctx, id, model.Result{Status: model.TxStatusSuccess}); err != nil {
		return fmt.Errorf("UOW: failed to save result to db: %v. Tx id: %s", err, id)
	}

	delete(uow.transactions, id)

	logrus.Debugf("UOW: transaction %s committed", id)

	return nil
}

func (uow *UnitOfWork) Rollback(ctx context.Context, id string, theErr error) error {
	uow.mu.Lock()
	defer uow.mu.Unlock()

	logrus.Debugf("UOW: rolling back transaction %s", id)

	if _, ok := uow.transactions[id]; !ok {
		return errors.New("transaction not found")
	}

	err := uow.Repos.Postgres.Rollback(ctx, id)
	if err != nil {
		return fmt.Errorf("UOW: failed to rollback postgres: %w", err)
	}

	txErr := uow.Repos.TxRepo.SaveResult(ctx, id, model.Result{Status: model.TxStatusFailed, Error: theErr.Error()})
	if txErr != nil {
		return fmt.Errorf("UOW: failed to save result to db: %v. Tx id: %s", txErr, id)
	}

	delete(uow.transactions, id)

	return nil
}

func (uow *UnitOfWork) Close() {
	uow.Repos.Postgres.Close()
	uow.Repos.TxRepo.Close()
}
