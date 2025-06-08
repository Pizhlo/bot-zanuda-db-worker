package uow

import (
	"context"
	"fmt"

	interfaces "db-worker/internal/service/message/interface"
	"db-worker/pkg/random"

	"github.com/sirupsen/logrus"
)

func (s *UnitOfWork) SaveNotes(ctx context.Context, notes []interfaces.Message) {
	id := random.String(10)

	// Начинаем транзакцию
	if err := s.Begin(ctx, id); err != nil {
		logrus.Errorf("UOW: failed to begin transaction: %v", err)

		return
	}

	// Сохраняем транзакцию в БД
	if err := s.Repos.TxRepo.CreateTx(ctx, id, notes); err != nil {
		logrus.Errorf("UOW: failed to save transaction to db: %v", err)

		return
	}

	// Сохраняем в PostgreSQL используя существующий репозиторий
	if err := s.execWithRollback(ctx, id, func() error {
		return s.Repos.Postgres.SaveNotes(ctx, id, notes)
	}); err != nil {
		logrus.Errorf("UOW: failed to save to postgres: %v", err)

		return
	}

	// Если всё прошло успешно, коммитим транзакцию
	if err := s.execWithRollback(ctx, id, func() error {
		return s.Commit(ctx, id)
	}); err != nil {
		logrus.Errorf("UOW: failed to commit transaction %s: %v. Tx id: %s", id, err, id)

		return
	}
}

func (s *UnitOfWork) execWithRollback(ctx context.Context, id string, fn func() error) error {
	if err := fn(); err != nil {
		rbErr := s.Rollback(ctx, id, err)
		if rbErr != nil {
			return fmt.Errorf("UOW: failed to rollback transaction %s: %v", id, rbErr)
		}

		return fmt.Errorf("UOW: failed to exec with rollback: %w", err)
	}

	return nil
}
