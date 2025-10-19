package uow

import (
	"context"
	"db-worker/internal/storage"
	"db-worker/pkg/random"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
)

type transaction struct {
	id     string
	status txStatus
	// если транзакция не успешна, то в этом поле будет название драйвера, который не успел выполниться.
	// это нужно, чтобы остальные драйвера могли откатиться.
	failedDriver string
	// если транзакция не успешна, то в этом поле будет ошибка, которая произошла при выполнении транзакции.
	err error

	requests map[storage.Driver]*storage.Request
	begun    map[string]struct{} // драйвера, в которых транзакция была начата.
}

type txStatus string

const (
	txStatusInProgress txStatus = "in progress"
	txStatusSuccess    txStatus = "success"
	txStatusFailed     txStatus = "failed"
)

// beginTx начинает транзакцию.
func (s *Service) beginTx(ctx context.Context, requests map[storage.Driver]*storage.Request) (*transaction, error) {
	id := random.String(10)

	logrus.WithFields(logrus.Fields{
		"transaction_id":           id,
		"operation":                s.cfg.Name,
		"service":                  "uow",
		"transaction_requests_num": len(requests),
	}).Info("beginning transaction")

	tx := &transaction{
		id:       id,
		status:   txStatusInProgress,
		requests: requests,
		begun:    make(map[string]struct{}),
	}

	s.mu.Lock()
	s.transactions[id] = tx
	s.mu.Unlock()

	for driver := range requests {
		err := driver.Begin(ctx, id)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": id,
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          err,
			}).Error("failed to begin transaction in driver")

			tx.setFailedStatus(driver.Name(), err)

			// если не удалось начать транзакцию в одном из драйверов, то завершаем транзакцию.

			finishErr := s.finishTx(ctx, tx)
			if finishErr != nil {
				logrus.WithFields(logrus.Fields{
					"transaction_id": id,
					"operation":      s.cfg.Name,
					"service":        "uow",
					"driver":         driver.Name(),
					"error":          finishErr,
				}).Error("failed to finish transaction in driver after failed to begin transaction")

				return nil, fmt.Errorf("failed to begin transaction in driver %q: %w (also failed to finish: %v)", driver.Name(), err, finishErr)
			}

			return nil, fmt.Errorf("failed to begin transaction in driver %q: %w", driver.Name(), err)
		}

		tx.begun[driver.Name()] = struct{}{}
	}

	return tx, nil
}

// finishTx завершает транзакцию.
// Транзакция не должна быть в статусе in progress.
// В случае успеха удаляет транзакцию из map.
func (s *Service) finishTx(ctx context.Context, tx *transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// либо успешная, либо неудачная транзакция
	if tx.isInProgress() {
		return fmt.Errorf("transaction status equal to: %q, but expected: %q or %q", txStatusInProgress, txStatusSuccess, txStatusFailed)
	}

	var errs []error

	for driver := range tx.requests {
		if driver.Name() == tx.failedDriver {
			continue
		}

		if _, ok := tx.begun[driver.Name()]; !ok {
			continue
		}

		err := driver.FinishTx(ctx, tx.id)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"transaction_id": tx.id,
				"operation":      s.cfg.Name,
				"service":        "uow",
				"driver":         driver.Name(),
				"error":          err,
			}).Error("failed to finish transaction in driver")

			errs = append(errs, err)
		}
	}

	delete(s.transactions, tx.id)

	logrus.WithFields(logrus.Fields{
		"transaction_id":            tx.id,
		"operation":                 s.cfg.Name,
		"service":                   "uow",
		"transaction_requests_num":  len(tx.requests),
		"transaction_failed_driver": tx.failedDriver,
		"transaction_error":         tx.err,
		"transaction_status":        tx.status,
	}).Info("transaction finished")

	if len(errs) > 0 {
		return fmt.Errorf("failed to finish transaction in drivers: %w", errors.Join(errs...))
	}

	return nil
}

func (tx *transaction) setFailedDriver(driver string) {
	tx.failedDriver = driver
}

func (tx *transaction) setStatus(status txStatus) {
	tx.status = status
}

func (tx *transaction) setFailedStatus(driver string, err error) {
	tx.setFailedDriver(driver)
	tx.setStatus(txStatusFailed)
	tx.err = err
}

func (tx *transaction) setSuccessStatus() {
	tx.setStatus(txStatusSuccess)
}

func (tx *transaction) isInProgress() bool {
	return tx.isEqualStatus(txStatusInProgress)
}

func (tx *transaction) isFailed() bool {
	return tx.isEqualStatus(txStatusFailed)
}

func (tx *transaction) isEqualStatus(status txStatus) bool {
	return tx.status == status
}
