package uow

import (
	"db-worker/internal/storage"
	"db-worker/pkg/random"
	"fmt"

	"github.com/sirupsen/logrus"
)

type transaction struct {
	id     string
	status txStatus
	// если транзакция не успешна, то в этом поле будет название драйвера, который не успел выполниться.
	// это нужно, чтобы остальные драйвера могли откатиться.
	failedDriver string

	requests map[storage.Driver]*storage.Request
}

type txStatus string

const (
	txStatusInProgress txStatus = "in progress"
	txStatusSuccess    txStatus = "success"
	txStatusFailed     txStatus = "failed"
)

// beginTx начинает транзакцию.
func (s *Service) beginTx(requests map[storage.Driver]*storage.Request) *transaction {
	id := random.String(10)

	logrus.WithFields(logrus.Fields{
		"transaction_id": id,
		"operation":      s.cfg.Name,
		"service":        "uow",
	}).Info("beginning transaction")

	tx := &transaction{
		id:       id,
		status:   txStatusInProgress,
		requests: requests,
	}

	s.transactions[id] = tx

	return tx
}

// finishTx завершает транзакцию.
// Транзакция не должна быть в статусе in progress.
// В случае успеха удаляет транзакцию из map.
func (s *Service) finishTx(tx *transaction) error {
	// либо успешная, либо неудачная транзакция
	if tx.isInProgress() {
		return fmt.Errorf("transaction status equal to: %q, but expected: %q or %q", txStatusInProgress, txStatusSuccess, txStatusFailed)
	}

	delete(s.transactions, tx.id)

	logrus.WithFields(logrus.Fields{
		"transaction_id": tx.id,
		"operation":      s.cfg.Name,
		"service":        "uow",
	}).Info("transaction finished")

	return nil
}

func (tx *transaction) setFailedDriver(driver string) {
	tx.failedDriver = driver
}

func (tx *transaction) setStatus(status txStatus) {
	tx.status = status
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
