package storage

import (
	"db-worker/pkg/random"
	"fmt"
)

// Transaction - реализация сущности транзакции.
type Transaction struct {
	ID     string
	Status txStatus
	// если транзакция не успешна, то в этом поле будет название драйвера, который не успел выполниться.
	// это нужно, чтобы остальные драйвера могли откатиться.
	FailedDriver string
	// если транзакция не успешна, то в этом поле будет ошибка, которая произошла при выполнении транзакции.
	Err error

	Requests map[Driver]*Request
	Begun    map[string]struct{} // драйвера, в которых транзакция была успешно начата.

	InstanceID    int    // экземпляр приложения, выполняющий транзакцию
	OperationHash []byte // хеш операции
}

type txStatus string

const (
	TxStatusInProgress txStatus = "in progress"
	TxStatusSuccess    txStatus = "success"
	TxStatusFailed     txStatus = "failed"
	TxStatusCanceled   txStatus = "canceled"
)

func NewTransaction(status txStatus, requests map[Driver]*Request, instanceID int, operationHash []byte) (*Transaction, error) {
	if status == "" {
		return nil, fmt.Errorf("status not provided")
	}

	if len(requests) == 0 {
		return nil, fmt.Errorf("requests not provided")
	}

	if len(operationHash) == 0 {
		return nil, fmt.Errorf("operation hash not provided")
	}

	// instanceID может быть 0

	return &Transaction{
		ID:            random.String(10),
		Status:        status,
		Requests:      requests,
		Begun:         make(map[string]struct{}),
		InstanceID:    instanceID,
		OperationHash: operationHash,
	}, nil
}

func (tx *Transaction) SetFailedDriver(driver string) {
	tx.FailedDriver = driver
}

func (tx *Transaction) SetStatus(status txStatus) {
	tx.Status = status
}

func (tx *Transaction) SetFailedStatus(driver string, err error) {
	tx.SetFailedDriver(driver)
	tx.SetStatus(TxStatusFailed)
	tx.Err = err
}

func (tx *Transaction) SetSuccessStatus() {
	tx.SetStatus(TxStatusSuccess)
}

func (tx *Transaction) IsInProgress() bool {
	return tx.isEqualStatus(TxStatusInProgress)
}

func (tx *Transaction) IsFailed() bool {
	return tx.isEqualStatus(TxStatusFailed)
}

func (tx *Transaction) isEqualStatus(status txStatus) bool {
	return tx.Status == status
}
