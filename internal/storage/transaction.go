package storage

import (
	"db-worker/pkg/random"
	"errors"
	"fmt"
)

// TransactionEditor - интерфейс для доступа к транзакции.
type TransactionEditor interface {
	SaveRequests(requests map[Driver]*Request)
	Requests() map[Driver]*Request
	ID() string
	Error() error
	FailedDriver() string
	Status() string
	SetFailedStatus(driver string, err error)
	IsInProgress() bool
	IsFailed() bool
	SetSuccessStatus()
	SetFailedDriver(driver string)
	Begun() map[string]struct{}
	AddBegunDriver(driver Driver)
	InstanceID() int
	OperationHash() []byte
}

// Transaction - реализация сущности транзакции.
type Transaction struct {
	id     string
	status txStatus
	// если транзакция не успешна, то в этом поле будет название драйвера, который не успел выполниться.
	// это нужно, чтобы остальные драйвера могли откатиться.
	failedDriver string
	// если транзакция не успешна, то в этом поле будет ошибка, которая произошла при выполнении транзакции.
	err error

	requests map[Driver]*Request
	begun    map[string]struct{} // драйвера, в которых транзакция была успешно начата.

	instanceID    int    // экземпляр приложения, выполняющий транзакцию
	operationHash []byte // хеш операции
}

// utilityTransaction - вспомогательный тип для сохранения транзакции Transaction.
type utilityTransaction struct {
	Transaction
	OriginalTx *Transaction
	requests   map[Driver]*Request
	drivers    []Driver // драйвера, для которых нужно составить запросы на сохранение оригинальной транзакции
}

type txStatus string

const (
	TxStatusInProgress txStatus = "IN_PROGRESS"
	TxStatusSuccess    txStatus = "SUCCESS"
	TxStatusFailed     txStatus = "FAILED"
	TxStatusCanceled   txStatus = "CANCELED"
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
		id:            random.String(10),
		status:        status,
		requests:      requests,
		begun:         make(map[string]struct{}),
		instanceID:    instanceID,
		operationHash: operationHash,
	}, nil
}

func (tx *Transaction) SetFailedDriver(driver string) {
	tx.failedDriver = driver
}

func (tx *Transaction) SetStatus(status txStatus) {
	tx.status = status
}

func (tx *Transaction) SetFailedStatus(driver string, err error) {
	tx.SetFailedDriver(driver)
	tx.SetStatus(TxStatusFailed)
	tx.err = err
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
	return tx.status == status
}

func (tx *Transaction) Requests() map[Driver]*Request {
	return tx.requests
}

func (tx *Transaction) SaveRequests(requests map[Driver]*Request) {
	// not implemented
}

func (tx *Transaction) ID() string {
	return tx.id
}
func (tx *Transaction) Error() error {
	return tx.err
}
func (tx *Transaction) FailedDriver() string {
	return tx.failedDriver
}

func (tx *Transaction) Status() string {
	return string(tx.status)
}

func (tx *Transaction) Begun() map[string]struct{} {
	return tx.begun
}

func (tx *Transaction) AddBegunDriver(driver Driver) {
	tx.begun[driver.Name()] = struct{}{}
}

func (tx *Transaction) InstanceID() int {
	return tx.instanceID
}

func (tx *Transaction) OperationHash() []byte {
	return tx.operationHash
}

type option func(*utilityTransaction)

func WithDriver(driver Driver) option {
	return func(ut *utilityTransaction) {
		ut.drivers = append(ut.drivers, driver)
	}
}

func WithOriginalTx(originalTx *Transaction) option {
	return func(ut *utilityTransaction) {
		ut.OriginalTx = originalTx
	}
}

// NewUtilityTransaction создает служебную транзакцию - для сохранения основной транзакции.
// Самостоятельно составляет запросы для сохранения транзакции.
func NewUtilityTransaction(opts ...option) (*utilityTransaction, error) {
	tx := &utilityTransaction{
		Transaction: Transaction{
			begun: make(map[string]struct{}),
		},
	}

	for _, opt := range opts {
		opt(tx)
	}

	if len(tx.drivers) == 0 {
		return nil, errors.New("no drivers provided")
	}

	if tx.OriginalTx == nil {
		return nil, errors.New("original transaction not provided")
	}

	return tx, nil
}

func (ux *utilityTransaction) Drivers() []Driver {
	return ux.drivers
}

func (tx *utilityTransaction) SetFailedDriver(driver string) {
	tx.failedDriver = driver
}

func (tx *utilityTransaction) SetStatus(status txStatus) {
	tx.status = status
}

func (tx *utilityTransaction) SetFailedStatus(driver string, err error) {
	tx.SetFailedDriver(driver)
	tx.SetStatus(TxStatusFailed)
	tx.err = err
}

func (tx *utilityTransaction) SetSuccessStatus() {
	tx.SetStatus(TxStatusSuccess)
}

func (tx *utilityTransaction) IsInProgress() bool {
	return tx.isEqualStatus(TxStatusInProgress)
}

func (tx *utilityTransaction) IsFailed() bool {
	return tx.isEqualStatus(TxStatusFailed)
}

func (tx *utilityTransaction) isEqualStatus(status txStatus) bool {
	return tx.status == status
}

func (tx *utilityTransaction) Requests() map[Driver]*Request {
	return tx.requests
}

func (tx *utilityTransaction) ID() string {
	return tx.OriginalTx.id
}
func (tx *utilityTransaction) Error() error {
	return tx.err
}
func (tx *utilityTransaction) FailedDriver() string {
	return tx.failedDriver
}

func (tx *utilityTransaction) Status() string {
	return string(tx.status)
}

func (tx *utilityTransaction) Begun() map[string]struct{} {
	return tx.begun
}

func (tx *utilityTransaction) AddBegunDriver(driver Driver) {
	tx.begun[driver.Name()] = struct{}{}
}

func (tx *utilityTransaction) InstanceID() int {
	return tx.instanceID
}

func (tx *utilityTransaction) OperationHash() []byte {
	return tx.operationHash
}

func (tx *utilityTransaction) SaveRequests(requests map[Driver]*Request) {
	tx.requests = requests
}
