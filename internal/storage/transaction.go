package storage

import (
	"db-worker/pkg/random"
	"errors"
	"fmt"
	"sync"
)

// TransactionEditor - интерфейс для доступа к транзакции.
type TransactionEditor interface {
	// SaveRequests сохраняет запросы транзакции. Не реализовано для основной транакзции, только для служебной.
	SaveRequests(requests map[Driver]*Request)
	// Requests возвращает запросы транзакции.
	Requests() map[Driver]*Request
	// ID возвращает идентификатор транзакции. Служебная транзакция возвращает идентификатор оригинальной транзакции.
	ID() string
	// Error возвращает ошибку транзакции.
	Error() error
	// ErrorString возвращает строковое представление ошибки транзакции.
	ErrorString() string
	// FailedDriver возвращает "сломанный" драйвер транзакции.
	FailedDriver() Driver
	// Status возвращает статус транзакции.
	Status() string
	// SetFailedStatus устанавливает статус failed и ошибку транзакции.
	SetFailedStatus(driver Driver, err error)
	// IsInProgress проверяет, находится ли транзакция в статусе in progress.
	IsInProgress() bool
	// IsFailed проверяет, находится ли транзакция в статусе failed.
	IsFailed() bool
	// SetSuccessStatus устанавливает статус success транзакции.
	SetSuccessStatus()
	// SetFailedDriver устанавливает "сломанный" драйвер транзакции.
	SetFailedDriver(driver Driver)
	// Begun возвращает драйвера, в которых транзакция была успешно начата.
	Begun() map[Driver]struct{}
	// AddBegunDriver добавляет драйвер, в котором транзакция была успешно начата.
	AddBegunDriver(driver Driver)
	// InstanceID возвращает экземпляр приложения, выполняющий транзакцию.
	InstanceID() int
	// OperationHash возвращает хеш операции.
	OperationHash() []byte
	// Drivers возвращает драйвера, для которых нужно составить запросы на сохранение транзакции.
	Drivers() []Driver
	// OriginalTx возвращает оригинальную транзакцию, которую нужно сохранить.
	OriginalTx() TransactionEditor
	// FailedDriverName возвращает название "сломанного" драйвера транзакции.
	FailedDriverName() string
}

// Transaction - реализация сущности транзакции.
type Transaction struct {
	id     string
	status txStatus
	// если транзакция не успешна, то в этом поле будет название драйвера, который не успел выполниться.
	// это нужно, чтобы остальные драйвера могли откатиться.
	failedDriver Driver
	// если транзакция не успешна, то в этом поле будет ошибка, которая произошла при выполнении транзакции.
	err error

	requests map[Driver]*Request
	begun    map[Driver]struct{} // драйвера, в которых транзакция была успешно начата.

	instanceID    int    // экземпляр приложения, выполняющий транзакцию
	operationHash []byte // хеш операции
}

type txStatus string

const (
	// TxStatusInProgress - транзакция в процессе выполнения.
	TxStatusInProgress txStatus = "IN_PROGRESS"
	// TxStatusSuccess - транзакция выполнена успешно.
	TxStatusSuccess txStatus = "SUCCESS"
	// TxStatusFailed - транзакция выполнена с ошибкой.
	TxStatusFailed txStatus = "FAILED"
	// TxStatusCanceled - транзакция отменена (например, изменилась конфигурация операции).
	TxStatusCanceled txStatus = "CANCELED"
)

// NewTransaction создает новую транзакцию.
// Требуется передать статус, запросы, экземпляр приложения и хеш операции.
func NewTransaction(requests map[Driver]*Request, instanceID int, operationHash []byte) (*Transaction, error) {
	if len(requests) == 0 {
		return nil, fmt.Errorf("requests not provided")
	}

	if len(operationHash) == 0 {
		return nil, fmt.Errorf("operation hash not provided")
	}

	// instanceID может быть 0

	return &Transaction{
		id:            random.String(10),
		status:        TxStatusInProgress,
		requests:      requests,
		err:           ErrEmpty,
		begun:         make(map[Driver]struct{}),
		instanceID:    instanceID,
		operationHash: operationHash,
	}, nil
}

// SetFailedDriver устанавливает "сломанный" драйвер транзакции.
func (tx *Transaction) SetFailedDriver(driver Driver) {
	tx.failedDriver = driver
}

// Drivers возвращает драйвера, для которых нужно составить запросы на сохранение транзакции.
func (tx *Transaction) Drivers() []Driver {
	drivers := make([]Driver, 0, len(tx.requests))
	for driver := range tx.requests {
		drivers = append(drivers, driver)
	}

	return drivers
}

// SetStatus устанавливает статус транзакции.
func (tx *Transaction) SetStatus(status txStatus) {
	tx.status = status
}

// SetFailedStatus устанавливает статус failed и ошибку транзакции.
// Требуется передать "сломанный" драйвер и ошибку, которая произошла при выполнении транзакции.
func (tx *Transaction) SetFailedStatus(driver Driver, err error) {
	tx.SetFailedDriver(driver)
	tx.SetStatus(TxStatusFailed)
	tx.err = err
}

// SetSuccessStatus устанавливает статус success транзакции.
func (tx *Transaction) SetSuccessStatus() {
	tx.SetStatus(TxStatusSuccess)
}

// IsInProgress проверяет, находится ли транзакция в статусе in progress.
func (tx *Transaction) IsInProgress() bool {
	return tx.isEqualStatus(TxStatusInProgress)
}

// IsFailed проверяет, находится ли транзакция в статусе failed.
func (tx *Transaction) IsFailed() bool {
	return tx.isEqualStatus(TxStatusFailed)
}

// isEqualStatus проверяет, равен ли статус транзакции заданному статусу.
func (tx *Transaction) isEqualStatus(status txStatus) bool {
	return tx.status == status
}

// Requests возвращает запросы транзакции.
func (tx *Transaction) Requests() map[Driver]*Request {
	return tx.requests
}

// SaveRequests сохраняет запросы транзакции.
// Не реализовано для основной транакзции, только для служебной.
func (tx *Transaction) SaveRequests(requests map[Driver]*Request) {
	// not implemented
}

// ID возвращает идентификатор транзакции.
func (tx *Transaction) ID() string {
	return tx.id
}

// Error возвращает ошибку транзакции.
// Если ошибка не установлена, возвращается пустая ошибка.
func (tx *Transaction) Error() error {
	return tx.err
}

// ErrorString возвращает строковое представление ошибки транзакции.
func (tx *Transaction) ErrorString() string {
	return tx.err.Error()
}

// FailedDriver возвращает "сломанный" драйвер транзакции.
func (tx *Transaction) FailedDriver() Driver {
	return tx.failedDriver
}

// Status возвращает статус транзакции.
func (tx *Transaction) Status() string {
	return string(tx.status)
}

// Begun возвращает драйвера, в которых транзакция была успешно начата.
func (tx *Transaction) Begun() map[Driver]struct{} {
	return tx.begun
}

// AddBegunDriver добавляет драйвер, в котором транзакция была успешно начата.
func (tx *Transaction) AddBegunDriver(driver Driver) {
	tx.begun[driver] = struct{}{}
}

// InstanceID возвращает экземпляр приложения, выполняющий транзакцию.
func (tx *Transaction) InstanceID() int {
	return tx.instanceID
}

// OperationHash возвращает хеш операции.
func (tx *Transaction) OperationHash() []byte {
	return tx.operationHash
}

// OriginalTx возвращает оригинальную транзакцию, которую нужно сохранить.
func (tx *Transaction) OriginalTx() TransactionEditor {
	return tx
}

// FailedDriverName возвращает название "сломанного" драйвера транзакции.
// Если "сломанный" драйвер не установлен, возвращается пустая строка.
func (tx *Transaction) FailedDriverName() string {
	var failedDriverName string

	if tx.failedDriver != nil {
		failedDriverName = tx.failedDriver.Name()
	}

	return failedDriverName
}

// utilityTransaction - вспомогательный тип для сохранения транзакции Transaction.
type utilityTransaction struct {
	Transaction
	originalTx *Transaction
	requests   map[Driver]*Request
	drivers    []Driver // драйвера, для которых нужно составить запросы на сохранение оригинальной транзакции

	mu sync.RWMutex
}

type option func(*utilityTransaction)

// WithDriver добавляет драйвер, для которого нужно составить запросы на сохранение оригинальной транзакции.
// Можно вызывать несколько раз, чтобы добавить несколько драйверов.
func WithDriver(driver Driver) option {
	return func(ut *utilityTransaction) {
		ut.drivers = append(ut.drivers, driver)
	}
}

// WithOriginalTx устанавливает оригинальную транзакцию, которую нужно сохранить.
func WithOriginalTx(originalTx *Transaction) option {
	return func(ut *utilityTransaction) {
		ut.originalTx = originalTx
	}
}

// NewUtilityTransaction создает служебную транзакцию - для сохранения основной транзакции.
// Самостоятельно составляет запросы для сохранения транзакции.
func NewUtilityTransaction(opts ...option) (*utilityTransaction, error) {
	tx := &utilityTransaction{
		Transaction: Transaction{
			begun:  make(map[Driver]struct{}),
			err:    ErrEmpty,
			status: TxStatusInProgress,
		},
		drivers:  make([]Driver, 0),
		requests: make(map[Driver]*Request),
	}

	for _, opt := range opts {
		opt(tx)
	}

	if len(tx.drivers) == 0 {
		return nil, errors.New("no drivers provided")
	}

	if tx.originalTx == nil {
		return nil, errors.New("original transaction not provided")
	}

	return tx, nil
}

// Drivers возвращает драйвера, для которых нужно составить запросы на сохранение оригинальной транзакции.
func (ux *utilityTransaction) Drivers() []Driver {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.drivers
}

// SetFailedDriver устанавливает "сломанный" драйвер транзакции.
func (ux *utilityTransaction) SetFailedDriver(driver Driver) {
	ux.mu.Lock()
	defer ux.mu.Unlock()

	ux.failedDriver = driver
}

// SetStatus устанавливает статус транзакции.
func (ux *utilityTransaction) SetStatus(status txStatus) {
	ux.mu.Lock()
	defer ux.mu.Unlock()

	ux.status = status
}

// SetFailedStatus устанавливает статус failed и ошибку транзакции.
// Требуется передать "сломанный" драйвер и ошибку, которая произошла при выполнении транзакции.
func (ux *utilityTransaction) SetFailedStatus(driver Driver, err error) {
	ux.SetFailedDriver(driver)
	ux.SetStatus(TxStatusFailed)

	ux.mu.Lock()
	defer ux.mu.Unlock()

	ux.err = err
}

func (ux *utilityTransaction) SetSuccessStatus() {
	ux.SetStatus(TxStatusSuccess)
}

func (ux *utilityTransaction) IsInProgress() bool {
	return ux.isEqualStatus(TxStatusInProgress)
}

func (ux *utilityTransaction) IsFailed() bool {
	return ux.isEqualStatus(TxStatusFailed)
}

func (ux *utilityTransaction) isEqualStatus(status txStatus) bool {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.status == status
}

func (ux *utilityTransaction) Requests() map[Driver]*Request {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.requests
}

func (ux *utilityTransaction) ID() string {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.originalTx.id
}

// ErrEmpty - пустая ошибка.
// Используется, когда ошибка не установлена.
var ErrEmpty = errors.New("")

// Error возвращает ошибку транзакции.
// Если ошибка не установлена, возвращается пустая ошибка.
func (ux *utilityTransaction) Error() error {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.err
}

func (ux *utilityTransaction) ErrorString() string {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.err.Error()
}

func (ux *utilityTransaction) FailedDriver() Driver {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	if ux.originalTx.FailedDriver() != nil {
		return ux.originalTx.FailedDriver()
	}

	return ux.failedDriver
}

func (ux *utilityTransaction) Status() string {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return string(ux.status)
}

func (ux *utilityTransaction) Begun() map[Driver]struct{} {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.begun
}

func (ux *utilityTransaction) AddBegunDriver(driver Driver) {
	ux.mu.Lock()
	defer ux.mu.Unlock()

	ux.begun[driver] = struct{}{}
}

func (ux *utilityTransaction) InstanceID() int {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.originalTx.instanceID
}

func (ux *utilityTransaction) OperationHash() []byte {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	if ux.originalTx.operationHash == nil {
		return ux.operationHash
	}

	return ux.originalTx.operationHash
}

func (ux *utilityTransaction) SaveRequests(requests map[Driver]*Request) {
	ux.mu.Lock()
	defer ux.mu.Unlock()

	ux.requests = requests
}

func (ux *utilityTransaction) OriginalTx() TransactionEditor {
	ux.mu.RLock()
	defer ux.mu.RUnlock()

	return ux.originalTx
}

func (ux *utilityTransaction) FailedDriverName() string {
	var failedDriverName string

	if ux.originalTx.FailedDriver() != nil {
		failedDriverName = ux.originalTx.FailedDriver().Name()
	}

	return failedDriverName
}
