package testtransaction

import (
	"db-worker/internal/storage"
)

// TestTransaction - имплементация Transaction для тестирования.
type TestTransaction struct {
	id     string
	status string
	// если транзакция не успешна, то в этом поле будет название драйвера, который не успел выполниться.
	// это нужно, чтобы остальные драйвера могли откатиться.
	failedDriver storage.Driver
	// если транзакция не успешна, то в этом поле будет ошибка, которая произошла при выполнении транзакции.
	err error

	requests map[storage.Driver]*storage.Request
	begun    map[storage.Driver]struct{} // драйвера, в которых транзакция была успешно начата.

	instanceID    int    // экземпляр приложения, выполняющий транзакцию
	operationHash []byte // хеш операции
	originalTx    storage.TransactionEditor
}

type option func(*TestTransaction)

// WithID устанавливает идентификатор транзакции.
func WithID(id string) option {
	return func(tx *TestTransaction) {
		tx.id = id
	}
}

// WithStatus устанавливает статус транзакции.
func WithStatus(status string) option {
	return func(tx *TestTransaction) {
		tx.status = status
	}
}

// WithFailedDriver устанавливает "сломанный" драйвер транзакции.
func WithFailedDriver(failedDriver storage.Driver) option {
	return func(tx *TestTransaction) {
		tx.failedDriver = failedDriver
	}
}

// WithErr устанавливает ошибку транзакции.
func WithErr(err error) option {
	return func(tx *TestTransaction) {
		tx.err = err
	}
}

// WithRequests устанавливает запросы транзакции.
func WithRequests(requests map[storage.Driver]*storage.Request) option {
	return func(tx *TestTransaction) {
		tx.requests = requests
	}
}

// WithBegun устанавливает драйвера, в которых транзакция была успешно начата.
func WithBegun(begun map[storage.Driver]struct{}) option {
	return func(tx *TestTransaction) {
		tx.begun = begun
	}
}

// WithInstanceID устанавливает экземпляр приложения, выполняющий транзакцию.
func WithInstanceID(instanceID int) option {
	return func(tx *TestTransaction) {
		tx.instanceID = instanceID
	}
}

// WithOperationHash устанавливает хеш операции.
func WithOperationHash(operationHash []byte) option {
	return func(tx *TestTransaction) {
		tx.operationHash = operationHash
	}
}

// WithOriginalTx устанавливает оригинальную транзакцию, которую нужно сохранить.
func WithOriginalTx(originalTx storage.TransactionEditor) option {
	return func(tx *TestTransaction) {
		tx.originalTx = originalTx
	}
}

// NewTestTransaction создает новый экземпляр TestTransaction с заданными опциями.
func NewTestTransaction(opts ...option) *TestTransaction {
	tx := &TestTransaction{
		err:           storage.ErrEmpty,
		requests:      make(map[storage.Driver]*storage.Request),
		begun:         make(map[storage.Driver]struct{}),
		operationHash: make([]byte, 0),
	}

	for _, opt := range opts {
		opt(tx)
	}

	return tx
}

// SaveRequests сохраняет запросы транзакции.
func (tx *TestTransaction) SaveRequests(requests map[storage.Driver]*storage.Request) {
	tx.requests = requests
}

// Requests возвращает запросы транзакции.
func (tx *TestTransaction) Requests() map[storage.Driver]*storage.Request {
	return tx.requests
}

// ID возвращает идентификатор транзакции.
func (tx *TestTransaction) ID() string {
	return tx.id
}

// Error возвращает ошибку транзакции.
func (tx *TestTransaction) Error() error {
	return tx.err
}

// ErrorString возвращает строковое представление ошибки транзакции.
func (tx *TestTransaction) ErrorString() string {
	return tx.err.Error()
}

// FailedDriver возвращает "сломанный" драйвер транзакции.
func (tx *TestTransaction) FailedDriver() storage.Driver {
	return tx.failedDriver
}

// Status возвращает статус транзакции.
func (tx *TestTransaction) Status() string {
	return tx.status
}

// Begun возвращает драйвера, в которых транзакция была успешно начата.
func (tx *TestTransaction) Begun() map[storage.Driver]struct{} {
	return tx.begun
}

// AddBegunDriver добавляет драйвер, в котором транзакция была успешно начата.
func (tx *TestTransaction) AddBegunDriver(driver storage.Driver) {
	tx.begun[driver] = struct{}{}
}

// InstanceID возвращает экземпляр приложения, выполняющий транзакцию.
func (tx *TestTransaction) InstanceID() int {
	return tx.instanceID
}

// OperationHash возвращает хеш операции.
func (tx *TestTransaction) OperationHash() []byte {
	return tx.operationHash
}

// Drivers возвращает драйвера, для которых нужно составить запросы на сохранение оригинальной транзакции.
func (tx *TestTransaction) Drivers() []storage.Driver {
	drivers := make([]storage.Driver, 0, len(tx.requests))
	for driver := range tx.requests {
		drivers = append(drivers, driver)
	}

	return drivers
}

// SetFailedDriver устанавливает "сломанный" драйвер транзакции.
func (tx *TestTransaction) SetFailedDriver(driver storage.Driver) {
	tx.failedDriver = driver
}

// SetStatus устанавливает статус транзакции.
func (tx *TestTransaction) SetStatus(status string) {
	tx.status = status
}

// SetSuccessStatus устанавливает статус success транзакции.
func (tx *TestTransaction) SetSuccessStatus() {
	tx.status = string(storage.TxStatusSuccess)
}

// SetFailedStatus устанавливает статус failed и ошибку транзакции.
// Требуется передать "сломанный" драйвер и ошибку, которая произошла при выполнении транзакции.
func (tx *TestTransaction) SetFailedStatus(driver storage.Driver, err error) {
	tx.failedDriver = driver
	tx.err = err
	tx.status = string(storage.TxStatusFailed)
}

// IsInProgress проверяет, находится ли транзакция в статусе in progress.
func (tx *TestTransaction) IsInProgress() bool {
	return tx.isEqualStatus(string(storage.TxStatusInProgress))
}

// IsFailed проверяет, находится ли транзакция в статусе failed.
func (tx *TestTransaction) IsFailed() bool {
	return tx.isEqualStatus(string(storage.TxStatusFailed))
}

func (tx *TestTransaction) isEqualStatus(status string) bool {
	return tx.status == status
}

// OriginalTx возвращает оригинальную транзакцию, которую нужно сохранить.
func (tx *TestTransaction) OriginalTx() storage.TransactionEditor {
	return tx.originalTx
}

// FailedDriverName возвращает название "сломанного" драйвера транзакции.
func (tx *TestTransaction) FailedDriverName() string {
	var failedDriverName string

	if tx.failedDriver != nil {
		failedDriverName = tx.failedDriver.Name()
	}

	return failedDriverName
}
