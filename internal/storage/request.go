package storage

import (
	"db-worker/internal/storage/model"
	"time"

	"github.com/google/uuid"
)

// Request - запрос к хранилищу.
// Перенаправляем на тип из пакета interfaces для избежания циклических импортов.
type Request = model.Request

// RequestModel - модель запроса, которая нужна для его хранения в БД.
type RequestModel struct {
	ID         uuid.UUID
	TxID       string
	DriverType string
	DriverName string
	CreatedAt  time.Time
}

// TransactionModel - модель транзакции, которая нужна для её хранения в БД.
type TransactionModel struct {
	ID            string
	Status        txStatus
	Data          map[string]any
	Error         string
	InstanceID    int
	OperationHash []byte
	OperationType string
	FailedDriver  string
	CreatedAt     time.Time
	Requests      []RequestModel
}
