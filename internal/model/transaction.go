package model

// txStatus - статус транзакции.
type txStatus string

const (
	// TxStatusInProgress - транзакция в процессе выполнения.
	TxStatusInProgress txStatus = "in progress"
	// TxStatusSuccess - транзакция выполнена успешно.
	TxStatusSuccess txStatus = "success"
	// TxStatusFailed - транзакция выполнена с ошибкой.
	TxStatusFailed txStatus = "failed"
)

// Result - результат выполнения транзакции.
type Result struct {
	Status txStatus `json:"status"`
	Error  string   `json:"error"`
}
