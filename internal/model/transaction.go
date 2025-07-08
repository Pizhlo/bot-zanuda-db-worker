package model

type txStatus string

const (
	TxStatusInProgress txStatus = "in progress"
	TxStatusSuccess    txStatus = "success"
	TxStatusFailed     txStatus = "failed"
)

type Result struct {
	Status txStatus `json:"status"`
	Error  string   `json:"error"`
}
