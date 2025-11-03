package message

import (
	"time"

	"github.com/google/uuid"
)

// Status - статус сообщения.
type Status string

const (
	// StatusInProgress - сообщение в процессе обработки.
	StatusInProgress Status = "IN_PROGRESS"
	// StatusValidated - сообщение успешно обработано.
	StatusValidated Status = "VALIDATED"
	// StatusFailed - сообщение не обработано.
	StatusFailed Status = "FAILED"
)

// Message - сообщение для обработки.
type Message struct {
	ID            uuid.UUID
	Data          map[string]interface{}
	Status        Status
	Error         string
	DriverType    string
	DriverName    string
	InstanceID    int
	OperationHash []byte
	CreatedAt     time.Time
}
