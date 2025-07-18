package model

import (
	"time"

	"github.com/google/uuid"
)

// Space - пространство пользователя. Может быть личным или совместным (указано во флаге personal bool)
type Space struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	Created  time.Time `json:"created"`  // TODO: unix in UTC
	Creator  int64     `json:"creator"`  // айди пользователя-создателя в телеге
	Personal bool      `json:"personal"` // личное / совместное пространство
}
