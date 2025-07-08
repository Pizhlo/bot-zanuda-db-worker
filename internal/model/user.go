package model

import "database/sql"

type User struct {
	ID            int            `json:"id"`
	TgID          int64          `json:"tg_id"`
	Username      string         `json:"username"`
	UsernameSQL   sql.NullString `json:"-"`
	PersonalSpace *Space         `json:"personal_space"`
	Timezone      string         `json:"timezone"`
}
