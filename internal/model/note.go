package model

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

// тип заметки
type NoteType string

const (
	// текстовая заметка
	TextNoteType NoteType = "text"
	// заметка с фото
	PhotoNoteType NoteType = "photo"
)

type Note struct {
	ID       uuid.UUID    `json:"id"`
	User     *User        `json:"user"`    // кто создал заметку
	Text     string       `json:"text"`    // текст заметки
	Space    *Space       `json:"space"`   // айди пространства, куда сохранить заметку
	Created  time.Time    `json:"created"` // дата создания заметки в часовом поясе пользователя в unix
	LastEdit sql.NullTime `json:"last_edit"`
	Type     NoteType     `json:"type"` // тип заметки: текстовая, фото, видео, етс
	File     string       `json:"file"` // название файла в Minio (если есть)
}

//	{
//	    "user_id": 297850814,
//	    "text": "text",
//	    "space_id": "a2ea4881-bef7-473a-9081-b3df602ba481",
//	    "type": "text"
//	}
//
// Запрос на создание заметки
type CreateNoteRequest struct {
	ID        uuid.UUID `json:"id"`         // айди заметки
	RequestID uuid.UUID `json:"request_id"` // айди запроса
	UserID    int64     `json:"user_id"`    // кто создал заметку
	SpaceID   uuid.UUID `json:"space_id"`   // айди пространства, куда сохранить заметку
	Text      string    `json:"text"`       // текст заметки
	Type      NoteType  `json:"type"`       // тип заметки: текстовая, фото, видео, етс
	File      string    `json:"file"`       // название файла в Minio (если есть)
	Operation Operation `json:"operation"`  // какое действие сделать: создать, удалить, редактировать
	Created   int64     `json:"created"`    // дата обращения в Unix в UTC
}

func (CreateNoteRequest) MessageType() MessageType {
	return MessageTypeNoteCreate
}

func (s CreateNoteRequest) OperationType() Operation {
	return s.Operation
}

func (s CreateNoteRequest) Model() any {
	return s
}

type Operation string

// в будущем будет использоваться в других местах, когда реализуются хендлеры
var (
	CreateOp         Operation = "create"
	UpdateOp         Operation = "update"
	DeleteOp         Operation = "delete"
	DeleteAllOp      Operation = "delete_all"
	AddParticipantOp Operation = "add_participant"
)
