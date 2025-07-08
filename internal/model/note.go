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

func (s CreateNoteRequest) GetRequestID() uuid.UUID {
	return s.RequestID
}

func (s CreateNoteRequest) GetOperation() Operation {
	return s.Operation
}

//	{
//	    "request_id": "0a3d46d0-b9ea-4acd-bd9c-2a7bd0cabc4b",
//	    "space_id": "842f6d7f-c8cc-4fd7-91c7-0d2e1c524732",
//	    "user_id": 297850814,
//	    "note_id": "62a8b9aa-a74b-4bde-b356-f5f4d73fb622",
//	    "text": "new text",
//	    "file": "",
//	    "operation": "update",
//	    "created": 1752000473
//	}
type UpdateNoteRequest struct {
	ID        uuid.UUID `json:"note_id"`    // айди заметки
	RequestID uuid.UUID `json:"request_id"` // айди запроса
	UserID    int64     `json:"user_id"`    // кто создал заметку
	SpaceID   uuid.UUID `json:"space_id"`   // айди пространства, куда сохранить заметку
	Text      string    `json:"text"`       // текст заметки
	Operation Operation `json:"operation"`  // какое действие сделать: создать, удалить, редактировать
	File      string    `json:"file"`
}

func (UpdateNoteRequest) MessageType() MessageType {
	return MessageTypeNoteUpdate
}

func (s UpdateNoteRequest) OperationType() Operation {
	return s.Operation
}

func (s UpdateNoteRequest) Model() any {
	return s
}

func (s UpdateNoteRequest) GetRequestID() uuid.UUID {
	return s.RequestID
}

func (s UpdateNoteRequest) GetOperation() Operation {
	return s.Operation
}

type Operation string

var (
	CreateOp         Operation = "create"
	UpdateOp         Operation = "update"
	DeleteOp         Operation = "delete"
	DeleteAllOp      Operation = "delete_all"
	AddParticipantOp Operation = "add_participant"
)
