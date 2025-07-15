package model

import "github.com/google/uuid"

type MessageType string

const (
	MessageTypeNoteCreate MessageType = "note_create"
	MessageTypeNoteDelete MessageType = "note_delete"
	MessageTypeNoteUpdate MessageType = "note_update"
)

type Message map[string]interface{}

func (m Message) GetOperation() Operation {
	val, ok := m["operation"]
	if !ok {
		return ""
	}

	return Operation(val.(string))
}

func (m Message) GetRequestID() uuid.UUID {
	val, ok := m["request_id"]
	if !ok {
		return uuid.Nil
	}

	valStr, ok := val.(string)
	if !ok {
		return uuid.Nil
	}

	return uuid.MustParse(valStr)
}

func (m Message) MessageType() MessageType {
	return ""
}

func (m Message) Model() any {
	return nil
}
