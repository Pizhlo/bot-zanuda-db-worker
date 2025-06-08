package model

type MessageType string

const (
	MessageTypeNoteCreate MessageType = "note_create"
	MessageTypeNoteDelete MessageType = "note_delete"
	MessageTypeNoteUpdate MessageType = "note_update"
)
