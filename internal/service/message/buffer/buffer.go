package buffer

import (
	interfaces "db-worker/internal/service/message/interface"
	"fmt"
)

type Buffer struct {
	bufferSize int
	buff       []interfaces.Message
}

func New(bufferSize int) *Buffer {
	return &Buffer{
		bufferSize: bufferSize,
		buff:       make([]interfaces.Message, 0, bufferSize),
	}
}

func (b *Buffer) Add(msg interfaces.Message) error {
	if b.IsFull() {
		return fmt.Errorf("buffer is full")
	}

	b.buff = append(b.buff, msg)

	return nil
}

func (b *Buffer) Flush() {
	b.buff = make([]interfaces.Message, 0, b.bufferSize)
}

func (b *Buffer) Get() []interfaces.Message {
	buff := b.buff

	return buff
}

func (b *Buffer) IsFull() bool {
	return len(b.buff) >= b.bufferSize
}
