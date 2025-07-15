package message

import (
	"context"
	"db-worker/internal/model"
	interfaces "db-worker/internal/service/message/interface"
	"db-worker/internal/service/message/mocks"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestHandleCreateNotes(t *testing.T) {
	const testTimeout = 1 * time.Second

	notesCount := 100
	bufferSize := 10

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	createHandler := mocks.NewMockHandler(ctrl)
	message := newMessageMock(model.MessageTypeNoteCreate, model.CreateOp, notesCount)

	msgChan := make(chan interfaces.Message, bufferSize)
	service := &Service{
		createChannels: []chan interfaces.Message{msgChan},
		createHandler:  createHandler,
		updateChannels: []chan interfaces.Message{msgChan},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go service.handleCreateOperation(ctx, msgChan, createHandler)

	done := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(1)

	// сколько дошло сообщений
	msgCount := 0
	mu := sync.Mutex{}

	go func() {
		defer wg.Done()
		for range notesCount {
			createHandler.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Do(func(ctx context.Context, msg interfaces.Message, shouldSave bool) error {
				mu.Lock()
				msgCount++

				if msgCount == notesCount {
					done <- struct{}{}
				}

				mu.Unlock()

				return nil
			})

			msgChan <- message.Model().(interfaces.Message)
		}
	}()

	wg.Wait()

	for {
		select {
		case <-done:
			mu.Lock()
			assert.Equal(t, notesCount, msgCount)
			mu.Unlock()

			return
		case <-time.After(testTimeout):
			t.Fatal("timeout")
		}
	}
}

func TestHandleUpdateNotes(t *testing.T) {
	const testTimeout = 1 * time.Second

	notesCount := 100
	bufferSize := 10

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	updateHandler := mocks.NewMockHandler(ctrl)
	message := newMessageMock(model.MessageTypeNoteUpdate, model.UpdateOp, notesCount)

	msgChan := make(chan interfaces.Message, bufferSize)
	service := &Service{
		updateChannels: []chan interfaces.Message{msgChan},
		updateHandler:  updateHandler,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go service.handleUpdateOperation(ctx, msgChan, updateHandler)

	done := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(1)

	// сколько дошло сообщений
	msgCount := 0
	mu := sync.Mutex{}

	go func() {
		defer wg.Done()
		for range notesCount {
			updateHandler.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Do(func(ctx context.Context, msg interfaces.Message, shouldSave bool) error {
				mu.Lock()
				msgCount++

				if msgCount == notesCount {
					done <- struct{}{}
				}

				mu.Unlock()

				return nil
			})

			msgChan <- message.Model().(interfaces.Message)
		}
	}()

	wg.Wait()

	for {
		select {
		case <-done:
			mu.Lock()
			assert.Equal(t, notesCount, msgCount)
			mu.Unlock()

			return
		case <-time.After(testTimeout):
			t.Fatal("timeout")
		}
	}
}

type messageMock struct {
	messageType   model.MessageType
	operation     model.Operation
	expectedCount int
	count         int
	mu            sync.Mutex
}

func newMessageMock(messageType model.MessageType, operation model.Operation, expectedCount int) *messageMock {
	return &messageMock{
		messageType:   messageType,
		operation:     operation,
		expectedCount: expectedCount,
	}
}

func (m *messageMock) MessageType() model.MessageType {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.count++

	return m.messageType
}

func (m *messageMock) Model() any {
	return m
}

func (m *messageMock) GetOperation() model.Operation {
	return m.operation
}

func (m *messageMock) GetRequestID() uuid.UUID {
	return uuid.New()
}
