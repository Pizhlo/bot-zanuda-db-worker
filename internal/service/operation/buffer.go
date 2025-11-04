package operation

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type buffer struct {
	size int
	mu   sync.Mutex

	items []item
}

type item struct {
	ids  []uuid.UUID
	data map[string]any
}

func newBuffer(size int) (*buffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("size must be greater than 0")
	}

	return &buffer{
		size:  size,
		items: make([]item, 0, size),
	}, nil
}

func (b *buffer) add(ids []uuid.UUID, data map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.items) >= b.size {
		return fmt.Errorf("buffer is full")
	}

	b.items = append(b.items, item{
		ids:  ids,
		data: data,
	})

	return nil
}

func (b *buffer) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.items)
}

func (b *buffer) getAll() []item {
	b.mu.Lock()
	defer b.mu.Unlock()

	items := make([]item, len(b.items))
	copy(items, b.items)

	return items
}

func (b *buffer) isFull() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.items) >= b.size
}

func (b *buffer) clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.items = make([]item, 0, b.size)
}
