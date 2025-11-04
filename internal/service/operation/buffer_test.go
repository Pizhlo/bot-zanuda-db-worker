package operation

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuffer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    int
		want    *buffer
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "positive case",
			size:    10,
			wantErr: require.NoError,
			want: &buffer{
				size:  10,
				items: make([]item, 0, 10),
			},
		},
		{
			name:    "negative case: size is 0",
			size:    0,
			wantErr: require.Error,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buffer, err := newBuffer(tt.size)
			tt.wantErr(t, err)

			assert.Equal(t, tt.want, buffer)
		})
	}
}

//nolint:funlen // тестовая функция
func TestBuffer_Add(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		buffer  *buffer
		ids     []uuid.UUID
		data    map[string]any
		wantErr require.ErrorAssertionFunc
		want    *buffer
	}{
		{
			name: "positive case",
			ids:  []uuid.UUID{uuid.New(), uuid.New()},
			data: map[string]any{"test": "test"},
			buffer: &buffer{
				size:  10,
				items: make([]item, 0, 10),
			},
			wantErr: require.NoError,
			want: &buffer{
				size: 10,
				items: []item{
					{
						ids:  []uuid.UUID{uuid.New(), uuid.New()},
						data: map[string]any{"test": "test"},
					},
				},
			},
		},
		{
			name: "negative case: buffer is full",
			ids:  []uuid.UUID{uuid.New(), uuid.New()},
			data: map[string]any{"test": "test"},
			buffer: &buffer{
				size: 2,
				items: []item{
					{
						ids:  []uuid.UUID{uuid.New(), uuid.New()},
						data: map[string]any{"test": "test"},
					},
					{
						ids:  []uuid.UUID{uuid.New(), uuid.New()},
						data: map[string]any{"test": "test"},
					},
				},
			},
			wantErr: require.Error,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.buffer.add(tt.ids, tt.data)
			tt.wantErr(t, err)

			if tt.want != nil {
				assert.Equal(t, tt.want.count(), tt.buffer.count())
			}
		})
	}
}

func TestBuffer_Count(t *testing.T) {
	t.Parallel()

	buffer := &buffer{
		size:  10,
		items: make([]item, 0, 10),
	}

	ids := []uuid.UUID{uuid.New(), uuid.New()}
	data := map[string]any{"test": "test"}

	err := buffer.add(ids, data)
	require.NoError(t, err)

	assert.Equal(t, buffer.count(), 1)
}

func TestBuffer_GetAll(t *testing.T) {
	t.Parallel()

	buffer := &buffer{
		size:  10,
		items: make([]item, 0, 10),
	}

	ids := []uuid.UUID{uuid.New(), uuid.New()}
	data := map[string]any{"test": "test"}

	err := buffer.add(ids, data)
	require.NoError(t, err)

	expected := []item{
		{
			ids:  ids,
			data: data,
		},
	}

	items := buffer.getAll()
	assert.Equal(t, expected, items)
}

//nolint:dupl // похожие тест кейсы
func TestBuffer_IsFull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		size  int
		items []item
		want  bool
	}{
		{
			name: "buffer is full",
			size: 3,
			items: []item{
				{
					ids:  []uuid.UUID{uuid.New(), uuid.New()},
					data: map[string]any{"test": "test"},
				},
				{
					ids:  []uuid.UUID{uuid.New(), uuid.New()},
					data: map[string]any{"test": "test"},
				},
				{
					ids:  []uuid.UUID{uuid.New(), uuid.New()},
					data: map[string]any{"test": "test"},
				},
			},
			want: true,
		},
		{
			name: "buffer is not full",
			size: 10,
			items: []item{
				{
					ids:  []uuid.UUID{uuid.New(), uuid.New()},
					data: map[string]any{"test": "test"},
				},
				{
					ids:  []uuid.UUID{uuid.New(), uuid.New()},
					data: map[string]any{"test": "test"},
				},
				{
					ids:  []uuid.UUID{uuid.New(), uuid.New()},
					data: map[string]any{"test": "test"},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buffer := &buffer{
				size:  tt.size,
				items: tt.items,
			}

			assert.Equal(t, tt.want, buffer.isFull())
		})
	}
}

func TestBuffer_Clear(t *testing.T) {
	t.Parallel()

	buffer := &buffer{
		size:  10,
		items: make([]item, 0, 10),
	}

	buffer.items = []item{
		{
			ids:  []uuid.UUID{uuid.New(), uuid.New()},
			data: map[string]any{"test": "test"},
		},
		{
			ids:  []uuid.UUID{uuid.New(), uuid.New()},
			data: map[string]any{"test": "test"},
		},
	}

	buffer.clear()

	assert.Len(t, buffer.items, 0)
}
