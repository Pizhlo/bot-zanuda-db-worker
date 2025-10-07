package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForPostgres(t *testing.T) {
	t.Parallel()

	builder := ForPostgres()
	require.NotNil(t, builder)
	assert.Equal(t, builder, &postgresBuilder{})
}

func TestForRabbitMQ(t *testing.T) {
	t.Parallel()

	builder := ForRabbitMQ()
	require.Nil(t, builder)
}

func TestForTypesense(t *testing.T) {
	t.Parallel()

	builder := ForTypesense()
	require.Nil(t, builder)
}
