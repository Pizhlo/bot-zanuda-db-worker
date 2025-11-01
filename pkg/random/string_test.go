package random

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	t.Parallel()

	length := 10

	got := String(length)

	assert.Len(t, got, length)
	assert.Regexp(t, `^[a-zA-Z0-9]+$`, got)
}
