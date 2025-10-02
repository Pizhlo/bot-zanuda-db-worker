package random

import (
	"math/rand"
)

// String генерирует случайную строку длиной n.
func String(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] //nolint:gosec // заведена задача BZ-18
	}

	return string(b)
}
