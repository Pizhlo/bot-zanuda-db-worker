package model

// Request - запрос к хранилищу.
// Val - запрос, который может быть разным в зависимости от хранилища.
// Args - аргументы для запроса (по необходимости).
type Request struct {
	Val  any
	Args any
}
