package storage

import "db-worker/internal/storage/model"

// Request - запрос к хранилищу.
// Перенаправляем на тип из пакета interfaces для избежания циклических импортов.
type Request = model.Request
