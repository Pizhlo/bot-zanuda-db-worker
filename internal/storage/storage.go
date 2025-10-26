package storage

import "db-worker/internal/storage/model"

// Driver определяет интерфейс для работы с хранилищем.
// Перенаправляем на интерфейс из пакета model для избежания циклических импортов.
type Driver = model.Driver
