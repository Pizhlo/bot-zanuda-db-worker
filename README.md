# Bot Zanuda DB Worker

Сервис для обработки операций с базой данных через RabbitMQ.

## 🚀 Быстрый старт

```bash
# Клонирование репозитория
git clone <repository-url>
cd bot-zanuda-db-worker

# Установка зависимостей
go mod download

# Запуск сервиса
make run
```

## ⚙️ Конфигурация
Сервис полностью настраивается через конфигурационный файл. В конфигурации описываются:

### Модели данных
```yaml
note: # название модели
    operations: # операции, которые можно выполнить над моделью
      create_notes:
        type: create
        storage: postgres # хранилище, в котором нужно производить операцию
        table: notes.notes # название таблицы, в которой будет храниться модель
        fields: # поля, необходимые для операции
          text:
            type: string
            required: true
            validation:
              - type: not_empty
              - type: max_length
                max_length: 10000
```

### Операции
Возможные операции: `create`, `update`, `delete`.

### Соединения
```yaml
request: # каким образом будет получен запрос на операцию
          from: rabbitmq
          config:
            address: "amqp://user:password@localhost:5672/"
            queue: notes
            routing_key: create # или update, delete
            message:
              operation:
                type: string
                required: true
                value: "create"
```

## 🐰 RabbitMQ Architecture

Сервис использует **Topic Exchange** для разделения операций:

### Exchange
- **Имя**: `notes`
- **Тип**: `topic`
- **Долговечность**: `true`

### Routing Keys
- `create` - для операций создания
- `update` - для операций обновления  
- `delete` - для операций удаления

### Очереди
Каждый worker создает уникальную очередь с именем: `{topic}_{operation}_{routing_key}`

Например:
- `notes_create_create` - для обработки операций создания
- `notes_update_update` - для обработки операций обновления
- `notes_delete_delete` - для обработки операций удаления

### Отправка сообщений
Для отправки сообщений используйте routing key, соответствующий операции:

```json
{
  "operation": "create",
  "request_id": "uuid",
  "text": "Текст заметки"
}
```

Отправьте в exchange `notes` с routing key `create`.

Пример конфигурационного файла можно посмотреть по пути - `internal/config/testdata/valid_model.yaml`.

## 📈 Масштабирование