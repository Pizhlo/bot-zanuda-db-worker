CREATE SCHEMA IF NOT EXISTS public;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE TYPE tx_status AS ENUM ('IN_PROGRESS', 'FAILED', 'CANCELED', 'SUCCESS');

create schema if not exists transactions;

-- таблица с транзакциями
create table if not exists transactions.transactions (
    id varchar PRIMARY KEY,
    status tx_status NOT NULL,
    error varchar,
    instance_id integer, -- id экземпляра приложения, который выполняет транзакцию. может быть null
    failed_driver varchar, -- название драйвера, который не успел выполниться (если есть)
    operation_hash BYTEA NOT NULL, -- hash операции, которая выполняется в транзакции. нужно для того, чтобы можно было не выполнять операцию, если изменилась конфигурация.
    created_at BIGINT NOT NULL DEFAULT extract(epoch from current_timestamp)::BIGINT-- extract(epoch from current_timestamp)::BIGINT - получить текущее время в секундах
);

-- таблица с запросами на создание / удаление сущностей
CREATE TABLE IF NOT EXISTS transactions.requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data JSONB NOT NULL, -- мапа с полями сообщения: "field1": "value1", "field2": "value2"
    tx_id varchar NOT NULL,
    driver_type varchar not null,
    driver_name varchar not null,
    FOREIGN KEY (tx_id) REFERENCES transactions.transactions(id)
);

CREATE INDEX IF NOT EXISTS requests_tx_id_idx ON transactions.requests(tx_id);

ALTER TABLE public.schema_migrations ADD COLUMN IF NOT EXISTS created_at timestamp with time zone NOT NULL DEFAULT now();
