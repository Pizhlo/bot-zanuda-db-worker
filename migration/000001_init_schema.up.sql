CREATE SCHEMA IF NOT EXISTS public;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE TYPE tx_status AS ENUM ('IN_PROGRESS', 'FAILED', 'CANCELED', 'SUCCESS');

create schema if not exists transactions;

-- таблица с транзакциями
create table if not exists transactions.transactions (
    id varchar PRIMARY KEY,
    status tx_status NOT NULL,
    data JSONB NOT NULL, -- мапа с полями сообщения: "field1": "value1", "field2": "value2"
    error varchar,
    instance_id integer, -- id экземпляра приложения, который выполняет транзакцию. может быть null
    failed_driver varchar, -- название драйвера, который не успел выполниться (если есть)
    operation_hash BYTEA NOT NULL, -- hash операции, которая выполняется в транзакции. нужно для того, чтобы можно было не выполнять операцию, если изменилась конфигурация.
    operation_type varchar not null, -- тип операции, которая выполняется в транзакции.
    created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- время создания в UTC
);

-- таблица с запросами на создание / удаление сущностей
CREATE TABLE IF NOT EXISTS transactions.requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tx_id varchar NOT NULL,
    driver_type varchar not null,
    driver_name varchar not null,
    FOREIGN KEY (tx_id) REFERENCES transactions.transactions(id)
);

CREATE INDEX IF NOT EXISTS requests_tx_id_idx ON transactions.requests(tx_id);

ALTER TABLE public.schema_migrations ADD COLUMN IF NOT EXISTS created_at timestamp with time zone NOT NULL DEFAULT now();

CREATE SCHEMA IF NOT EXISTS messages;

-- типы статусов сообщений
CREATE TYPE message_status AS ENUM ('IN_PROGRESS', 'VALIDATED','FAILED');

-- таблица с сообщениями
CREATE TABLE IF NOT EXISTS messages.messages (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL,
    status message_status NOT NULL,
    error varchar,
    driver_type varchar not null,
    driver_name varchar not null,
    instance_id integer,
    operation_hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- время создания в UTC
);
