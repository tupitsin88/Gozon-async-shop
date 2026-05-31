# Gozon Async Shop

Учебная микросервисная e-commerce система на Go. Проект показывает асинхронную обработку заказов, разделение данных между сервисами и устойчивую обработку событий через Kafka.

## Что внутри

- **Orders Service** - создает заказы, сохраняет их в PostgreSQL и пишет события в outbox в той же транзакции.
- **Payments Service** - читает события из Kafka, проверяет баланс пользователя, списывает средства и дедуплицирует входящие сообщения через inbox.
- **API Gateway** - Nginx-прокси для HTTP API и WebSocket-соединений.
- **WebSocket client** - простой HTML-клиент для проверки статусов заказа в реальном времени.

## Backend / Infra

- Go
- PostgreSQL для отдельных баз Orders и Payments
- Apache Kafka + Zookeeper
- Transactional Outbox
- Inbox / idempotent consumer
- WebSocket notifications
- Docker Compose
- Swagger-документация для сервисов

## Почему это не просто CRUD

Главный сценарий проекта - отказоустойчивая обработка заказа:

1. Orders Service создает заказ и outbox-событие в одной DB-транзакции.
2. Background worker публикует событие в Kafka.
3. Payments Service получает событие и проверяет inbox, чтобы не обработать один и тот же event повторно.
4. Баланс обновляется атомарной SQL-операцией.
5. Статус заказа возвращается клиенту через WebSocket.

Это не заявка на магическое `exactly-once` в распределенной системе. Реализация делает ставку на практичный подход: transactional outbox, дедупликацию входящих сообщений и идемпотентную обработку платежного события.

## Запуск

Требования:

- Docker
- Docker Compose

```bash
docker compose up -d --build
```

После старта подождите около 30 секунд: PostgreSQL, Kafka и сервисы должны успеть инициализироваться.

## Локальная проверка

После запуска доступны:

```text
API Gateway: http://localhost:8000
Orders Service: http://localhost:8080
Payments Service: http://localhost:8081
```

Swagger-файлы лежат в директориях сервисов:

```text
orders/docs/swagger.yaml
payments/docs/swagger.yaml
```

## Ограничения проекта

- Это учебный проект, а не production-ready платежная система.
- Нет полноценной аутентификации пользователей.
- Нет CI и тестового покрытия; это следующий очевидный шаг для усиления проекта.
- Kafka и PostgreSQL поднимаются локально через Docker Compose без production hardening.
