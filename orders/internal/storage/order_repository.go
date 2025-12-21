package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Order struct {
	ID          uuid.UUID `json:"id"`
	UserID      uuid.UUID `json:"user_id"`
	Amount      int64     `json:"amount"`
	Description string    `json:"description"`
	Status      string    `json:"status"`
}

type OrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

// CreateOrderWithOutbox создает заказ и запись в outbox в ОДНОЙ транзакции.
func (r *OrderRepository) CreateOrderWithOutbox(ctx context.Context, order *Order) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("не удалось начать транзакцию: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO orders (id, user_id, amount, description, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		order.ID, order.UserID, order.Amount, order.Description, order.Status, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("ошибка вставки заказа: %w", err)
	}

	// Формируем событие для Kafka
	eventPayload := map[string]interface{}{
		"order_id": order.ID,
		"user_id":  order.UserID,
		"amount":   order.Amount,
	}
	payloadBytes, _ := json.Marshal(eventPayload)

	// Сохраняем событие в Outbox таблицу
	outboxID := uuid.New()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (id, topic, payload, created_at, processed)
		VALUES ($1, $2, $3, $4, $5)`,
		outboxID, "orders.created", payloadBytes, time.Now(), false,
	)
	if err != nil {
		return fmt.Errorf("ошибка вставки в outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ошибка коммита транзакции: %w", err)
	}
	return nil
}
