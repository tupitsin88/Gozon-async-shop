package handler

import (
	"encoding/json"
	"gozon/orders/internal/storage"
	"net/http"

	"github.com/google/uuid"
)

type CreateOrderRequest struct {
	UserID      uuid.UUID `json:"user_id"`
	Amount      int64     `json:"amount"`
	Description string    `json:"description"`
}

type Handler struct {
	repo *storage.OrderRepository
}

func NewHandler(repo *storage.OrderRepository) *Handler {
	return &Handler{repo: repo}
}

// CreateOrder godoc
// @Summary      Создание нового заказа
// @Description  Создает заказ и асинхронно запускает процесс оплаты через Transactional Outbox
// @Tags         orders
// @Accept       json
// @Produce      json
// @Param        input body CreateOrderRequest true "Данные заказа"
// @Success      201  {object}  map[string]interface{} "Успешное создание"
// @Failure      400  {string}  string "Неверные данные"
// @Failure      500  {string}  string "Внутренняя ошибка"
// @Router       /api/orders [post]
func (h *Handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Неверный формат JSON", http.StatusBadRequest)
		return
	}
	if req.Amount <= 0 {
		http.Error(w, "Сумма должна быть положительной", http.StatusBadRequest)
		return
	}
	newOrder := &storage.Order{
		ID:          uuid.New(),
		UserID:      req.UserID,
		Amount:      req.Amount,
		Description: req.Description,
		Status:      "NEW",
	}

	err := h.repo.CreateOrderWithOutbox(r.Context(), newOrder)
	if err != nil {
		http.Error(w, "Ошибка создания заказа: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"order_id": newOrder.ID,
		"status":   "NEW",
		"message":  "Заказ создан и ожидает оплаты",
	})
}
