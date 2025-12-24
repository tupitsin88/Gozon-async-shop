package handler

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

type Handler struct {
	db *sql.DB
}

func NewHandler(db *sql.DB) *Handler {
	return &Handler{db: db}
}

type AccountRequest struct {
	UserID uuid.UUID `json:"user_id"`
}

type DepositRequest struct {
	UserID uuid.UUID `json:"user_id"`
	Amount int64     `json:"amount"`
}

// CreateAccount godoc
// @Summary      Создание счета
// @Description  Создает новый счет для пользователя (баланс 0)
// @Tags         payments
// @Accept       json
// @Produce      json
// @Param        input body AccountRequest true "User ID"
// @Success      201  {string}  string "Account created"
// @Failure      400  {string}  string "Error"
// @Router       /api/payments/create_account [post]
func (h *Handler) CreateAccount(w http.ResponseWriter, r *http.Request) {
	var req AccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad JSON", http.StatusBadRequest)
		return
	}
	_, err := h.db.Exec("INSERT INTO accounts (user_id, balance) VALUES ($1, 0)", req.UserID)
	if err != nil {
		http.Error(w, "Error creating account (maybe exists?): "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"message": "Account created"}`))
}

// Deposit godoc
// @Summary      Пополнение счета
// @Description  Добавляет деньги на счет (идемпотентность не реализована в рамках ДЗ для простоты API)
// @Tags         payments
// @Accept       json
// @Produce      json
// @Param        input body DepositRequest true "Данные пополнения"
// @Success      200  {string}  string "Updated"
// @Failure      500  {string}  string "Error"
// @Router       /api/payments/deposit [post]
func (h *Handler) Deposit(w http.ResponseWriter, r *http.Request) {
	var req DepositRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad JSON", http.StatusBadRequest)
		return
	}
	if req.Amount <= 0 {
		http.Error(w, "Amount must be positive", http.StatusBadRequest)
		return
	}
	_, err := h.db.Exec("UPDATE accounts SET balance = balance + $1 WHERE user_id = $2", req.Amount, req.UserID)
	if err != nil {
		http.Error(w, "Error updating balance: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"message": "Balance updated"}`))
}

// GetBalance godoc
// @Summary      Баланс счета
// @Description  Получить текущий баланс
// @Tags         payments
// @Param        user_id query string true "User UUID"
// @Success      200  {object}  map[string]int64
// @Router       /api/payments/balance [get]
func (h *Handler) GetBalance(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	var balance int64
	err := h.db.QueryRow("SELECT balance FROM accounts WHERE user_id = $1", userID).Scan(&balance)
	if err != nil {
		http.Error(w, "Account not found", http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(map[string]int64{"balance": balance})
}
