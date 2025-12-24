package handler

import (
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

// WSHub хранит активные соединения: UserID -> Conn
type WSHub struct {
	clients  map[string]*websocket.Conn
	mu       sync.RWMutex
	upgrader websocket.Upgrader
}

func NewWSHub() *WSHub {
	return &WSHub{
		clients: make(map[string]*websocket.Conn),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// HandleConnection обрабатывает входящие WS соединения
func (h *WSHub) HandleConnection(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	userID = strings.ToLower(userID)
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade WS: %v", err)
		return
	}

	h.mu.Lock()
	h.clients[userID] = conn
	h.mu.Unlock()
	log.Printf("User connected to WS: %s", userID)

	// Держим соединение открытым пока клиент не отключится
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			h.mu.Lock()
			delete(h.clients, userID)
			h.mu.Unlock()
			break
		}
	}
}

// SendNotification отправляет JSON сообщение юзеру
func (h *WSHub) SendNotification(userID string, message interface{}) {
	h.mu.RLock()
	conn, ok := h.clients[userID]
	h.mu.RUnlock()
	if ok {
		if err := conn.WriteJSON(message); err != nil {
			log.Printf("WS send error: %v", err)
			conn.Close()
		} else {
			log.Printf("Push notification sent to user %s", userID)
		}
	}
}
