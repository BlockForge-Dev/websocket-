package httpapi

import "github.com/gorilla/websocket"

type closingSessionHandler struct{}

func (closingSessionHandler) Handle(connection *websocket.Conn, _ Session) {
	_ = connection.Close()
}
