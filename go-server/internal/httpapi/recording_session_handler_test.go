package httpapi

import "github.com/gorilla/websocket"

type recordingSessionHandler struct {
	session chan Session
	release chan struct{}
}

func (h *recordingSessionHandler) Handle(connection *websocket.Conn, session Session) {
	h.session <- session
	<-h.release
	_ = connection.Close()
}
