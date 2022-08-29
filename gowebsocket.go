package gowebsocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/gofrs/uuid"
	"github.com/gorilla/websocket"
)

type Server struct {
	connectionClient    websocket.Upgrader
	registerEventClient map[string]clientData // map event with clients
	lockingReWrites     *sync.RWMutex
	handler             *Handler
}

// upgrader holds the websocket connection.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type Handler struct {
	RedisClient *redis.Client
}

type clientData struct {
	Indentifier map[string]*websocket.Conn
}

const (
	ROOT       = "default"
	DISCONNECT = "disconnect"
)

func NewServer(h *Handler) *Server {
	return &Server{
		registerEventClient: make(map[string]clientData),
		lockingReWrites:     &sync.RWMutex{},
		handler:             h,
	}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.connectionClient.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	con, err := s.connectionClient.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Client Connection Failed : ", err.Error())
		return
	}
	uuid, _ := uuid.NewV4()
	clientId := uuid.String()
	logMsg := "New Client Connected!"
	if _, ok := s.registerEventClient[ROOT]; ok {
		logMsg = "client connected !"
	}
	s.lockingReWrites.Lock()
	if s.registerEventClient[ROOT].Indentifier == nil {
		var NewClient clientData
		NewClient.Indentifier = make(map[string]*websocket.Conn)
		NewClient.Indentifier[clientId] = con
		s.registerEventClient[ROOT] = NewClient
	} else {
		s.registerEventClient[ROOT].Indentifier[clientId] = con
	}
	s.lockingReWrites.Unlock()
	go s.handler.subscribe(r.Context(), con, ROOT)
	log.Println(logMsg)
	go s.clientConnection(ROOT, clientId, con)
}

func (s *Server) OnEvent(event string, ReaderFunction func(msg []byte, Socket CallBack)) (clientConnection ClientConnection, err error) {
	s.connectionClient.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	con, err := s.connectionClient.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Client Connection Failed : ", err.Error())
		return nil, err
	}

	uuid, _ := uuid.NewV4()
	clientId := uuid.String()
	logMsg := "New Client Connected!"
	if _, ok := s.registerEventClient[event]; ok {
		logMsg = "client connected !"
	}
	s.lockingReWrites.Lock()
	if s.registerEventClient[event].Indentifier == nil {
		var NewClient clientData
		NewClient.Indentifier = make(map[string]*websocket.Conn)
		NewClient.Indentifier[clientId] = con
		s.registerEventClient[event] = NewClient
	} else {
		s.registerEventClient[event].Indentifier[clientId] = con
	}
	s.lockingReWrites.Unlock()
	go s.clientConnection(event, clientId, con)
	log.Println(logMsg)
	return con, nil
}

func (s *Server) clientConnection(Channels, clientId string, clientCon *websocket.Conn) {
	// for {
	// 	_, msg, err := clientCon.ReadMessage()
	// 	if err != nil {
	// 		err = errors.New(err.Error() + " : Failed to read Msg from client")
	// 		// s.disconnectClient(Channels, clientId)
	// 		break
	// 	}
	// 	if string(msg) == DISCONNECT {
	// 		s.disconnectClient(Channels, clientId)
	// 		break
	// 	}
	// }
	for {
		messageType, data, err := clientCon.ReadMessage()
		if err != nil {
			log.Printf("conn.ReadMessage: %v", err)
			return
		}
		if string(data) == DISCONNECT {
			s.disconnectClient(Channels, clientId)
			break
		}
		msg, err := validateSchema(data)
		if err != nil {
			errMsg := fmt.Sprintf("\nunmarshal.ReadMessage (Invalid Payload): %v", err)
			sendResponse(clientCon, messageType, errMsg)
			continue
		}

		err = s.handler.publish(context.Background(), msg)
		if err != nil {
			log.Printf("conn.PublishMessage: %v", err)
			continue
		}

		log.Printf("Host: %v - PublishMessage: %v", "", msg)
	}
}

func (s *Server) disconnectClient(event, clientId string) {
	s.lockingReWrites.Lock()
	defer s.lockingReWrites.Unlock()
	if _, ok := s.registerEventClient[event]; ok {
		if _, ok := s.registerEventClient[event].Indentifier[clientId]; ok {
			s.registerEventClient[event].Indentifier[clientId].Close()
			delete(s.registerEventClient[event].Indentifier, clientId)
			if len(s.registerEventClient[event].Indentifier) == 0 {
				delete(s.registerEventClient, event)
			}
			log.Println("User disconnected!")
		}
	}
}

// SocketHandler echos websocket messages back to the client.
func (h *Handler) SocketHandler(w http.ResponseWriter, r *http.Request) {
	s.connectionClient.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	defer conn.Close()

	if err != nil {
		log.Printf("upgrader.Upgrade: %v", err)
		return
	}

	go h.subscribe(r.Context(), conn, ROOT)

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("conn.ReadMessage: %v", err)
			return
		}

		msg, err := validateSchema(data)
		if err != nil {
			errMsg := fmt.Sprintf("\nunmarshal.ReadMessage (Invalid Payload): %v", err)
			sendResponse(conn, messageType, errMsg)
			continue
		}

		err = h.publish(r.Context(), msg)
		if err != nil {
			log.Printf("conn.PublishMessage: %v", err)
			continue
		}

		log.Printf("Host: %v - PublishMessage: %v", "", msg)
	}
}

func validateSchema(data []byte) (msg []byte, err error) {

	if data != nil && string(data) != "" {
		err = json.Unmarshal(data, &msg)
	}

	// msg.UserID = userID
	// msg.ReceivedBy = hostname

	return msg, err
}

func sendResponse(conn *websocket.Conn, msgType int, message string) {
	if err := conn.WriteMessage(msgType, []byte(message)); err != nil {
		fmt.Printf("\nconn.WriteMessage: %v", err)
		return
	}
}

func (h *Handler) publish(ctx context.Context, msg []byte) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = h.RedisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "Event",
		Values: map[string]interface{}{"data": payload},
	}).Result()

	return err
}

func (h *Handler) subscribe(ctx context.Context, conn *websocket.Conn, event string) {
	subscriber := h.RedisClient.Subscribe(ctx, event)

	defer func() {
		subscriber.Unsubscribe(ctx, event)
		fmt.Println("exiting goroutine subscribe")
	}()

	messagesChan := subscriber.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messagesChan:
			sendResponse(conn, 1, msg.Payload)
		}
	}
}
