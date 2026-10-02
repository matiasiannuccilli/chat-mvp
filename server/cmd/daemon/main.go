package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	_ "github.com/mattn/go-sqlite3"
)

type TextMsg struct {
	ServerID  string `json:"s"`
	ChannelID string `json:"c"`
	Sender    string `json:"u"`
	Content   string `json:"m"`
	Timestamp int64  `json:"t"`
}

type SignalPacket struct {
	Target  string `json:"target"`
	Sender  string `json:"sender,omitempty"`
	Payload string `json:"payload"`
}

type Presence struct {
	User   string `json:"user"`
	Name   string `json:"name"`
	Action string `json:"action"`
}

type Client struct {
	id     string
	name   string
	server string
	conn   *websocket.Conn
	hub    *Hub
	send   chan []byte
}

type Hub struct {
	id          string
	clients     map[string]*Client
	broadcast   chan []byte
	register    chan *Client
	unregister  chan *Client
	dbQueue     chan TextMsg
	db          *sql.DB
	mu          sync.RWMutex
	voiceUsers  map[string]bool
	onlineUsers map[string]string // ID -> Name
}

var (
	managerMu sync.RWMutex
	hubs      = make(map[string]*Hub)
)

func getHub(serverID string, db *sql.DB) *Hub {
	managerMu.Lock()
	defer managerMu.Unlock()
	if h, ok := hubs[serverID]; ok {
		return h
	}
	h := &Hub{
		id:          serverID,
		clients:     make(map[string]*Client),
		broadcast:   make(chan []byte, 256),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		dbQueue:     make(chan TextMsg, 1024),
		db:          db,
		voiceUsers:  make(map[string]bool),
		onlineUsers: make(map[string]string),
	}
	hubs[serverID] = h
	go h.run()
	return h
}

func (h *Hub) run() {
	go func() {
		stmt, _ := h.db.Prepare("INSERT INTO messages(server_id, channel_id, sender, content, created_at) VALUES(?, ?, ?, ?, ?)")
		for msg := range h.dbQueue {
			stmt.Exec(msg.ServerID, msg.ChannelID, msg.Sender, msg.Content, msg.Timestamp)
		}
	}()

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.id] = client
			h.onlineUsers[client.id] = client.name
			h.mu.Unlock()
			joinMsg, _ := json.Marshal(Presence{User: client.id, Name: client.name, Action: "join"})
			h.broadcast <- append([]byte{0x0B}, joinMsg...)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.id]; ok {
				delete(h.clients, client.id)
				delete(h.onlineUsers, client.id)
				close(client.send)
			}
			h.mu.Unlock()
			leaveMsg, _ := json.Marshal(Presence{User: client.id, Action: "leave"})
			h.broadcast <- append([]byte{0x0B}, leaveMsg...)
			if h.voiceUsers[client.id] {
				h.mu.Lock()
				delete(h.voiceUsers, client.id)
				h.mu.Unlock()
				vLeaveMsg, _ := json.Marshal(Presence{User: client.id, Action: "leave"})
				h.broadcast <- append([]byte{0x09}, vLeaveMsg...)
			}

		case message := <-h.broadcast:
			h.mu.RLock()
			for _, client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client.id)
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) getRecentMessages(channelID int) ([]TextMsg, error) {
	rows, err := h.db.Query(`SELECT sender, content, created_at FROM messages WHERE server_id = ? AND channel_id = 'general' ORDER BY id DESC LIMIT 50`, h.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []TextMsg
	for rows.Next() {
		var m TextMsg
		if err := rows.Scan(&m.Sender, &m.Content, &m.Timestamp); err == nil {
			list = append(list, m)
		}
	}
	for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
		list[i], list[j] = list[j], list[i]
	}
	return list, nil
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			break
		}
		if len(data) == 0 {
			continue
		}

		opcode := data[0]
		payload := data[1:]

		switch opcode {
		case 0x02:
			var msg TextMsg
			if err := json.Unmarshal(payload, &msg); err == nil {
				msg.ServerID = c.server
				c.hub.broadcast <- data
				c.hub.dbQueue <- msg
			}
		case 0x04:
			history, _ := c.hub.getRecentMessages(50)
			resp, _ := json.Marshal(map[string]any{"c": "general", "msgs": history})
			c.send <- append([]byte{0x05}, resp...)

			c.hub.mu.RLock()
			var vUsers []string
			for u := range c.hub.voiceUsers {
				vUsers = append(vUsers, u)
			}
			syncResp, _ := json.Marshal(map[string]any{"voice": vUsers, "online": c.hub.onlineUsers})
			c.hub.mu.RUnlock()
			c.send <- append([]byte{0x0A}, syncResp...)

		case 0x09:
			var vp Presence
			if err := json.Unmarshal(payload, &vp); err == nil {
				c.hub.mu.Lock()
				if vp.Action == "join" {
					c.hub.voiceUsers[vp.User] = true
				} else {
					delete(c.hub.voiceUsers, vp.User)
				}
				c.hub.mu.Unlock()
				c.hub.broadcast <- data
			}
		case 0x06, 0x07, 0x08:
			var sig SignalPacket
			if err := json.Unmarshal(payload, &sig); err == nil {
				sig.Sender = c.id
				packed, _ := json.Marshal(sig)
				c.hub.mu.RLock()
				if target, ok := c.hub.clients[sig.Target]; ok {
					target.send <- append([]byte{opcode}, packed...)
				}
				c.hub.mu.RUnlock()
			}
		}
	}
}

func (c *Client) writePump() {
	for message := range c.send {
		c.conn.Write(context.Background(), websocket.MessageBinary, message)
	}
}

func initDB(path string) *sql.DB {
	db, _ := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		server_id TEXT NOT NULL,
		channel_id TEXT NOT NULL,
		sender TEXT NOT NULL,
		content TEXT NOT NULL,
		created_at INTEGER NOT NULL
	);`)
	return db
}

func main() {
	db := initDB("./data/chat_v2.db")
	defer db.Close()

	http.Handle("/", http.FileServer(http.Dir("./public")))
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		q := r.URL.Query()
		serverID := q.Get("server")
		if serverID == "" {
			serverID = "global"
		}
		client := &Client{
			id:     q.Get("user"),
			name:   q.Get("name"),
			server: serverID,
			conn:   conn,
			hub:    getHub(serverID, db),
			send:   make(chan []byte, 64),
		}
		client.hub.register <- client
		go client.writePump()
		client.readPump()
	})

	log.Println("Servidor Go Multi-Sala escuchando en :8080...")
	http.ListenAndServe(":8080", nil)
}