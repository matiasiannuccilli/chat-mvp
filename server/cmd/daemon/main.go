package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/coder/websocket"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vmihailenco/msgpack/v5"
)

type TextMsg struct {
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

type Client struct {
	id   string
	conn *websocket.Conn
	hub  *Hub
	send chan []byte
}

type Hub struct {
	clients    map[string]*Client
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	dbQueue    chan TextMsg
	db         *sql.DB
	mu         sync.RWMutex
}

func newHub(db *sql.DB) *Hub {
	return &Hub{
		clients:    make(map[string]*Client),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		dbQueue:    make(chan TextMsg, 1024),
		db:         db,
	}
}

func (h *Hub) run() {
	go func() {
		stmt, err := h.db.Prepare("INSERT INTO messages(channel_id, sender, content, created_at) VALUES(?, ?, ?, ?)")
		if err != nil {
			log.Fatalf("Error preparando stmt SQL: %v", err)
		}
		for msg := range h.dbQueue {
			stmt.Exec(msg.ChannelID, msg.Sender, msg.Content, msg.Timestamp)
		}
	}()

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.id] = client
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.id]; ok {
				delete(h.clients, client.id)
				close(client.send)
			}
			h.mu.Unlock()
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

func (h *Hub) getRecentMessages(channelID string, limit int) ([]TextMsg, error) {
	query := `SELECT channel_id, sender, content, created_at FROM messages WHERE channel_id = ? ORDER BY id DESC LIMIT ?`
	rows, err := h.db.Query(query, channelID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []TextMsg
	for rows.Next() {
		var m TextMsg
		if err := rows.Scan(&m.ChannelID, &m.Sender, &m.Content, &m.Timestamp); err == nil {
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
				c.hub.broadcast <- data
				c.hub.dbQueue <- msg
			}
		case 0x04:
			var req struct {
				ChannelID string `msgpack:"c"`
				Limit     int    `msgpack:"l"`
			}
			if err := json.Unmarshal(payload, &req); err == nil {
				if req.Limit <= 0 || req.Limit > 100 {
					req.Limit = 50
				}
				history, _ := c.hub.getRecentMessages(req.ChannelID, req.Limit)
				resp, _ := json.Marshal(map[string]any{"c": req.ChannelID, "msgs": history})
				c.send <- append([]byte{0x05}, resp...)
			}
		case 0x09:
			c.hub.broadcast <- data
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
		err := c.conn.Write(context.Background(), websocket.MessageBinary, message)
		if err != nil {
			break
		}
	}
}

func initDB(path string) *sql.DB {
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		log.Fatalf("Error abriendo SQLite: %v", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		channel_id TEXT NOT NULL,
		sender TEXT NOT NULL,
		content TEXT NOT NULL,
		created_at INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_channel_created ON messages(channel_id, id DESC);
	CREATE TRIGGER IF NOT EXISTS prune_5k_limit
	AFTER INSERT ON messages
	BEGIN
		DELETE FROM messages 
		WHERE channel_id = NEW.channel_id 
		  AND id <= (
			  SELECT id FROM messages 
			  WHERE channel_id = NEW.channel_id 
			  ORDER BY id DESC 
			  LIMIT 1 OFFSET 5000
		  );
	END;
	`
	if _, err := db.Exec(schema); err != nil {
		log.Fatalf("Error inicializando esquema: %v", err)
	}
	return db
}

func main() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./data/chat.db"
	}

	db := initDB(dbPath)
	defer db.Close()

	hub := newHub(db)
	go hub.run()

	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}

		clientID := r.URL.Query().Get("user")
		if clientID == "" {
			clientID = fmt.Sprintf("peer-%d", time.Now().UnixNano())
		}

		client := &Client{
			id:   clientID,
			conn: conn,
			hub:  hub,
			send: make(chan []byte, 64),
		}

		hub.register <- client
		go client.writePump()
		client.readPump()
	})

	log.Println("Servidor Go escuchando en :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}