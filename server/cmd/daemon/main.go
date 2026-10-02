package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

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
	done   chan struct{}
	once   sync.Once
}

type Hub struct {
	id          string
	clients     map[string]*Client
	broadcast   chan []byte
	register    chan *Client
	unregister  chan *Client
	dbQueue     chan TextMsg
	db          *sql.DB
	voiceUsers  map[string]bool
	onlineUsers map[string]string
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

func (c *Client) kick() {
	c.once.Do(func() {
		close(c.done)
		c.conn.Close(websocket.StatusPolicyViolation, "slow client")
	})
}

func (c *Client) trySend(m []byte) {
	select {
	case c.send <- m:
	default:
		c.kick()
	}
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
			h.clients[client.id] = client
			h.onlineUsers[client.id] = client.name
			joinMsg, _ := json.Marshal(Presence{User: client.id, Name: client.name, Action: "join"})
			pkt := append([]byte{0x0B}, joinMsg...)
			for _, c := range h.clients {
				c.trySend(pkt)
			}

		case client := <-h.unregister:
			if _, ok := h.clients[client.id]; ok {
				delete(h.clients, client.id)
				delete(h.onlineUsers, client.id)
				leaveMsg, _ := json.Marshal(Presence{User: client.id, Action: "leave"})
				pkt := append([]byte{0x0B}, leaveMsg...)

				if h.voiceUsers[client.id] {
					delete(h.voiceUsers, client.id)
					vLeaveMsg, _ := json.Marshal(Presence{User: client.id, Action: "leave"})
					vPkt := append([]byte{0x09}, vLeaveMsg...)
					for _, c := range h.clients {
						c.trySend(vPkt)
					}
				}
				for _, c := range h.clients {
					c.trySend(pkt)
				}
				close(client.send)
			}

		case message := <-h.broadcast:
			for _, client := range h.clients {
				client.trySend(message)
			}
		}
	}
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
			if err := json.Unmarshal(payload, &msg); err != nil {
				continue
			}
			msg.Content = strings.TrimSpace(msg.Content)
			if msg.Content == "" || len(msg.Content) > 2000 {
				continue
			}
			msg.ServerID, msg.Sender, msg.Timestamp = c.server, c.id, time.Now().UnixMilli()
			out, _ := json.Marshal(msg)
			c.hub.broadcast <- append([]byte{0x02}, out...)
			c.hub.dbQueue <- msg

		case 0x04:
			rows, _ := c.hub.db.Query(`SELECT sender, content, created_at FROM messages WHERE server_id = ? ORDER BY id DESC LIMIT 50`, c.server)
			var list []TextMsg
			if rows != nil {
				for rows.Next() {
					var m TextMsg
					if rows.Scan(&m.Sender, &m.Content, &m.Timestamp) == nil {
						list = append(list, m)
					}
				}
				rows.Close()
				for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
					list[i], list[j] = list[j], list[i]
				}
			}
			resp, _ := json.Marshal(map[string]any{"c": "general", "msgs": list})
			c.trySend(append([]byte{0x05}, resp...))

			var vUsers []string
			for u := range c.hub.voiceUsers {
				vUsers = append(vUsers, u)
			}
			syncResp, _ := json.Marshal(map[string]any{"voice": vUsers, "online": c.hub.onlineUsers})
			c.trySend(append([]byte{0x0A}, syncResp...))

		case 0x09:
			var vp Presence
			if err := json.Unmarshal(payload, &vp); err == nil {
				vp.User = c.id
				out, _ := json.Marshal(vp)
				pkt := append([]byte{0x09}, out...)
				if vp.Action == "join" {
					c.hub.register <- &Client{id: "voice_join"} // Señal interna
					c.hub.voiceUsers[c.id] = true
				} else {
					c.hub.register <- &Client{id: "voice_leave"}
					delete(c.hub.voiceUsers, c.id)
				}
				c.hub.broadcast <- pkt
			}

		case 0x0C:
			var sp struct {
				U string `json:"u"`
				S bool   `json:"s"`
			}
			if json.Unmarshal(payload, &sp) == nil {
				sp.U = c.id
				out, _ := json.Marshal(sp)
				c.hub.broadcast <- append([]byte{0x0C}, out...)
			}

		case 0x06, 0x07, 0x08:
			var sig SignalPacket
			if json.Unmarshal(payload, &sig) == nil {
				sig.Sender = c.id
				packed, _ := json.Marshal(sig)
				if target, ok := c.hub.clients[sig.Target]; ok {
					target.trySend(append([]byte{opcode}, packed...))
				}
			}
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case m, ok := <-c.send:
			if !ok {
				c.conn.Close(websocket.StatusNormalClosure, "")
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.conn.Write(ctx, websocket.MessageBinary, m)
			cancel()
			if err != nil {
				c.kick()
				return
			}
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.conn.Ping(ctx)
			cancel()
			if err != nil {
				c.kick()
				return
			}
		case <-c.done:
			return
		}
	}
}

func initDB(path string) *sql.DB {
	os.MkdirAll("./data", 0o755)
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		log.Fatal(err)
	}
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
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			return
		}
		q := r.URL.Query()
		serverID := q.Get("server")
		if serverID == "" {
			serverID = "global"
		}
		name := q.Get("name")
		if name == "" || len(name) > 32 {
			conn.Close(websocket.StatusPolicyViolation, "invalid name")
			return
		}

		client := &Client{
			id:     fmt.Sprintf("u_%d", time.Now().UnixNano()),
			name:   name,
			server: serverID,
			conn:   conn,
			hub:    getHub(serverID, db),
			send:   make(chan []byte, 256),
			done:   make(chan struct{}),
		}

		welcome, _ := json.Marshal(map[string]string{"id": client.id, "name": client.name})
		client.trySend(append([]byte{0x01}, welcome...))

		client.hub.register <- client
		go client.writePump()
		client.readPump()
	})

	log.Println("Servidor Go Multi-Sala escuchando en :8080...")
	server := &http.Server{Addr: ":8080", ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}