package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/coder/websocket"
	_ "github.com/mattn/go-sqlite3"
)

// Opcodes (1 byte) + JSON.
// 0x01 hello/welcome  0x02 chat  0x03 crear canal  0x05 historial  0x06-08 señalización WebRTC
// 0x09 voz (entrar/salir)  0x0A estado inicial  0x0B presencia  0x0C hablando  0x0D susurro
// 0x0E lista de canales  0x0F error

const (
	maxChannels = 8
	maxName     = 24
)

type Channel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Presence struct {
	User   string `json:"user"`
	Name   string `json:"name,omitempty"`
	Action string `json:"action"`
	Ch     string `json:"ch,omitempty"`
}

type Signal struct {
	Target  string `json:"target"`
	Sender  string `json:"sender,omitempty"`
	Payload string `json:"payload"`
}

var cfg struct {
	baseURL  string
	maxUsers int
	maxRooms int
	origins  []string
	ice      []map[string]any
}

var db *sql.DB

// ---------- utilidades ----------

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		log.Fatal(err)
	}
	return b
}
func randID(n int) string { return hex.EncodeToString(randBytes(n)) }
func randToken() string   { return base64.RawURLEncoding.EncodeToString(randBytes(16)) }
func hashTok(t string) string {
	h := sha256.Sum256([]byte(t))
	return hex.EncodeToString(h[:])
}

func frame(op byte, v any) []byte {
	b, _ := json.Marshal(v)
	out := make([]byte, 1+len(b))
	out[0] = op
	copy(out[1:], b)
	return out
}

func cleanName(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '<' || r == '>' {
			return -1
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > maxName {
		s = string(r[:maxName])
	}
	return strings.TrimSpace(s)
}

type bucket struct {
	tokens, rate, burst float64
	last                time.Time
}

func newBucket(rate, burst float64) *bucket { return &bucket{burst, rate, burst, time.Now()} }
func (b *bucket) allow() bool {
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// ---------- base de datos ----------

type dbMsg struct {
	room, sender, content string
	ts                    int64
}

var dbQueue = make(chan dbMsg, 2048)

func initDB(path string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}
	var err error
	db, err = sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, server_id TEXT NOT NULL, channel_id TEXT NOT NULL, sender TEXT NOT NULL, content TEXT NOT NULL, created_at INTEGER NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(server_id, id)`,
		`CREATE TABLE IF NOT EXISTS rooms (id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL DEFAULT 0, last_active INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, room_id TEXT NOT NULL, name TEXT NOT NULL)`,
	} {
		if _, err := db.Exec(q); err != nil {
			log.Fatal(err)
		}
	}
	db.Exec(`ALTER TABLE rooms ADD COLUMN last_active INTEGER NOT NULL DEFAULT 0`) // bases viejas; si ya existe, el error se ignora
}

func dbWriter() {
	stmt, err := db.Prepare(`INSERT INTO messages(server_id, channel_id, sender, content, created_at) VALUES(?, 'general', ?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for m := range dbQueue {
		if _, err := stmt.Exec(m.room, m.sender, m.content, m.ts); err != nil {
			log.Println("db:", err)
		}
	}
}

type Room struct {
	ID, Name, TokenHash string
	Expires             int64
}

func loadRoom(id string) (Room, bool) {
	var r Room
	err := db.QueryRow(`SELECT id, name, token_hash, expires_at FROM rooms WHERE id = ?`, id).Scan(&r.ID, &r.Name, &r.TokenHash, &r.Expires)
	return r, err == nil
}

var (
	errExists = errors.New("ya existe un servidor con ese nombre")
	errFull   = errors.New("hay demasiados servidores")
)

func createRoom(name string, hours int) (string, string, error) {
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM rooms`).Scan(&n); err != nil {
		return "", "", err
	}
	if n >= cfg.maxRooms {
		return "", "", errFull
	}
	var dup int
	if err := db.QueryRow(`SELECT COUNT(*) FROM rooms WHERE lower(name) = lower(?)`, name).Scan(&dup); err != nil {
		return "", "", err
	}
	if dup > 0 {
		return "", "", errExists
	}
	id, tok := randID(6), randToken()
	var exp int64
	if hours > 0 {
		exp = time.Now().Add(time.Duration(hours) * time.Hour).Unix()
	}
	tx, err := db.Begin()
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO rooms(id, name, token_hash, created_at, expires_at, last_active) VALUES(?,?,?,?,?,?)`, id, name, hashTok(tok), time.Now().Unix(), exp, time.Now().Unix()); err != nil {
		return "", "", err
	}
	if _, err := tx.Exec(`INSERT INTO channels(id, room_id, name) VALUES(?,?,?)`, randID(4), id, "General"); err != nil {
		return "", "", err
	}
	return id, tok, tx.Commit()
}

func touchRoom(id string) {
	db.Exec(`UPDATE rooms SET last_active = ? WHERE id = ?`, time.Now().Unix(), id)
}

// Borra servidores sin actividad en 14 días (y sin usuarios conectados).
func pruneRooms() {
	for range time.Tick(time.Hour) {
		cutoff := time.Now().Add(-14 * 24 * time.Hour).Unix()
		rows, err := db.Query(`SELECT id FROM rooms WHERE max(last_active, created_at) < ?`, cutoff)
		if err != nil {
			continue
		}
		var ids []string
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				ids = append(ids, id)
			}
		}
		rows.Close()
		for _, id := range ids {
			hubsMu.Lock()
			if h := hubs[id]; h != nil {
				h.mu.Lock()
				busy := len(h.clients) > 0
				h.mu.Unlock()
				if busy {
					hubsMu.Unlock()
					continue
				}
				delete(hubs, id)
			}
			hubsMu.Unlock()
			for _, q := range []string{`DELETE FROM messages WHERE server_id = ?`, `DELETE FROM channels WHERE room_id = ?`, `DELETE FROM rooms WHERE id = ?`} {
				db.Exec(q, id)
			}
		}
	}
}

func loadChannels(room string) []Channel {
	out := []Channel{}
	rows, err := db.Query(`SELECT id, name FROM channels WHERE room_id = ? ORDER BY rowid`, room)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var c Channel
		if rows.Scan(&c.ID, &c.Name) == nil {
			out = append(out, c)
		}
	}
	return out
}

func history(room string) []map[string]any {
	out := []map[string]any{}
	rows, err := db.Query(`SELECT sender, content, created_at FROM messages WHERE server_id = ? ORDER BY id DESC LIMIT 50`, room)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var n, m string
		var t int64
		if rows.Scan(&n, &m, &t) == nil {
			out = append(out, map[string]any{"n": n, "m": m, "t": t})
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// ---------- hub y clientes ----------

type Client struct {
	id, name, resume string
	conn             *websocket.Conn
	hub              *Hub
	send             chan []byte
	done             chan struct{}
	once             sync.Once
}

// kick cierra la conexión una sola vez. Nunca se cierra c.send (evita "send on closed channel").
func (c *Client) kick() {
	c.once.Do(func() {
		close(c.done)
		c.conn.CloseNow()
	})
}

// trySend nunca bloquea: si la cola está llena, el cliente es lento y se expulsa.
func (c *Client) trySend(m []byte) {
	select {
	case c.send <- m:
	default:
		c.kick()
	}
}

type Hub struct {
	id, name string
	mu       sync.Mutex
	clients  map[string]*Client
	voice    map[string]string          // userID -> channelID
	whisper  map[string]map[string]bool // emisor -> destinatarios
	channels []Channel
}

var (
	hubsMu sync.Mutex
	hubs   = map[string]*Hub{}
)

func getHub(r Room) *Hub {
	hubsMu.Lock()
	defer hubsMu.Unlock()
	if h, ok := hubs[r.ID]; ok {
		return h
	}
	h := &Hub{id: r.ID, name: r.Name, clients: map[string]*Client{}, voice: map[string]string{}, whisper: map[string]map[string]bool{}, channels: loadChannels(r.ID)}
	hubs[r.ID] = h
	return h
}

// Los métodos con sufijo "L" requieren h.mu tomado.
func (h *Hub) bcastL(m []byte) {
	for _, c := range h.clients {
		c.trySend(m)
	}
}
func (h *Hub) broadcast(m []byte) { h.mu.Lock(); h.bcastL(m); h.mu.Unlock() }

func (h *Hub) uniqueNameL(base string) string {
	n := base
	for i := 2; ; i++ {
		taken := false
		for _, o := range h.clients {
			if strings.EqualFold(o.name, n) {
				taken = true
				break
			}
		}
		if !taken {
			return n
		}
		suf := " #" + strconv.Itoa(i)
		r := []rune(base)
		if len(r)+len(suf) > maxName {
			r = r[:maxName-len(suf)]
		}
		n = string(r) + suf
	}
}

func (h *Hub) leaveVoiceL(id string) {
	delete(h.whisper, id)
	if _, ok := h.voice[id]; ok {
		delete(h.voice, id)
		h.bcastL(frame(0x09, Presence{User: id, Action: "leave"}))
	}
}

func (h *Hub) add(c *Client, name, resume string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	var old *Client
	if resume != "" {
		for _, o := range h.clients {
			if subtle.ConstantTimeCompare([]byte(o.resume), []byte(resume)) == 1 {
				old = o
				break
			}
		}
	}
	if old == nil && len(h.clients) >= cfg.maxUsers {
		return false
	}
	if old != nil { // reconexión: conserva identidad, reemplaza la conexión vieja
		c.id, c.resume = old.id, old.resume
		delete(h.clients, old.id)
		h.leaveVoiceL(old.id)
		old.kick()
	}
	c.name = h.uniqueNameL(name)
	h.clients[c.id] = c
	return true
}

func (h *Hub) remove(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[c.id] != c {
		return
	}
	delete(h.clients, c.id)
	h.leaveVoiceL(c.id)
	h.bcastL(frame(0x0B, Presence{User: c.id, Action: "leave"}))
}

func (h *Hub) setWhisper(c *Client, on bool, users, chans []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.voice[c.id]; !ok {
		return
	}
	to := map[string]bool{}
	if on {
		if len(users) > 20 {
			users = users[:20]
		}
		if len(chans) > maxChannels {
			chans = chans[:maxChannels]
		}
		for _, u := range users {
			if _, v := h.voice[u]; v && u != c.id {
				to[u] = true
			}
		}
		for _, ch := range chans {
			for u, uc := range h.voice {
				if uc == ch && u != c.id {
					to[u] = true
				}
			}
		}
	}
	for u := range h.whisper[c.id] {
		if !to[u] {
			if o := h.clients[u]; o != nil {
				o.trySend(frame(0x0D, map[string]any{"from": c.id, "on": false}))
			}
		}
	}
	list := make([]string, 0, len(to))
	for u := range to {
		list = append(list, u)
		if o := h.clients[u]; o != nil {
			o.trySend(frame(0x0D, map[string]any{"from": c.id, "on": true}))
		}
	}
	if on {
		h.whisper[c.id] = to
	} else {
		delete(h.whisper, c.id)
	}
	c.trySend(frame(0x0D, map[string]any{"from": c.id, "on": on, "to": list}))
}

func (c *Client) readPump() {
	h := c.hub
	defer func() { h.remove(c); c.kick() }()
	gen, chat := newBucket(200, 400), newBucket(3, 6)
	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			return
		}
		if len(data) < 1 {
			continue
		}
		if !gen.allow() {
			return // flood: se corta la conexión
		}
		op, p := data[0], data[1:]
		switch op {
		case 0x02:
			var in struct{ M string }
			if !chat.allow() || json.Unmarshal(p, &in) != nil {
				continue
			}
			m := strings.TrimSpace(in.M)
			if m == "" || utf8.RuneCountInString(m) > 1000 {
				continue
			}
			ts := time.Now().UnixMilli()
			h.broadcast(frame(0x02, map[string]any{"u": c.id, "n": c.name, "m": m, "t": ts}))
			select {
			case dbQueue <- dbMsg{h.id, c.name, m, ts}:
			default:
				log.Println("cola de db llena, mensaje no persistido")
			}
		case 0x03:
			var in struct{ Name string }
			name := ""
			if json.Unmarshal(p, &in) == nil {
				name = cleanName(in.Name)
			}
			if name == "" || !chat.allow() {
				continue
			}
			h.mu.Lock()
			if len(h.channels) >= maxChannels {
				c.trySend(frame(0x0F, map[string]string{"msg": "Límite de salas alcanzado"}))
			} else {
				ch := Channel{ID: randID(4), Name: name}
				if _, err := db.Exec(`INSERT INTO channels(id, room_id, name) VALUES(?,?,?)`, ch.ID, h.id, ch.Name); err != nil {
					log.Println("db:", err)
				} else {
					h.channels = append(h.channels, ch)
					h.bcastL(frame(0x0E, map[string]any{"channels": h.channels}))
				}
			}
			h.mu.Unlock()
		case 0x09:
			var in struct{ Action, Ch string }
			if json.Unmarshal(p, &in) != nil {
				continue
			}
			h.mu.Lock()
			if in.Action == "join" {
				for _, ch := range h.channels {
					if ch.ID == in.Ch {
						h.voice[c.id] = ch.ID
						h.bcastL(frame(0x09, Presence{User: c.id, Action: "join", Ch: ch.ID}))
						break
					}
				}
			} else {
				h.leaveVoiceL(c.id)
			}
			h.mu.Unlock()
		case 0x06, 0x07, 0x08:
			var sig Signal
			if json.Unmarshal(p, &sig) != nil {
				continue
			}
			h.mu.Lock()
			_, mine := h.voice[c.id]
			_, theirs := h.voice[sig.Target]
			t := h.clients[sig.Target]
			h.mu.Unlock()
			if mine && theirs && t != nil && t != c {
				sig.Sender = c.id
				t.trySend(frame(op, sig))
			}
		case 0x0C:
			var in struct{ S bool }
			h.mu.Lock()
			if _, ok := h.voice[c.id]; ok && json.Unmarshal(p, &in) == nil {
				h.bcastL(frame(0x0C, map[string]any{"u": c.id, "s": in.S}))
			}
			h.mu.Unlock()
		case 0x0D:
			var in struct {
				On           bool
				Users, Chans []string
			}
			if json.Unmarshal(p, &in) == nil {
				h.setWhisper(c, in.On, in.Users, in.Chans)
			}
		}
	}
}

func (c *Client) writePump() {
	t := time.NewTicker(25 * time.Second)
	defer t.Stop()
	for {
		select {
		case m := <-c.send:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := c.conn.Write(ctx, websocket.MessageBinary, m)
			cancel()
			if err != nil {
				c.kick()
				return
			}
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

// ---------- HTTP ----------

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: cfg.origins})
	if err != nil {
		return
	}
	conn.SetReadLimit(64 << 10)
	fail := func(msg string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn.Write(ctx, websocket.MessageBinary, frame(0x0F, map[string]string{"msg": msg}))
		conn.Close(websocket.StatusPolicyViolation, "denied")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, data, err := conn.Read(ctx)
	cancel()
	if err != nil || len(data) < 2 || data[0] != 0x01 {
		conn.CloseNow()
		return
	}
	var in struct{ Room, Token, Name, Resume string }
	if json.Unmarshal(data[1:], &in) != nil {
		conn.CloseNow()
		return
	}
	room, ok := loadRoom(in.Room)
	if !ok || subtle.ConstantTimeCompare([]byte(hashTok(in.Token)), []byte(room.TokenHash)) != 1 || (room.Expires > 0 && time.Now().Unix() > room.Expires) {
		fail("Link de invitación inválido o vencido")
		return
	}
	name := cleanName(in.Name)
	if name == "" {
		fail("Elegí un nombre")
		return
	}
	h := getHub(room)
	c := &Client{id: randID(8), resume: randID(16), conn: conn, hub: h, send: make(chan []byte, 128), done: make(chan struct{})}
	if !h.add(c, name, in.Resume) {
		fail("La sala está llena")
		return
	}
	touchRoom(h.id)
	go c.writePump()
	c.trySend(frame(0x01, map[string]any{"id": c.id, "name": c.name, "resume": c.resume, "room": h.name, "ice": cfg.ice}))
	h.mu.Lock()
	online := map[string]string{}
	for id, o := range h.clients {
		online[id] = o.name
	}
	c.trySend(frame(0x0A, map[string]any{"online": online, "voice": h.voice, "channels": h.channels}))
	h.bcastL(frame(0x0B, Presence{User: c.id, Name: c.name, Action: "join"}))
	h.mu.Unlock()
	c.trySend(frame(0x05, map[string]any{"msgs": history(h.id)}))
	c.readPump()
}

var (
	ipMu   sync.Mutex
	ipLims = map[string]*bucket{}
)

func clientIP(r *http.Request) string {
	if ip := r.Header.Get("CF-Connecting-IP"); ip != "" { // detrás de cloudflared
		return ip
	}
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return h
}

// 3 servidores seguidos por IP y luego 1 por minuto.
func allowCreate(ip string) bool {
	ipMu.Lock()
	defer ipMu.Unlock()
	if len(ipLims) > 10000 {
		ipLims = map[string]*bucket{}
	}
	b := ipLims[ip]
	if b == nil {
		b = newBucket(1.0/60, 3)
		ipLims[ip] = b
	}
	return b.allow()
}

func apiRooms(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	if !allowCreate(clientIP(r)) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	var in struct {
		Name  string
		Hours int
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in) != nil || cleanName(in.Name) == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id, tok, err := createRoom(cleanName(in.Name), in.Hours)
	switch {
	case errors.Is(err, errExists):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case errors.Is(err, errFull):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	case err != nil:
		log.Println("createRoom:", err)
		http.Error(w, "error interno", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"id": id, "token": tok})
}

func secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; media-src 'self' blob:; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func main() {
	cfg.maxRooms, _ = strconv.Atoi(env("MAX_ROOMS", "200"))
	if cfg.maxRooms < 1 {
		cfg.maxRooms = 200
	}
	cfg.baseURL = strings.TrimRight(env("BASE_URL", "http://localhost:8080"), "/")
	cfg.maxUsers, _ = strconv.Atoi(env("MAX_USERS", "10"))
	if cfg.maxUsers < 2 {
		cfg.maxUsers = 10
	}
	for _, o := range strings.Split(env("ALLOWED_ORIGINS", ""), ",") {
		if o = strings.TrimSpace(o); o != "" {
			cfg.origins = append(cfg.origins, o)
		}
	}
	cfg.ice = []map[string]any{{"urls": []string{"stun:stun.l.google.com:19302", "stun:stun.cloudflare.com:3478"}}}
	if t := env("TURN_URLS", ""); t != "" { // ej: turn:tu.dominio:3478?transport=udp,turns:tu.dominio:443?transport=tcp
		cfg.ice = append(cfg.ice, map[string]any{"urls": strings.Split(t, ","), "username": env("TURN_USER", ""), "credential": env("TURN_PASS", "")})
	}

	initDB(env("DB_PATH", "./data/chat_v3.db"))
	defer db.Close()
	go dbWriter()
	go pruneRooms()

	var n int
	if db.QueryRow(`SELECT COUNT(*) FROM rooms`).Scan(&n) == nil && n == 0 {
		id, tok, err := createRoom("General", 0)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Primer servidor creado. Link de invitación: %s/?r=%s#%s", cfg.baseURL, id, tok)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler)
	mux.HandleFunc("/api/rooms", apiRooms)
	mux.Handle("/", http.FileServer(http.Dir("./public")))

	srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: secure(mux), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	log.Println("Servidor escuchando en", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}