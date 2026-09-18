package main

import (
	"sync"

	"github.com/coder/websocket"
)

// registry merkt sich, welcher Socket zu welchem Nutzer gehört.
// Pro Nutzer eine MENGE von Sockets: Handy und Tablet dürfen gleichzeitig
// hängen, und beim Reconnect verdrängt die neue Verbindung die alte nicht.
type registry struct {
	mu    sync.RWMutex
	conns map[int]map[*websocket.Conn]struct{}
}

func newRegistry() *registry {
	return &registry{
		conns: make(map[int]map[*websocket.Conn]struct{}),
	}
}

// add trägt einen Socket ein.
func (reg *registry) add(userID int, conn *websocket.Conn) {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if reg.conns[userID] == nil {
		reg.conns[userID] = make(map[*websocket.Conn]struct{})
	}
	reg.conns[userID][conn] = struct{}{}
}

// remove trägt genau diesen einen Socket aus — nie die Verbindungen,
// die derselbe Nutzer sonst noch offen hat.
// Rückgabe: war das seine letzte Verbindung?
func (reg *registry) remove(userID int, conn *websocket.Conn) bool {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	sockets := reg.conns[userID]
	if sockets == nil {
		return false
	}

	delete(sockets, conn)
	if len(sockets) == 0 {
		delete(reg.conns, userID)
		return true
	}
	return false
}

// socketsOf gibt eine KOPIE der Sockets eines Nutzers zurück, damit der
// Aufrufer schreiben kann, ohne die Sperre festzuhalten.
func (reg *registry) socketsOf(userID int) []*websocket.Conn {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	sockets := reg.conns[userID]
	if len(sockets) == 0 {
		return nil
	}

	out := make([]*websocket.Conn, 0, len(sockets))
	for conn := range sockets {
		out = append(out, conn)
	}
	return out
}

// closeAll leert die Registry und schließt alle Sockets – nur fürs
// Herunterfahren. Rückgabe: alle user_ids, die jetzt abgemeldet gehören,
// und die Zahl der geschlossenen Sockets (mehr als user_ids, wenn jemand
// mit zwei Geräten dranhing).

func (reg *registry) closeAll() ([]int, int) {
	// Map TAUSCHEN statt einzeln löschen: danach laufen die Aufräum-Closures
	// der Verbindungen gegen eine leere Map, ihr remove gibt false, und
	// niemand meldet dieselben Nutzer ein zweites Mal ab. Das erledigt der
	// Aufrufer in einem Rutsch.
	reg.mu.Lock()
	all := reg.conns
	reg.conns = make(map[int]map[*websocket.Conn]struct{})
	reg.mu.Unlock()

	ids := make([]int, 0, len(all))
	var wg sync.WaitGroup
	var n int

	for userID, sockets := range all {
		ids = append(ids, userID)
		for conn := range sockets {
			n++
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
			}()
		}
	}

	wg.Wait()
	return ids, n
}
