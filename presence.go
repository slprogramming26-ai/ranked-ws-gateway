package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/coder/websocket"
)

// Wie lange ein Online-Eintrag gilt und wie oft er erneuert wird.
// 60 zu 25 heißt: zwei verpasste Erneuerungen sind noch verzeihlich.
const (
	presenceTTL     = 60 * time.Second
	presenceRefresh = 25 * time.Second
	// So lange darf der Client für seinen Pong brauchen. Deutlich kürzer als
	// der Takt, sonst überholen sich zwei Runden.
	pongFrist = 10 * time.Second
)

func presenceKey(userID int) string {
	return fmt.Sprintf("ws:online:%d", userID)
}

// markOnline schreibt den Schlüssel MIT Verfallszeit. Der Wert ist die
// Instanz-Kennung: Python wirft sie heute weg, aber sie steht ab Tag eins da,
// damit der spätere Umstieg auf Kanäle pro Instanz nichts an Redis ändert
func (g *Gateway) markOnline(ctx context.Context, userID int) error {
	return g.rdb.Set(ctx, presenceKey(userID), g.cfg.InstanceID, presenceTTL).Err()
}

// markOffline räumt sofort auf, statt bis zu 60 s auf die TTL zu warten.
//
// ACHTUNG für den Aufrufer: hier NIEMALS den Context der Verbindung
// hineingeben. Der ist beim Trennen bereits abgelaufen, das DEL würde
// sofort scheitern – und der Nutzer stünde bis zum TTL-Ablauf fälschlich
// online. Es braucht einen frischen Context.
func (g *Gateway) markOffline(ctx context.Context, userID int) error {
	return g.rdb.Del(ctx, presenceKey(userID)).Err()
}

// heartbeat ist das regelmäßige Lebenszeichen einer Verbindung, in beide
// Richtungen: es erneuert den Presence-Eintrag in Redis UND prüft mit einem
// Ping, ob der Client überhaupt noch da ist.
//
// Ohne den Ping bliebe ein Handy, das im Funkloch verschwindet oder dessen
// App weggewischt wurde, für den Server unbegrenzt "verbunden" – es schickt
// kein Close-Frame, und TCP merkt davon nichts.
//
// Läuft als eigene Goroutine, eine pro Verbindung, und endet mit ctx.
func (g *Gateway) heartbeat(ctx context.Context, userID int, conn *websocket.Conn) {
	ticker := time.NewTicker(presenceRefresh)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := g.markOnline(ctx, userID); err != nil {
				log.Printf("presence: erneuern fehlgeschlagen user=%d: %v", userID, err)
			}

			// Eigene Frist: sonst hinge der Ping an der Lebensdauer der
			// ganzen Verbindung, also womöglich für immer.
			pctx, cancel := context.WithTimeout(ctx, pongFrist)
			err := conn.Ping(pctx)
			cancel()

			if err != nil {
				// Die Verbindung wird gerade sowieso abgebaut – dann ist
				// das kein toter Client, sondern ein normaler Abgang.
				if ctx.Err() != nil {
					return
				}

				// Keine Antwort: tot, auch wenn TCP das noch nicht weiß.
				// Das Schließen weckt den Read-Loop, und dessen Aufräum-
				// Closure erledigt Registry und Presence wie immer.
				log.Printf("kein pong, trenne user=%d: %v", userID, err)
				_ = conn.CloseNow()
				return
			}
		}
	}
}

// clearPresence meldet ab – mit EIGENEM Context, aus dem Grund, der oben
// bei markOffline steht.
func (g *Gateway) clearPresence(userID int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := g.markOffline(ctx, userID); err != nil {
		log.Printf("presence: abmelden fehlgeschlagen user=%d: %v", userID, err)
	}
}

// restorePresence meldet wieder AN – mit eigenem Context, aus demselben
// Grund wie clearPresence.
//
// Gebraucht wird das nur in einem Fall: eine alte Verbindung hat gerade
// abgemeldet, während der Nutzer sich schon neu verbunden hatte. Dann ist
// das DEL zwar bestätigt, aber es hat womöglich das SET der neuen
// Verbindung mitgenommen – also einmal nachlegen.
func (g *Gateway) restorePresence(userID int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := g.markOnline(ctx, userID); err != nil {
		log.Printf("presence: nachtragen fehlgeschlagen user=%d: %v", userID, err)
	}
}

// clearPresenceAll meldet viele Nutzer auf einmal ab – fürs Herunterfahren.
// EIN DEL mit allen Einträgen statt N Einzelaufrufe.
func (g *Gateway) clearPresenceAll(userIDs []int) {
	if len(userIDs) == 0 {
		return
	}

	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, presenceKey(userID))
	}

	// Eigener Context: der Wurzel-Context ist beim Herunterfahren
	// schon abgebrochen, mit ihm würde das DEL sofort scheitern.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := g.rdb.Del(ctx, keys...).Err(); err != nil {
		log.Printf("presence: sammelabmeldung fehlgeschlagen (%d nutzer): %v", len(keys), err)
	}
}

// Notiz für später: Das DEL in clearPresenceAll schickt alle Einträge in
// EINEM Befehl. Redis arbeitet Befehle einzeln und vollständig ab – ein
// sehr großes DEL hält deshalb alles andere so lange auf. Ab ~10.000
// Einträgen in Blöcke à 1000 teilen. Heute nicht relevant.
