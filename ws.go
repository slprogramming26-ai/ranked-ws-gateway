package main

import (
	"context"
	"log"
	"net/http"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/redis/go-redis/v9"
)

// Gateway hält alles, was die Handler brauchen.
type Gateway struct {
	cfg *Config
	rdb *redis.Client
	hc  *http.Client
	reg *registry
	ips *ipLimiter
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	// Vor dem Upgrade: ein abgelehnter Versuch soll billig bleiben.
	if !g.ips.allow(clientIP(r)) {
		log.Print("verbindung abgelehnt: ratelimit")
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("accept fehlgeschlagen: %v", err)
		return
	}
	defer conn.CloseNow()

	userID, err := userIDFromToken(r.URL.Query().Get("token"), g.cfg.SecretKey)
	if err != nil {
		log.Printf("token abgelehnt: %v", err)
		conn.Close(websocket.StatusPolicyViolation, "invalid token")
		return
	}

	// Ein Context, der genau so lange lebt wie diese Verbindung.
	// Ein Context, der genau so lange lebt wie diese Verbindung.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Erst eintragen, DANN anmelden. So gilt immer: wer in der Registry
	// steht, ist auch online gemeldet – nie umgekehrt. Darauf baut die
	// Nachprüfung unten auf.
	g.reg.add(userID, conn)
	defer func() {
		// Erst keepPresence stoppen – sonst könnte ein letzter Tick
		// den Eintrag direkt nach dem DEL wieder anlegen.
		cancel()

		// Offline nur, wenn das seine LETZTE Verbindung war –
		// sonst würde das Weglegen des Tablets das Handy mit abmelden.
		if g.reg.remove(userID, conn) {
			g.clearPresence(userID)

			// Das DEL ist jetzt bestätigt. Hat der Nutzer trotzdem wieder
			// einen Socket, hat er sich während des Abmeldens neu verbunden
			// und wir haben ihm womöglich gerade sein SET weggelöscht.
			if len(g.reg.socketsOf(userID)) > 0 {
				g.restorePresence(userID)
			}
		}
	}()

	if err := g.markOnline(ctx, userID); err != nil {
		log.Printf("presence: anmelden fehlgeschlagen user=%d: %v", userID, err)
	}

	go g.heartbeat(ctx, userID, conn)

	log.Printf("verbunden: user=%d", userID)

	// Selbstschutz vor dem Parsen: mehr kann eine gültige Nachricht nicht sein.
	conn.SetReadLimit(64 * 1024)

	// Ein eigener Eimer je Verbindung. Er lebt und stirbt mit ihr –
	// kein geteilter Zustand, nichts aufzuräumen.
	lim := newMessageLimiter()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			log.Printf("getrennt: user=%d: %v", userID, err)
			return
		}

		waitCtx, waitCancel := context.WithTimeout(ctx, maxWait)
		err = lim.Wait(waitCtx)
		waitCancel()
		if err != nil {
			if ctx.Err() != nil {
				return // Verbindung weg, nicht das Limit
			}
			log.Printf("ratelimit user=%d", userID)
			if werr := wsjson.Write(ctx, conn, newErrorMessage("rate limit exceeded")); werr != nil {
				return
			}
			continue
		}

		msg, err := parseClientMessage(data)
		if err != nil {
			log.Printf("formfehler von user=%d: %v", userID, err)
			if werr := wsjson.Write(ctx, conn, newErrorMessage(err.Error())); werr != nil {
				log.Printf("antwort fehlgeschlagen user=%d: %v", userID, werr)
				return
			}
			continue
		}

		raw, err := g.postToBackend(ctx, userID, msg)
		if err != nil {
			// Störung, kein Protokoll-Inhalt (§ 5.1). Details bleiben im Log,
			// der Client bekommt trotzdem eine Antwort (§ 4.4: nie keine).
			log.Printf("strecke b fehlgeschlagen user=%d kind=%s: %v", userID, msg.Kind, err)
			if werr := wsjson.Write(ctx, conn, newErrorMessage("backend unavailable")); werr != nil {
				log.Printf("antwort fehlgeschlagen user=%d: %v", userID, werr)
				return
			}
			continue
		}

		// 200 -> Body WÖRTLICH in den Socket. Kein Parsen, kein Umbauen (§ 5.1, § 6).
		if werr := conn.Write(ctx, websocket.MessageText, raw); werr != nil {
			log.Printf("antwort fehlgeschlagen user=%d: %v", userID, werr)
			return
		}
	}

}
