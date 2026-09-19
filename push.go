package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/coder/websocket"
)

// Muss zu app/ws/publisher.py passen (PROTOCOL.md § 6).
const pushChannel = "ws:push"

// So lange darf EIN zäher Socket den Fanout aufhalten.
const pushWriteTimeout = 5 * time.Second

// pushEnvelope ist der Umschlag aus Redis. Payload bleibt RohJSON —
// Go öffnet ihn nie (§ 6: "payload wird nie umgebaut").
type pushEnvelope struct {
	ProtocolVersion int             `json:"protocol_version"`
	Targets         []int           `json:"targets"`
	Payload         json.RawMessage `json:"payload"`
}

// subscribePush lauscht auf ws:push, bis ctx endet.
func (g *Gateway) subscribePush(ctx context.Context) {
	sub := g.rdb.Subscribe(ctx, pushChannel)
	defer sub.Close()

	slog.Info("abonniert", "kanal", pushChannel)

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case redisMsg, ok := <-ch:
			if !ok {
				// Error, weil der Dienst danach weiterläuft, aber keine
				// Pushes mehr zustellt – Strecke C wäre still tot.
				slog.Error("redis-abo beendet")
				return
			}
			g.fanout(ctx, []byte(redisMsg.Payload))
		}
	}
}

// fanout öffnet den Umschlag und schreibt die Payload in die lokalen Sockets.
func (g *Gateway) fanout(ctx context.Context, envelope []byte) {
	var env pushEnvelope
	if err := json.Unmarshal(envelope, &env); err != nil {
		slog.Error("push verworfen: ungültiger umschlag", "err", err)
		return
	}

	if env.ProtocolVersion != protocolVersion {
		slog.Error("push verworfen: falsche protocol_version",
			"bekommen", env.ProtocolVersion, "erwartet", protocolVersion)
		return
	}

	for _, userID := range env.Targets {
		for _, conn := range g.reg.socketsOf(userID) {
			wctx, cancel := context.WithTimeout(ctx, pushWriteTimeout)
			err := conn.Write(wctx, websocket.MessageText, env.Payload)
			cancel()
			if err != nil {
				slog.Warn("push fehlgeschlagen", "user", userID, "err", err)
			}
		}
	}
}
