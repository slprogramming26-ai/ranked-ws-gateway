package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Die Version aus PROTOCOL.md § 3. Python lehnt jede andere Zahl mit 422 ab.
const protocolVersion = 1

// internalRequest ist der Body für POST /internal/ws/dm bzw. /group
// (PROTOCOL.md § 5). Eine Struktur für beide Strecken — der Unterschied
// ist nur, ob key_version dabei ist.
type internalRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	SenderID        int    `json:"sender_id"`
	To              int    `json:"to"`
	Message         string `json:"message"`
	KeyVersion      *int   `json:"key_version,omitempty"`
	ClientMsgID     string `json:"client_msg_id,omitempty"`
}

// newBackendClient baut den HTTP-Client für Strecke B.
//
// Kein http.DefaultClient: der hat KEIN Timeout, ein hängendes Backend
// würde Goroutines für immer festhalten. Das Timeout setzen wir pro
// Anfrage per Context (Block 2), deshalb steht hier kein Client.Timeout.
func newBackendClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 100
	return &http.Client{Transport: tr}
}

// postToBackend schickt eine formgeprüfte Client-Nachricht an Python und gibt
// dessen Antwort-Body WÖRTLICH zurück (PROTOCOL.md § 5.1: 200 = durchreichen).
func (g *Gateway) postToBackend(ctx context.Context, senderID int, msg *ClientMessage) ([]byte, error) {
	body, err := json.Marshal(internalRequest{
		ProtocolVersion: protocolVersion,
		SenderID:        senderID,
		To:              msg.To,
		Message:         msg.Message,
		KeyVersion:      msg.KeyVersion,
		ClientMsgID:     msg.ClientMsgID,
	})
	if err != nil {
		return nil, fmt.Errorf("request bauen: %w", err)
	}

	// Eigenes Zeitlimit, ABGELEITET vom Verbindungs-Context: die Anfrage bricht
	// ab, wenn Python zu lange braucht ODER der Client vorher weggeht.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// kind ist nach parseClientMessage garantiert "dm" oder "group" und heißt
	// genau wie der Endpoint (PROTOCOL.md § 5).
	url := g.cfg.BackendURL + "/internal/ws/" + msg.Kind

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("request bauen: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-WS-Secret", g.cfg.WSInternalSecret)

	resp, err := g.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("backend nicht erreichbar: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, fmt.Errorf("antwort lesen: %w", err)
	}

	// 200 -> fachliches Ergebnis, wörtlich weiterreichen. Alles andere ist eine
	// Störung (401 Secret, 422 Repos auseinander) und NIE Protokoll-Inhalt.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backend antwortete %d: %.200s", resp.StatusCode, raw)
	}

	return raw, nil
}
