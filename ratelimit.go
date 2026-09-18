package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Nachrichten je Verbindung: 5 pro Sekunde, kurze Spitzen bis 10.
// Der Burst fängt die Warteschlange ab, die eine App nach dem
// Reconnect auf einmal rausschickt.
const (
	msgRate  = 5
	msgBurst = 10

	// So lange darf der Read-Loop auf Kontingent warten. MUSS kleiner sein als
	// der Abstand zwischen zwei Marken (1/msgRate = 200 ms): unsere Schleife
	// fragt immer nur nach EINER Marke und wartet danach nie länger als genau
	// diesen Abstand. Bei 2 s scheiterte Wait deshalb nie und der error-Zweig
	// in ws.go war toter Code (empirisch geprüft: 0 von 40 abgelehnt).
	maxWait = 100 * time.Millisecond

	// Verbindungsversuche je IP. Großzügiger als bei Nachrichten:
	// hinter einer Mobilfunk-IP können tausende Handys stecken.
	connRate  = 10
	connBurst = 30

	// Wie lange ein IP-Eintrag ohne Besuch stehen bleibt
	// und in welchem Takt ausgemistet wird.
	ipTTL        = 10 * time.Minute
	ipSweepEvery = 5 * time.Minute
)

// HINWEIS: Alle vier Zahlen sind geschätzt, nicht gemessen. Nach dem
// Umschalten (Phase 3) im Log nachsehen, wie oft "ratelimit" und
// "zu viele verbindungen" auftauchen – bleiben dort echte Nutzer
// hängen, sind die Zahlen zu streng. Am unsichersten ist connRate/
// connBurst wegen Carrier-Grade-NAT: tausende Kunden hinter einer IP.

func newMessageLimiter() *rate.Limiter {
	return rate.NewLimiter(rate.Limit(msgRate), msgBurst)
}

// Ein Eimer je IP, plus der Zeitpunkt des letzten Besuchs
// (den braucht das Ausmisten in Block 3).
type ipEntry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// ipLimiter ist das Regal. Gehört dem Prozess, nicht einer Verbindung.
type ipLimiter struct {
	mu   sync.Mutex
	byIP map[string]*ipEntry
}

func newIPLimiter() *ipLimiter {
	return &ipLimiter{byIP: make(map[string]*ipEntry)}
}

// allow meldet einen Verbindungsversuch und sagt, ob er erlaubt ist.
func (l *ipLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	e, ok := l.byIP[ip]
	if !ok {
		e = &ipEntry{lim: rate.NewLimiter(rate.Limit(connRate), connBurst)}
		l.byIP[ip] = e
	}
	e.lastSeen = time.Now()

	return e.lim.Allow()
}

// clientIP ermittelt die Adresse des Anrufers.
//
// ACHTUNG, Vertrauensannahme: X-Forwarded-For kann jeder frei erfinden,
// der uns DIREKT erreicht. Das ist nur deshalb vertretbar, weil auf
// Railway ausschließlich der Proxy zu uns durchkommt und dieser den
// Header selbst setzt. Würde der Dienst je ohne Proxy öffentlich
// stehen, wäre die Begrenzung damit wirkungslos.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if ip := strings.TrimSpace(first); ip != "" {
			return ip
		}
	}

	// Kein Proxy davor (lokaler Test): RemoteAddr ist "1.2.3.4:51234",
	// der Port gehört nicht zur Identität und muss weg.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// sweep wirft alle Einträge weg, die länger als ipTTL niemand angefasst hat.
func (l *ipLimiter) sweep() {
	l.mu.Lock()
	defer l.mu.Unlock()

	grenze := time.Now().Add(-ipTTL)
	for ip, e := range l.byIP {
		if e.lastSeen.Before(grenze) {
			delete(l.byIP, ip)
		}
	}
}

// sweepLoop mistet im Takt aus, bis der Prozess endet.
func (l *ipLimiter) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(ipSweepEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.sweep()
		}
	}
}
