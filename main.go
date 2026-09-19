package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// fatal ersetzt log.Fatalf: slog kennt kein Fatal. Über log.Fatalf käme die
// wichtigste Meldung überhaupt nach SetDefault nur als INFO an.
// Wie log.Fatalf überspringt os.Exit alle defer.
func fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

func main() {
	// JSON-Zeilen auf stdout: Railway liest "level" und "msg" selbst. Gos log
	// schreibt nach stderr, und Railway zeigt stderr pauschal als error an.
	// Leitet nebenbei alles, was noch über das log-Paket kommt (z. B.
	// net/http), als INFO hierher um.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := LoadConfig()
	if err != nil {
		fatal("Start abgebrochen", err)
	}

	slog.Info("ws-gateway startet",
		"instance", cfg.InstanceID,
		"port", cfg.Port,
		"backend", cfg.BackendURL,
		"redis", mask(cfg.RedisURL),
		"secret", mask(string(cfg.SecretKey)),
		"ws_secret", mask(cfg.WSInternalSecret),
	)

	rdb, err := newRedisClient(cfg.RedisURL)
	if err != nil {
		fatal("Start abgebrochen", err)
	}
	defer rdb.Close()
	slog.Info("Redis verbunden")

	// Lebt so lange wie der ganze Prozess. Endet er, endet das Redis-Abo.
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	mux := http.NewServeMux()
	gw := &Gateway{cfg: cfg, rdb: rdb, hc: newBackendClient(), reg: newRegistry(), ips: newIPLimiter()}

	// Genau EIN Abo pro Prozess – nicht eins pro Verbindung.
	go gw.subscribePush(rootCtx)

	// Ebenfalls genau EINE pro Prozess: mistet alte IP-Einträge aus.
	go gw.ips.sweepLoop(rootCtx)

	mux.HandleFunc("/ws/chat", gw.handleWS)

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	slog.Info("höre auf", "addr", srv.Addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal("Server gestoppt", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("Signal empfangen, fahre herunter")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Herunterfahren unsauber", "err", err)
	}
	// Keine neuen Pushes mehr annehmen.
	rootCancel()

	// srv.Shutdown lässt WebSockets links liegen – die gelten als übernommen.
	// Also selbst einsammeln, sonst stünden alle bis zu 60 s falsch online.
	ids, n := gw.reg.closeAll()
	gw.clearPresenceAll(ids)
	slog.Info("verbindungen geschlossen, nutzer abgemeldet", "verbindungen", n, "nutzer", len(ids))

	slog.Info("beendet")

}
