package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Start abgebrochen: %v", err)
	}

	log.Printf(
		"ws-gateway startet: instance=%s port=%s backend=%s redis=%s secret=%s ws_secret=%s",
		cfg.InstanceID,
		cfg.Port,
		cfg.BackendURL,
		mask(cfg.RedisURL),
		mask(string(cfg.SecretKey)),
		mask(cfg.WSInternalSecret),
	)

	rdb, err := newRedisClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("Start abgebrochen: %v", err)
	}
	defer rdb.Close()
	log.Println("Redis verbunden")

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

	log.Printf("höre auf %s", srv.Addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server gestoppt: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Signal empfangen, fahre herunter ...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Herunterfahren unsauber: %v", err)
	}
	// Keine neuen Pushes mehr annehmen.
	rootCancel()

	// srv.Shutdown lässt WebSockets links liegen – die gelten als übernommen.
	// Also selbst einsammeln, sonst stünden alle bis zu 60 s falsch online.
	ids, n := gw.reg.closeAll()
	gw.clearPresenceAll(ids)
	log.Printf("%d verbindungen geschlossen, %d nutzer abgemeldet", n, len(ids))

	log.Println("beendet")

}
