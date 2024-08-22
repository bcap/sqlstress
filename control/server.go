package control

import (
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/bcap/sqlstress/config"
	"github.com/bcap/sqlstress/log"

	"github.com/gorilla/mux"
)

type Server struct {
	config *config.Config
	mux    *mux.Router
}

func New(config *config.Config) *Server {
	srv := Server{
		config: config,
		mux:    mux.NewRouter(),
	}
	srv.mux.HandleFunc("/", srv.mainHandler)
	srv.mux.PathPrefix("/assets/").Handler(http.FileServer(http.FS(assets)))
	srv.mux.HandleFunc("/rate", srv.rateHandler)
	return &srv
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.ControlServerAddr)
	if err != nil {
		return err
	}

	log.Infof("Control server listening on %s", s.config.ControlServerAddr)

	closeOnce := sync.Once{}
	close := func() {
		closeOnce.Do(func() {
			listener.Close()
		})
	}
	defer close()
	go func() {
		<-ctx.Done()
		close()
	}()

	srv := http.Server{
		Addr:    s.config.ControlServerAddr,
		Handler: s.mux,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return ctx
		},
	}

	return srv.Serve(listener)
}
