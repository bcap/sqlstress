package control

import (
	"bytes"
	"fmt"
	"html/template"
	"net/http"
	"strconv"

	"github.com/bcap/sqlstress/config"
)

type renderData struct {
	Config *config.Config
}

func (s *Server) mainHandler(rw http.ResponseWriter, r *http.Request) {
	tmplt := template.New("main")
	parsed, err := tmplt.Parse(mustLoadAsset("assets/index.html"))
	if err != nil {
		writeErr(rw, err)
		return
	}

	var buf bytes.Buffer
	if err := parsed.Execute(&buf, renderData{Config: s.config}); err != nil {
		writeErr(rw, err)
		return
	}

	rw.Header().Set("Content-Type", "text/html")
	rw.WriteHeader(http.StatusOK)
	rw.Write(buf.Bytes())
}

func (s *Server) rateHandler(rw http.ResponseWriter, r *http.Request) {
	queryIdxStr := r.URL.Query().Get("query")
	deltaRateStr := r.URL.Query().Get("delta")
	if queryIdxStr == "" || deltaRateStr == "" {
		writeErr(rw, fmt.Errorf("query and delta rate are required"))
		return
	}

	queryIdx, err := strconv.Atoi(queryIdxStr)
	if err != nil {
		writeErr(rw, fmt.Errorf("invalid query index: %w", err))
		return
	}

	deltaRate, err := strconv.ParseFloat(deltaRateStr, 64)
	if err != nil {
		writeErr(rw, fmt.Errorf("invalid delta rate: %w", err))
		return
	}

	newRate := s.config.Queries[queryIdx].RatePerSecond + deltaRate
	if newRate < 0 {
		newRate = 0
	}
	s.config.Queries[queryIdx].RatePerSecond = newRate

	rw.WriteHeader(http.StatusOK)
}

func writeErr(rw http.ResponseWriter, err error) {
	rw.Header().Set("Content-Type", "text/plain")
	rw.WriteHeader(http.StatusInternalServerError)
	rw.Write([]byte(err.Error()))
}
