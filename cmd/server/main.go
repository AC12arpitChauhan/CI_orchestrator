package main
hemloo

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go_ci_orchestration/core"
)

var store *core.PipelineStore

type PipelineRequest struct {
	Image       string            `json:"image"`
	Environment map[string]string `json:"environment"`
	Steps       []string          `json:"steps"`
}

func main() {
	store = core.NewPipelineStore("data/pipelines")

	http.HandleFunc("/pipelines", handlePipelines)
	http.HandleFunc("/pipelines/stream/", handleSSEStream)
	http.HandleFunc("/pipelines/", handleGetPipeline)

	http.HandleFunc("/api/worker/poll", handleWorkerPoll)
	http.HandleFunc("/api/pipelines/", handleWorkerSync)

	fmt.Println("Starting Server (Phase 7&8) on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Server failed: %v\n", err)
	}
}

func handlePipelines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	if len(req.Steps) == 0 {
		http.Error(w, "Pipeline must contain at least one step", http.StatusBadRequest)
		return
	}

	env := req.Environment
	if env == nil {
		env = make(map[string]string)
	}

	img := req.Image
	if img == "" {
		img = "ubuntu:latest"
	}

	pipeline := &core.Pipeline{
		ID:          fmt.Sprintf("pipe-%d", time.Now().UnixNano()),
		Status:      core.StatusPending,
		Image:       img,
		Environment: env,
		Steps:       make([]*core.Step, len(req.Steps)),
	}

	for i, cmd := range req.Steps {
		pipeline.Steps[i] = &core.Step{
			Name:    fmt.Sprintf("Step %d", i+1),
			Command: cmd,
			Status:  core.StatusPending,
		}
	}

	store.Save(pipeline)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	pipeline.Mu.RLock()
	defer pipeline.Mu.RUnlock()
	json.NewEncoder(w).Encode(pipeline)
}

func handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/pipelines/")
	if id == "" {
		http.Error(w, "Missing pipeline ID", http.StatusBadRequest)
		return
	}

	pipeline, exists := store.Get(id)
	if !exists {
		http.Error(w, "Pipeline not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	pipeline.Mu.RLock()
	defer pipeline.Mu.RUnlock()
	json.NewEncoder(w).Encode(pipeline)
}

func handleWorkerPoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	p := store.PopPending()
	if p == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	p.Mu.Lock()
	p.Status = core.StatusRunning
	p.Mu.Unlock()
	store.Save(p)

	w.Header().Set("Content-Type", "application/json")
	p.Mu.RLock()
	defer p.Mu.RUnlock()
	json.NewEncoder(w).Encode(p)
}

func handleWorkerSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/pipelines/")
	var incoming core.Pipeline
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	incoming.ID = id
	store.Save(&incoming)
	broadcaster.Broadcast(&incoming)

	w.WriteHeader(http.StatusOK)
}

func handleSSEStream(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/pipelines/stream/")
	if id == "" {
		http.Error(w, "Missing pipeline ID", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := broadcaster.Subscribe(id)
	defer broadcaster.Unsubscribe(id, ch)

	if p, exists := store.Get(id); exists {
		p.Mu.RLock()
		data, _ := json.Marshal(p)
		p.Mu.RUnlock()
		fmt.Fprintf(w, "data: %s\n\n", string(data))
		flusher.Flush()
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-ch:
			p.Mu.RLock()
			data, _ := json.Marshal(p)
			p.Mu.RUnlock()
			fmt.Fprintf(w, "data: %s\n\n", string(data))
			flusher.Flush()
		}
	}
}
