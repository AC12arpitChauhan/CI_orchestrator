package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type PipelineStore struct {
	mu        sync.RWMutex
	pipelines map[string]*Pipeline
	dataDir   string
}

func NewPipelineStore(dataDir string) *PipelineStore {
	store := &PipelineStore{
		pipelines: make(map[string]*Pipeline),
		dataDir:   dataDir,
	}

	os.MkdirAll(dataDir, 0755)

	entries, err := os.ReadDir(dataDir)
	if err == nil {
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
				path := filepath.Join(dataDir, entry.Name())
				data, err := os.ReadFile(path)
				if err != nil {
					continue
				}

				var p Pipeline
				if err := json.Unmarshal(data, &p); err == nil {
					if p.Status == StatusRunning {
						p.Status = StatusFailed
						for _, step := range p.Steps {
							if step.Status == StatusRunning || step.Status == StatusPending {
								step.Status = StatusFailed
								step.Output += "\n[SYSTEM]: Pipeline aborted due to server crash/restart."
							}
						}
						store.Save(&p)
					}
					store.pipelines[p.ID] = &p
				}
			}
		}
	}

	return store
}

func (s *PipelineStore) Save(p *Pipeline) {
	s.mu.Lock()
	s.pipelines[p.ID] = p
	s.mu.Unlock()

	p.Mu.RLock()
	data, err := json.MarshalIndent(p, "", "  ")
	p.Mu.RUnlock()

	if err == nil {
		filePath := filepath.Join(s.dataDir, fmt.Sprintf("%s.json", p.ID))
		os.WriteFile(filePath, data, 0644)
	}
}

func (s *PipelineStore) Get(id string) (*Pipeline, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pipelines[id]
	return p, ok
}

func (s *PipelineStore) PopPending() *Pipeline {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range s.pipelines {
		if p.Status == StatusPending {
			return p
		}
	}
	return nil
}
