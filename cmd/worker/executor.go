package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go_ci_orchestration/core"
)

func syncPipeline(p *core.Pipeline) {
	p.Mu.RLock()
	data, err := json.Marshal(p)
	p.Mu.RUnlock()

	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/api/pipelines/%s", ServerURL, p.ID), bytes.NewBuffer(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func ExecutePipeline(p *core.Pipeline) {
	done := make(chan bool)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				syncPipeline(p)
			case <-done:
				syncPipeline(p)
				return
			}
		}
	}()

	defer func() {
		done <- true
	}()

	workspace := filepath.Join("data", "workspaces", fmt.Sprintf("pipeline-%s", p.ID))
	os.MkdirAll(workspace, 0755)
	defer os.RemoveAll(workspace)

	absWorkspace, _ := filepath.Abs(workspace)

	envFile := filepath.Join(absWorkspace, ".ci_env")
	os.WriteFile(envFile, []byte(""), 0644)

	p.Mu.Lock()
	p.Status = core.StatusRunning
	p.Mu.Unlock()

	for _, step := range p.Steps {
		p.Mu.Lock()
		step.Status = core.StatusRunning
		p.Mu.Unlock()

		args := []string{"run", "--rm",
			"-v", fmt.Sprintf("%s:%s", absWorkspace, "/workspace"),
			"-w", "/workspace",
		}

		p.Mu.RLock()
		for k, v := range p.Environment {
			args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
		}
		p.Mu.RUnlock()

		args = append(args, "-e", "CI_WORKSPACE=/workspace")
		args = append(args, "-e", "CI_ENV_FILE=/workspace/.ci_env")

		args = append(args, p.Image, "sh", "-c", step.Command)

		cmd := exec.Command("docker", args...)

		logger := &stepLogger{pipeline: p, step: step}
		cmd.Stdout = logger
		cmd.Stderr = logger

		err := cmd.Run()
		if err != nil {
			p.Mu.Lock()
			step.Status = core.StatusFailed
			step.ExitCode = 1
			step.Output += fmt.Sprintf("\nDocker Execution Error: %v\n", err)
			p.Status = core.StatusFailed
			p.Mu.Unlock()
			return
		}

		envBytes, err := os.ReadFile(envFile)
		if err == nil {
			lines := strings.Split(string(envBytes), "\n")
			p.Mu.Lock()
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				parts := strings.SplitN(line, "=", 2)
				if len(parts) == 2 {
					p.Environment[parts[0]] = parts[1]
				}
			}
			p.Mu.Unlock()
			os.WriteFile(envFile, []byte(""), 0644)
		}

		p.Mu.Lock()
		step.Status = core.StatusSuccess
		p.Mu.Unlock()
	}

	p.Mu.Lock()
	p.Status = core.StatusSuccess
	p.Mu.Unlock()
}

type stepLogger struct {
	pipeline *core.Pipeline
	step     *core.Step
}

func (l *stepLogger) Write(p []byte) (n int, err error) {
	l.pipeline.Mu.Lock()
	defer l.pipeline.Mu.Unlock()
	l.step.Output += string(p)
	return len(p), nil
}
