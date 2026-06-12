package core

import "sync"

type Status string

const (
	StatusPending Status = "Pending"
	StatusRunning Status = "Running"
	StatusSuccess Status = "Success"
	StatusFailed  Status = "Failed"
)

type Step struct {
	Name     string `json:"name,omitempty"`
	Command  string `json:"command"`
	Status   Status `json:"status"`
	Output   string `json:"output,omitempty"`
	ExitCode int    `json:"exit_code"`
}

type Pipeline struct {
	Mu          sync.RWMutex      `json:"-"`
	ID          string            `json:"id"`
	Image       string            `json:"image,omitempty"`
	Environment map[string]string `json:"environment,omitempty"`
	Steps       []*Step           `json:"steps"`
	Status      Status            `json:"status"`
}
