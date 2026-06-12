package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go_ci_orchestration/core"
)

const ServerURL = "http://localhost:8080"

func main() {
	fmt.Println("Starting CI/CD Worker (Phase 7&8)...")
	for {
		resp, err := http.Post(ServerURL+"/api/worker/poll", "application/json", nil)
		if err != nil {
			fmt.Printf("Worker: Failed to connect to server: %v\n", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Worker: Unexpected status code from server: %d\n", resp.StatusCode)
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		var p core.Pipeline
		if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
			fmt.Printf("Worker: Failed to decode pipeline: %v\n", err)
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		resp.Body.Close()

		fmt.Printf("Worker: Picked up pipeline %s\n", p.ID)

		ExecutePipeline(&p)

		fmt.Printf("Worker: Finished pipeline %s\n", p.ID)
	}
}
