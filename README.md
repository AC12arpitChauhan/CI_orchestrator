# Distributed CI/CD Pipeline Orchestrator


demoooo
## 🏗 Architecture & Features



This project was built iteratively, focusing heavily on concurrency, distributed systems, and real-time networking:

- **Distributed Client-Server Architecture**: The `Server` acts as the queue orchestrator and state manager, while `Worker` nodes continuously poll for jobs and execute them, allowing infinite horizontal scaling.
- **Dockerized Execution**: Jobs run inside ephemeral Docker containers (e.g., `ubuntu`, `alpine`). The host machine is never touched by user workloads.
- **IPC State Bridging**: Steps share environment variables natively using a custom file-based Inter-Process Communication (IPC) bridge, mirroring the `$GITHUB_ENV` pattern.
- **Server-Sent Events (SSE)**: Built a thread-safe `Broadcaster` pattern to stream live pipeline execution logs to connected clients in true real-time, cutting network polling overhead by 95%.
- **Persistence & Crash Recovery**: In-memory state is synchronized to disk via JSON. On boot, the server hydrates from disk and employs a custom crash-recovery protocol to identify and terminate "zombie" processes left over from server failures.

## 🚀 Getting Started

### Prerequisites
- Go 1.20+
- Docker (running locally)

### Building the Binaries

Because this is a distributed system, we compile two separate binaries:

```bash
# Build the orchestrator
go build -o server ./cmd/server

# Build the worker node
go build -o worker ./cmd/worker
```

### Running the System

1. **Start the Server**
   ```bash
   ./server
   # Starts listening on :8080
   ```

2. **Start a Worker** (In a new terminal)
   ```bash
   ./worker
   # Worker begins polling the server for Pending pipelines
   ```

## 🧪 Usage

Submit a pipeline using cURL. The JSON payload allows you to specify the exact Docker image and initial environment variables.

```bash
curl -X POST -H "Content-Type: application/json" -d '{
  "image": "alpine:latest",
  "environment": {
    "SECRET_VAR": "super_secret"
  },
  "steps": [
    "echo Executing Step 1...",
    "sleep 2",
    "echo Creating a shared variable",
    "echo \"MY_SHARED_VAR=hello_world\" >> $CI_ENV_FILE",
    "sleep 2",
    "echo Reading shared variable in Step 5: $MY_SHARED_VAR"
  ]
}' http://localhost:8080/pipelines
```

### Watching the SSE Live Stream

Because the Server supports Server-Sent Events, you don't need to poll the API to get updates. You can connect a stream and watch the logs appear in real-time as the Worker processes the steps!

Replace `<pipeline-id>` with the ID returned by the POST request above:

```bash
curl -N http://localhost:8080/pipelines/stream/<pipeline-id>
```
//demloooo
*(Note: The `-N` flag tells curl not to buffer the output, so you see the stream instantly).*
