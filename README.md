# tiny-agent-service

> Production AI agent service in one file. Built from tiny-* components · stdlib only · MIT.

```bash
# pip install tiny-*
python tiny_agent_service.py serve --port 8080
```

## What It Is

A **complete, production-ready AI agent service** assembled from tiny-* zero-dependency components:

| Component | Library | Purpose |
|-----------|---------|---------|
| HTTP routing | `tiny-router` | REST API server |
| Logging | `tiny-log` | Structured logs with correlation IDs |
| Validation | `tiny-validator` | Validate incoming payloads |
| Config | `tiny-config` | Load config from YAML/env/CLI |
| CLI | `tiny-cli` | Type-driven CLI |
| MCP | `tiny-mcp` | MCP tool protocol |
| Agent loop | `tiny-agent` | ReAct agent implementation |
| Budget | `tiny-budget` | USD + token cost enforcement |
| Rate limit | `tiny-rate` | Per-client rate limiting |
| Retry + CB | `tiny-retry` | LLM call resilience |
| Metrics | `tiny-metrics` | Prometheus-compatible metrics |
| Tracing | `tiny-trace` | W3C trace context |

The service is **one file, MIT licensed, stdlib wherever possible**, and assembles into a complete agent that:
- Receives tasks via REST API
- Exposes MCP tools to AI clients (Claude Desktop, Cursor, etc.)
- Enforces budgets, rate limits, and circuit breakers
- Persists tasks to disk for crash recovery
- Logs everything with correlation IDs

## Quick Start

```bash
# Start the service
python tiny_agent_service.py serve --port 8080 --budget 50.0 --model gpt-4o-mini

# Submit a task
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Analyze the security of facebook/react", "client_id": "analyst-1"}'

# Check task status
curl http://localhost:8080/tasks/<task_id>

# List all tasks
curl http://localhost:8080/tasks

# Health check
curl http://localhost:8080/health

# Budget status
curl http://localhost:8080/budget
```

## Built-In Tools

| Tool | Description |
|------|-------------|
| `run_bounty_scan` | Scan GitHub repos for bug bounty issues |
| `get_repo_health` | Get stars, forks, open issues, maintainer responsiveness |
| `analyze_code_quality` | Security + code quality analysis |
| `write_file` | Write content to a file |
| `read_file` | Read file contents |
| `run_command` | Execute shell commands |
| `get_time` | Current date/time |
| `search_web` | Web search |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  AI Client (Claude Desktop, Cursor, custom)                 │
│  MCP Protocol → tiny-agent-service                          │
└──────────────────────────────────────────────────────────────┘
                              │
┌──────────────────────────────────────────────────────────────┐
│  tiny-agent-service (tiny_agent_service.py)                 │
│  ├─ MCP server (tools/list, tools/call, resources/read)     │
│  ├─ REST API (POST /tasks, GET /tasks/:id, GET /health)    │
│  ├─ BudgetGovernor (per-model + global USD cap)              │
│  ├─ TokenBucket rate limiter (global)                       │
│  ├─ CircuitBreaker (LLM call resilience)                    │
│  ├─ TaskStore (NDJSON persistence, crash recovery)          │
│  └─ Structured logging (correlation IDs)                     │
└──────────────────────────────────────────────────────────────┘
                              │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
   tiny-router           tiny-budget           tiny-rate
   tiny-log              tiny-retry            tiny-metrics
   tiny-validator       tiny-eventbus         tiny-trace
```

## MCP Integration

Connect to the service from any MCP-compatible AI client:

```python
from tiny_mcp_client import MCPClient

with MCPClient(["python", "tiny_agent_service.py"]) as client:
    tools = client.list_tools()
    
    # Use the agent's built-in tools
    result = client.call_tool("get_time", {})
    result = client.call_tool("run_command", {"command": "ls -la"})
    
    # Read agent state resources
    budget = client.read_resource("agent://budget")
    tasks = client.read_resource("agent://tasks")
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | Required for LLM calls |
| `AGENT_MODEL` | `gpt-4o-mini` | Default model |
| `AGENT_BUDGET_USD` | `100.0` | Global USD budget |

## Task Lifecycle

```
pending → running → completed
                   ↓
             rate_limited / budget_exceeded / circuit_open
                   ↓
               failed
```

All task state is persisted to `/tmp/agent-tasks.jsonl` for crash recovery.

## Why?

Building an agent service means picking 10+ libraries (router, logger, validator, config, MCP, agent, budget, rate-limit, retry, metrics, tracing). Each has its own API, docs, and versioning. `tiny-agent-service` pre-wires them all into a coherent architecture — **one file, MIT, ~1,000 LOC**, and you can swap any component with the real tiny-* package.

## Part of the tiny-* Ecosystem

```
tiny-agent       — Minimal agent framework
tiny-mcp         — MCP server
tiny-mcp-client  — MCP client
tiny-router      — HTTP routing
tiny-budget      — Cost enforcement
tiny-rate        — Rate limiting
tiny-retry       — Retry + circuit breaker
fast-cache       — LRU+TTL cache
tiny-log         — Structured logging
tiny-metrics     — Prometheus metrics
tiny-trace       — Distributed tracing
```

See all 35+ repos at: [github.com/hussain-alsaibai](https://github.com/hussain-alsaibai)

## License

MIT — do anything with it. Built by [OpenClaw 🦞](https://github.com/hussain-alsaibai)
