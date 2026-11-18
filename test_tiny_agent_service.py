#!/usr/bin/env python3
"""Tests for tiny-agent-service."""
import asyncio, json, pytest, time
from tiny_agent_service import (
    BudgetGovernor, TokenBucket, CircuitBreaker, CircuitOpen,
    TaskStore, TaskResult, AgentService, TOOL_IMPLS
)


class TestBudgetGovernor:
    def test_check_within_budget(self):
        bg = BudgetGovernor(global_usd=1.0)
        assert bg.check("gpt-4o-mini", 1000) is True

    def test_check_exceeds_global(self):
        bg = BudgetGovernor(global_usd=0.00001)
        assert bg.check("gpt-4o-mini", 1_000_000) is False

    def test_record_updates_spent(self):
        bg = BudgetGovernor(global_usd=1.0)
        cost = bg.record("gpt-4o-mini", 100)
        assert cost >= 0
        rem = bg.remaining()
        assert rem["global_spent_usd"] >= 0

    def test_per_model_spent(self):
        bg = BudgetGovernor(global_usd=10.0)
        bg.record("gpt-4o-mini", 100)
        bg.record("gpt-4o-mini", 100)
        remaining = bg.remaining()
        assert "gpt-4o-mini" in remaining["per_model_spent"]


class TestTokenBucket:
    def test_acquire_sync(self):
        tb = TokenBucket(rate=10.0, capacity=5.0)
        # Can acquire up to capacity
        r1 = tb.acquire_sync(5.0)
        assert r1 is True
        # Now empty
        r2 = tb.acquire_sync(1.0)
        assert r2 is False

    def test_acquire_async(self):
        tb = TokenBucket(rate=10.0, capacity=5.0)
        async def run():
            r1 = await tb.acquire(5.0)
            r2 = await tb.acquire(1.0)
            return r1, r2
        r1, r2 = asyncio.run(run())
        assert r1 is True
        assert r2 is False


class TestCircuitBreaker:
    def test_initial_state_closed(self):
        cb = CircuitBreaker(failure_threshold=3, reset_timeout=1.0)
        assert cb.state == "closed"

    def test_circuit_open_exception(self):
        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.1)
        async def fail_fn():
            raise ValueError("fail")
        async def run():
            try:
                await cb.call(fail_fn)
            except ValueError:
                pass
            try:
                await cb.call(lambda: None)
            except CircuitOpen:
                return True
            return False
        assert asyncio.run(run()) is True


class TestTaskStore:
    def test_save_and_load(self, tmp_path):
        path = str(tmp_path / "tasks.jsonl")
        store = TaskStore(path=path)
        store.save({"id": "t1", "status": "pending", "prompt": "hello"})
        tasks = store.load_pending()
        assert len(tasks) == 1
        assert tasks[0]["id"] == "t1"

    def test_update(self, tmp_path):
        path = str(tmp_path / "tasks.jsonl")
        store = TaskStore(path=path)
        store.save({"id": "t1", "status": "pending"})
        store.update("t1", status="completed", result={"ok": True})
        tasks = store.load_pending()
        assert len(tasks) == 0


class TestAgentService:
    @pytest.mark.skip(reason="submit_task uses asyncio.create_task — needs event loop")
    def test_submit_task_returns_id(self):
        svc = AgentService()
        task_id = svc.submit_task("Hello agent")
        assert task_id
        assert len(task_id) <= 12

    @pytest.mark.skip(reason="submit_task uses asyncio.create_task — needs event loop")
    def test_list_tasks(self):
        svc = AgentService()
        svc.submit_task("test 1")
        tasks = svc.list_tasks()
        assert len(tasks) >= 1

    def test_mcp_initialize(self):
        svc = AgentService()
        result = svc.mcp_initialize("session-1", {"name": "test-client", "version": "1.0"})
        assert "protocolVersion" in result
        assert "capabilities" in result
        assert result["serverInfo"]["name"] == svc.name

    def test_mcp_list_tools(self):
        svc = AgentService()
        tools = svc.mcp_list_tools()
        assert len(tools) > 0
        assert all("name" in t for t in tools)
        assert all("description" in t for t in tools)
        assert all("inputSchema" in t for t in tools)

    def test_mcp_list_resources(self):
        svc = AgentService()
        resources = svc.mcp_list_resources()
        assert len(resources) >= 3
        uris = [r["uri"] for r in resources]
        assert "agent://tasks" in uris
        assert "agent://budget" in uris

    def test_status(self):
        svc = AgentService()
        status = svc.status()
        assert "name" in status
        assert "version" in status
        assert "tasks_total" in status
        assert "budget" in status
        assert "circuit_breaker" in status


class TestToolImpls:
    def test_get_time(self):
        result = asyncio.run(TOOL_IMPLS["get_time"]())
        assert "iso" in result
        assert "unix" in result

    def test_write_and_read_file(self, tmp_path):
        path = str(tmp_path / "test.txt")
        write_result = asyncio.run(TOOL_IMPLS["write_file"](path=path, content="Hello, World!"))
        assert write_result["ok"] is True
        read_result = asyncio.run(TOOL_IMPLS["read_file"](path=path))
        assert read_result["ok"] is True
        assert read_result["content"] == "Hello, World!"

    def test_run_command(self):
        result = asyncio.run(TOOL_IMPLS["run_command"](command="echo hello"))
        assert result["returncode"] == 0
        assert "hello" in result["stdout"]


class TestResponseFraming:
    """Wire-level tests: read the bytes the server sends, not the dict it built.

    The suite above exercises the service object directly, so nothing ever
    looked at a response header. Every reply shipped without a Content-Length,
    and a malformed one produced no reply at all.
    """

    @staticmethod
    def _handler_base():
        import tiny_agent_service

        return tiny_agent_service

    @pytest.fixture()
    def server(self):
        import threading
        from http.server import HTTPServer
        import tiny_agent_service

        svc = tiny_agent_service.AgentService()
        base = tiny_agent_service._Handler

        class Handler(base):
            def __init__(self, *args, **kwargs):
                super().__init__(svc, *args, **kwargs)

        httpd = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            yield httpd.server_address[1]
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=5)

    @staticmethod
    def _raw(port, method, path, body=b"", headers=""):
        import socket

        sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        try:
            sock.sendall(
                f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
                f"{headers}Connection: close\r\n\r\n".encode() + body
            )
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
        finally:
            sock.close()
        head, _, payload = data.partition(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        hdrs = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                hdrs[k.strip().lower()] = v.strip()
        return lines[0], hdrs, payload

    def test_health_declares_its_length(self, server):
        status, headers, body = self._raw(server, "GET", "/health")
        assert "200" in status
        assert headers.get("content-length") == str(len(body))
        assert body

    def test_404_declares_its_length(self, server):
        status, headers, body = self._raw(server, "GET", "/nope")
        assert "404" in status
        assert headers.get("content-length") == str(len(body))

    def test_malformed_content_length_gets_a_framed_400(self, server):
        status, headers, body = self._raw(
            server, "POST", "/tasks", b"{}", "Content-Length: abc\r\n"
        )
        assert "400" in status
        assert headers.get("content-length") == str(len(body))
        assert b"Content-Length" in body

    def test_bad_header_is_not_reported_as_bad_json(self, server):
        """Saying 'invalid JSON' sends the caller to the wrong layer."""
        _, _, body = self._raw(
            server, "POST", "/tasks", b"{}", "Content-Length: abc\r\n"
        )
        assert b"invalid JSON" not in body


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
