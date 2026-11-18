# Changelog

All notable changes to tiny-agent-service are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.1] - 2026-11-18

### Fixed

- **No HTTP response carried a `Content-Length`.** `_json_resp()` wrote the
  JSON body and returned. The server speaks HTTP/1.0, so a direct client
  framed the body by EOF and nothing in the suite or the README looked wrong.
  A proxy or load balancer that keeps the upstream connection alive has no
  boundary: it waits for a length that never arrives, or reads the next
  response's status line as body bytes.
- **A malformed `Content-Length` killed the response.**
  `int(self.headers.get("Content-Length", 0))` was evaluated before the
  `try/except` around `json.loads`, so `Content-Length: abc` raised
  `ValueError` from inside `_read_body`, escaped `do_POST`, and the caller got
  nothing until its timeout. A bad header is now a framed `400`, and it is
  reported as a header problem rather than "invalid JSON" — the old fallback
  would have sent the caller debugging a payload that was fine.
- `_json_resp()` swallows `BrokenPipeError` / `ConnectionResetError`.

### Added

- `TestResponseFraming` — four wire-level tests that open a TCP socket and
  assert `Content-Length == len(body)` on 200, 404 and 400 responses, plus one
  asserting a header error is not mislabelled as a JSON error. Verified
  against the pre-fix module: all four fail.

### Notes

- No API change.
