---
"@pmort/otel-cf-workers": patch
---

Propagate trace context through DO RPC calls. Previously every DO RPC invocation created an orphan root span because RPC has no header channel to carry traceparent. The client-side proxy now appends a sentinel object to the argument list carrying the caller span's W3C traceparent; the server-side proxy detects and strips it, and parents the DO RPC server span to the caller. Backwards compatible in both directions (JS arity leniency + graceful fallback when no sentinel is present).
