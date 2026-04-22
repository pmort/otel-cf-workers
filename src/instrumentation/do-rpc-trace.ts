import { TraceFlags, type SpanContext } from '@opentelemetry/api'

// ── DO RPC trace-context propagation ──────────────────────────────
//
// DO RPC has no header channel (unlike `stub.fetch()`), so the only way to
// propagate trace context from the calling Worker into the DO isolate is
// through the argument list itself. We append a sentinel object as an
// extra trailing argument; the server-side proxy detects and strips it
// before invoking the user's method, then establishes the caller's span
// as the parent of the DO RPC server span.
//
// The sentinel uses a private-looking key and has a constant marker value,
// so it's distinguishable from any plausible user-supplied argument and
// non-breaking in both directions:
//   * New client → old server: extra arg is silently ignored (JS arity
//     leniency); server still creates an orphan root span (old behaviour).
//   * Old client → new server: no sentinel at tail → no propagation; server
//     falls through to the default unparented span (old behaviour).

export const RPC_TRACE_SENTINEL_KEY = '__otel_rpc_parent'
export const RPC_TRACE_SENTINEL_MARKER = 'otel-cf-workers.do-rpc.v1'

export interface RpcTraceSentinel {
	readonly [RPC_TRACE_SENTINEL_KEY]: string
	readonly __marker: typeof RPC_TRACE_SENTINEL_MARKER
}

export function isRpcTraceSentinel(value: unknown): value is RpcTraceSentinel {
	if (!value || typeof value !== 'object') return false
	const obj = value as Record<string, unknown>
	return obj['__marker'] === RPC_TRACE_SENTINEL_MARKER && typeof obj[RPC_TRACE_SENTINEL_KEY] === 'string'
}

export function makeRpcTraceSentinel(traceparent: string): RpcTraceSentinel {
	return {
		[RPC_TRACE_SENTINEL_KEY]: traceparent,
		__marker: RPC_TRACE_SENTINEL_MARKER,
	}
}

/** Serialise a SpanContext into a W3C traceparent header value. */
export function spanContextToTraceparent(ctx: SpanContext): string {
	const flags = (ctx.traceFlags & 0xff).toString(16).padStart(2, '0')
	return `00-${ctx.traceId}-${ctx.spanId}-${flags}`
}

/**
 * Parse a W3C traceparent header back into a SpanContext. Returns undefined
 * for anything that isn't a well-formed version-00 traceparent — callers
 * should fall back to the default behaviour rather than propagate garbage.
 */
export function traceparentToSpanContext(tp: string): SpanContext | undefined {
	const parts = tp.split('-')
	if (parts.length !== 4) return undefined
	const version = parts[0]
	const traceId = parts[1]
	const spanId = parts[2]
	const flagsHex = parts[3]
	if (version !== '00' || !traceId || !spanId || !flagsHex) return undefined
	if (!/^[0-9a-f]{32}$/.test(traceId) || traceId === '0'.repeat(32)) return undefined
	if (!/^[0-9a-f]{16}$/.test(spanId) || spanId === '0'.repeat(16)) return undefined
	if (!/^[0-9a-f]{2}$/.test(flagsHex)) return undefined
	return {
		traceId,
		spanId,
		traceFlags: parseInt(flagsHex, 16) as TraceFlags,
		isRemote: true,
	}
}
