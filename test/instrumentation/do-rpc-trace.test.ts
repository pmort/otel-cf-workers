import { describe, expect, test } from 'vitest'
import { TraceFlags } from '@opentelemetry/api'
import {
	isRpcTraceSentinel,
	makeRpcTraceSentinel,
	spanContextToTraceparent,
	traceparentToSpanContext,
	RPC_TRACE_SENTINEL_KEY,
} from '../../src/instrumentation/do-rpc-trace'

const VALID_TRACE_ID = 'abcdef0123456789abcdef0123456789'
const VALID_SPAN_ID = '1234567890abcdef'

describe('spanContextToTraceparent', () => {
	test('emits version-00 W3C format with sampled flag', () => {
		const tp = spanContextToTraceparent({
			traceId: VALID_TRACE_ID,
			spanId: VALID_SPAN_ID,
			traceFlags: TraceFlags.SAMPLED,
		})
		expect(tp).toBe(`00-${VALID_TRACE_ID}-${VALID_SPAN_ID}-01`)
	})

	test('emits unsampled flag as 00', () => {
		const tp = spanContextToTraceparent({
			traceId: VALID_TRACE_ID,
			spanId: VALID_SPAN_ID,
			traceFlags: TraceFlags.NONE,
		})
		expect(tp).toBe(`00-${VALID_TRACE_ID}-${VALID_SPAN_ID}-00`)
	})
})

describe('traceparentToSpanContext', () => {
	test('round-trips a sampled span context', () => {
		const original = {
			traceId: VALID_TRACE_ID,
			spanId: VALID_SPAN_ID,
			traceFlags: TraceFlags.SAMPLED,
		}
		const tp = spanContextToTraceparent(original)
		const parsed = traceparentToSpanContext(tp)
		expect(parsed).toEqual({ ...original, isRemote: true })
	})

	test('rejects wrong version', () => {
		expect(traceparentToSpanContext(`99-${VALID_TRACE_ID}-${VALID_SPAN_ID}-01`)).toBeUndefined()
	})

	test('rejects all-zero traceId', () => {
		expect(traceparentToSpanContext(`00-${'0'.repeat(32)}-${VALID_SPAN_ID}-01`)).toBeUndefined()
	})

	test('rejects all-zero spanId', () => {
		expect(traceparentToSpanContext(`00-${VALID_TRACE_ID}-${'0'.repeat(16)}-01`)).toBeUndefined()
	})

	test('rejects malformed hex', () => {
		expect(traceparentToSpanContext(`00-zzz-${VALID_SPAN_ID}-01`)).toBeUndefined()
		expect(traceparentToSpanContext(`00-${VALID_TRACE_ID}-zzz-01`)).toBeUndefined()
		expect(traceparentToSpanContext(`00-${VALID_TRACE_ID}-${VALID_SPAN_ID}-zz`)).toBeUndefined()
	})

	test('rejects wrong segment count', () => {
		expect(traceparentToSpanContext('00-abc-def')).toBeUndefined()
		expect(traceparentToSpanContext(`00-${VALID_TRACE_ID}-${VALID_SPAN_ID}-01-extra`)).toBeUndefined()
	})
})

describe('isRpcTraceSentinel', () => {
	test('matches a sentinel created by makeRpcTraceSentinel', () => {
		const sentinel = makeRpcTraceSentinel(`00-${VALID_TRACE_ID}-${VALID_SPAN_ID}-01`)
		expect(isRpcTraceSentinel(sentinel)).toBe(true)
	})

	test('rejects plain objects that happen to have the key', () => {
		expect(isRpcTraceSentinel({ [RPC_TRACE_SENTINEL_KEY]: 'whatever' })).toBe(false)
	})

	test('rejects null, undefined, primitives', () => {
		expect(isRpcTraceSentinel(null)).toBe(false)
		expect(isRpcTraceSentinel(undefined)).toBe(false)
		expect(isRpcTraceSentinel('string')).toBe(false)
		expect(isRpcTraceSentinel(42)).toBe(false)
		expect(isRpcTraceSentinel([])).toBe(false)
	})

	test('rejects an object with marker but non-string traceparent', () => {
		expect(
			isRpcTraceSentinel({
				[RPC_TRACE_SENTINEL_KEY]: 123,
				__marker: 'otel-cf-workers.do-rpc.v1',
			}),
		).toBe(false)
	})
})
