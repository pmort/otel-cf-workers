import { Exception, SpanKind, SpanOptions, SpanStatusCode, trace } from '@opentelemetry/api'
import { SemanticAttributes } from '@opentelemetry/semantic-conventions'
import { wrap } from '../wrap.js'

const dbSystem = 'PostgreSQL'

function reconstructSql(strings: TemplateStringsArray): string {
	let sql = strings[0] ?? ''
	for (let i = 1; i < strings.length; i++) {
		sql += `$${i}${strings[i]}`
	}
	return sql
}

function spanOptions(name: string, operation: string, sql?: string): SpanOptions {
	const attributes: Record<string, string> = {
		binding_type: 'Postgres',
		[SemanticAttributes.DB_NAME]: name,
		[SemanticAttributes.DB_SYSTEM]: dbSystem,
		[SemanticAttributes.DB_OPERATION]: operation,
	}
	if (sql) {
		attributes[SemanticAttributes.DB_STATEMENT] = sql
	}
	return {
		kind: SpanKind.CLIENT,
		attributes,
	}
}

/**
 * Wrap a postgres Query's `.then()` so a span is created only when the
 * query is actually awaited/executed — never for fragments.
 *
 * The `postgres` library's tagged template returns a Query object that is
 * thenable. When used as a fragment inside another template
 * (`sql\`... ${frag} ...\``), the parent extracts its SQL via internal
 * methods (describe/first/etc) — `.then` is NOT called. Only
 * `await query` / `query.then(...)` triggers execution, so hooking `.then`
 * gives us exactly-one span per real query, with no fragment pollution.
 */
function instrumentQueryThenable(query: any, name: string, statement: string): any {
	if (!query || typeof query.then !== 'function') return query
	const tracer = trace.getTracer('Postgres')

	const originalThen = query.then.bind(query)
	let instrumented = false

	query.then = (onFulfilled: any, onRejected: any) => {
		// Guard against double-wrapping if .then is called multiple times
		if (instrumented) {
			return originalThen(onFulfilled, onRejected)
		}
		instrumented = true

		const options = spanOptions(name, 'query', statement)
		return tracer.startActiveSpan(`Postgres ${name} query`, options, (span) => {
			return originalThen(
				(value: unknown) => {
					span.setStatus({ code: SpanStatusCode.OK })
					span.end()
					return onFulfilled ? onFulfilled(value) : value
				},
				(error: unknown) => {
					span.recordException(error as Exception)
					span.setStatus({
						code: SpanStatusCode.ERROR,
						message: (error as Error)?.message,
					})
					span.end()
					if (onRejected) return onRejected(error)
					throw error
				},
			)
		})
	}

	return query
}

function instrumentPostgresFn(fn: Function, name: string, operation: string) {
	const tracer = trace.getTracer('Postgres')
	const fnHandler: ProxyHandler<any> = {
		apply: (target, thisArg, argArray) => {
			const options = spanOptions(name, operation)
			return tracer.startActiveSpan(`Postgres ${name} ${operation}`, options, async (span) => {
				try {
					const result = await Reflect.apply(target, thisArg, argArray)
					span.setStatus({ code: SpanStatusCode.OK })
					return result
				} catch (error) {
					span.recordException(error as Exception)
					span.setStatus({ code: SpanStatusCode.ERROR })
					throw error
				} finally {
					span.end()
				}
			})
		},
	}
	return wrap(fn, fnHandler)
}

export function instrumentPostgresClient(sql: any, name: string): any {
	const handler: ProxyHandler<any> = {
		// Intercept tagged template calls: sql`SELECT ...`
		//
		// CRITICAL: We must NOT `await` here. The postgres library uses the
		// same `sql\`\`` call for both executable queries AND fragments
		// interpolated into other templates. Awaiting would force fragments
		// to execute as standalone queries (with unfilled $1/$2 placeholders),
		// causing real production failures.
		//
		// Instead, we return the Query object unchanged and hook `.then()` so
		// a span is only created when the query is actually awaited.
		apply: (target, thisArg, argArray) => {
			const result = Reflect.apply(target, thisArg, argArray)

			// Tagged template: first arg is the strings array (has .raw property)
			const strings = argArray[0]
			if (strings && Array.isArray(strings.raw)) {
				const statement = reconstructSql(strings as TemplateStringsArray)
				return instrumentQueryThenable(result, name, statement)
			}

			// Non-template call (e.g. sql(...) with a column name), pass through
			return result
		},

		// Intercept property access for methods like .begin(), .end(), .unsafe()
		get: (target, prop, _receiver) => {
			if (prop === 'begin') {
				const beginFn = Reflect.get(target, prop)
				if (typeof beginFn !== 'function') return beginFn

				const beginHandler: ProxyHandler<any> = {
					apply: (fnTarget, fnThisArg, fnArgArray) => {
						const tracer = trace.getTracer('Postgres')
						const options = spanOptions(name, 'begin')
						return tracer.startActiveSpan(`Postgres ${name} begin`, options, async (span) => {
							try {
								// The callback receives a scoped sql instance — instrument it too
								const originalCallback = fnArgArray[0]
								if (typeof originalCallback === 'function') {
									fnArgArray[0] = (scopedSql: any) => {
										const instrumentedScoped = instrumentPostgresClient(scopedSql, name)
										return originalCallback(instrumentedScoped)
									}
								}
								const result = await Reflect.apply(fnTarget, fnThisArg, fnArgArray)
								span.setStatus({ code: SpanStatusCode.OK })
								return result
							} catch (error) {
								span.recordException(error as Exception)
								span.setStatus({ code: SpanStatusCode.ERROR })
								throw error
							} finally {
								span.end()
							}
						})
					},
				}
				return wrap(beginFn, beginHandler)
			}

			if (prop === 'end' || prop === 'unsafe') {
				const fn = Reflect.get(target, prop)
				if (typeof fn === 'function') {
					return instrumentPostgresFn(fn, name, String(prop))
				}
				return fn
			}

			// Pass through everything else
			const value = Reflect.get(target, prop)
			if (typeof value === 'function') {
				return value.bind(target)
			}
			return value
		},
	}

	return wrap(sql, handler, false)
}
