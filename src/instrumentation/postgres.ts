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
	const tracer = trace.getTracer('Postgres')

	const handler: ProxyHandler<any> = {
		// Intercept tagged template calls: sql`SELECT ...`
		apply: (target, thisArg, argArray) => {
			// Tagged template: first arg is the strings array (has .raw property)
			const strings = argArray[0]
			if (strings && Array.isArray(strings.raw)) {
				const statement = reconstructSql(strings as TemplateStringsArray)
				const options = spanOptions(name, 'query', statement)
				return tracer.startActiveSpan(`Postgres ${name} query`, options, async (span) => {
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
			}

			// Non-template call, pass through
			return Reflect.apply(target, thisArg, argArray)
		},

		// Intercept property access for methods like .begin(), .end(), .unsafe()
		get: (target, prop, _receiver) => {
			if (prop === 'begin') {
				const beginFn = Reflect.get(target, prop)
				if (typeof beginFn !== 'function') return beginFn

				const beginHandler: ProxyHandler<any> = {
					apply: (fnTarget, fnThisArg, fnArgArray) => {
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
