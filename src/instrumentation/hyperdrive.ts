import { trace } from '@opentelemetry/api'
import { wrap, passthroughGet } from '../wrap.js'

export function instrumentHyperdrive(hyperdrive: Hyperdrive, name: string): Hyperdrive {
	const tracer = trace.getTracer('Hyperdrive')
	const hyperdriveHandler: ProxyHandler<Hyperdrive> = {
		get: (target, prop, receiver) => {
			const value = Reflect.get(target, prop, receiver)
			if (prop === 'connect') {
				const fnHandler: ProxyHandler<any> = {
					apply: (fnTarget, thisArg, argArray) => {
						return tracer.startActiveSpan(`Hyperdrive ${name} connect`, { attributes: {
							binding_type: 'Hyperdrive',
							'db.name': name,
							'db.system': 'Cloudflare Hyperdrive',
							'db.operation': 'connect',
							'db.cf.hyperdrive.host': target.host,
							'db.cf.hyperdrive.port': target.port,
							'db.cf.hyperdrive.user': target.user,
							'db.cf.hyperdrive.database': target.database,
						} }, async (span) => {
							try {
								const result = await Reflect.apply(fnTarget, thisArg, argArray)
								span.end()
								return result
							} catch (error) {
								span.recordException(error as Error)
								span.end()
								throw error
							}
						})
					},
				}
				return wrap(value, fnHandler)
			}
			if (typeof value === 'function') {
				return passthroughGet(target, prop)
			}
			// Instrument property access for connectionString to add a span
			if (prop === 'connectionString' || prop === 'host' || prop === 'port' || prop === 'user' || prop === 'password' || prop === 'database') {
				return value
			}
			return value
		},
	}
	return wrap(hyperdrive, hyperdriveHandler)
}
