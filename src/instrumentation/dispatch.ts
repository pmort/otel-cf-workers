import { trace } from '@opentelemetry/api'
import { wrap } from '../wrap.js'
import { instrumentClientFetch } from './fetch.js'

export function instrumentDispatchNamespace(
	dispatcher: DispatchNamespace,
	name: string,
): DispatchNamespace {
	const tracer = trace.getTracer('Dispatch')
	const dispatchHandler: ProxyHandler<DispatchNamespace> = {
		get: (target, prop, receiver) => {
			if (prop === 'get') {
				const getFn = Reflect.get(target, prop, receiver)
				const fnHandler: ProxyHandler<any> = {
					apply: (fnTarget, thisArg, argArray) => {
						const workerName = argArray[0] as string
						return tracer.startActiveSpan(
							`Dispatch ${name} get`,
							{
								attributes: {
									binding_type: 'Dispatch Namespace',
									'cf.dispatch.namespace': name,
									'cf.dispatch.worker': workerName,
								},
							},
							(span) => {
								const fetcher = Reflect.apply(fnTarget, thisArg, argArray) as Fetcher
								span.end()
								return instrumentDispatchFetcher(fetcher, name, workerName)
							},
						)
					},
				}
				return wrap(getFn, fnHandler)
			}
			return Reflect.get(target, prop, receiver)
		},
	}
	return wrap(dispatcher, dispatchHandler)
}

function instrumentDispatchFetcher(fetcher: Fetcher, namespace: string, workerName: string): Fetcher {
	const fetcherHandler: ProxyHandler<Fetcher> = {
		get(target, prop) {
			if (prop === 'fetch') {
				const fetchFn = Reflect.get(target, prop)
				const attrs = {
					name: `Dispatch ${namespace}/${workerName}`,
					'cf.dispatch.namespace': namespace,
					'cf.dispatch.worker': workerName,
				}
				return instrumentClientFetch(fetchFn, () => ({ includeTraceContext: true }), attrs)
			} else {
				const value = Reflect.get(target, prop)
				if (typeof value === 'function') {
					return value.bind(target)
				}
				return value
			}
		},
	}
	return wrap(fetcher, fetcherHandler)
}
