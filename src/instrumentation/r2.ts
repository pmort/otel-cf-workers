import { Attributes, SpanKind, SpanOptions, SpanStatusCode, Exception, trace } from '@opentelemetry/api'
import { wrap } from '../wrap.js'

const dbSystem = 'Cloudflare R2'

type ExtraAttributeFn = (argArray: any[], result: any) => Attributes

const R2Attributes: Record<string | symbol, ExtraAttributeFn> = {
	head(argArray) {
		return { 'db.cf.r2.key': argArray[0] }
	},
	get(argArray) {
		return { 'db.cf.r2.key': argArray[0] }
	},
	put(argArray) {
		const attrs: Attributes = { 'db.cf.r2.key': argArray[0] }
		if (argArray[2]) {
			const opts = argArray[2] as R2PutOptions
			if (opts.httpMetadata) attrs['db.cf.r2.http_metadata'] = true
			if (opts.customMetadata) attrs['db.cf.r2.custom_metadata'] = true
		}
		return attrs
	},
	delete(argArray) {
		const key = argArray[0]
		if (Array.isArray(key)) {
			return { 'db.cf.r2.key_count': key.length }
		}
		return { 'db.cf.r2.key': key }
	},
	list(_argArray, result) {
		const attrs: Attributes = {}
		const listResult = result as R2Objects
		attrs['db.cf.r2.truncated'] = listResult.truncated
		attrs['db.cf.r2.object_count'] = listResult.objects.length
		if (listResult.truncated) {
			attrs['db.cf.r2.cursor'] = listResult.cursor
		}
		return attrs
	},
	createMultipartUpload(argArray) {
		return { 'db.cf.r2.key': argArray[0] }
	},
	resumeMultipartUpload(argArray) {
		return {
			'db.cf.r2.key': argArray[0],
			'db.cf.r2.upload_id': argArray[1],
		}
	},
}

function instrumentR2Fn(fn: Function, name: string, operation: string) {
	const tracer = trace.getTracer('R2')
	const fnHandler: ProxyHandler<any> = {
		apply: (target, thisArg, argArray) => {
			const attributes: Attributes = {
				binding_type: 'R2',
				'db.name': name,
				'db.system': dbSystem,
				'db.operation': operation,
			}
			const options: SpanOptions = {
				kind: SpanKind.CLIENT,
				attributes,
			}
			return tracer.startActiveSpan(`R2 ${name} ${operation}`, options, async (span) => {
				try {
					const result = await Reflect.apply(target, thisArg, argArray)
					const extraAttrsFn = R2Attributes[operation]
					const extraAttrs = extraAttrsFn ? extraAttrsFn(argArray, result) : {}
					span.setAttributes(extraAttrs)
					span.setAttribute('db.cf.r2.has_result', !!result)
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

export function instrumentR2(bucket: R2Bucket, name: string): R2Bucket {
	const bucketHandler: ProxyHandler<R2Bucket> = {
		get: (target, prop, receiver) => {
			const operation = String(prop)
			const fn = Reflect.get(target, prop, receiver)
			if (typeof fn === 'function') {
				return instrumentR2Fn(fn, name, operation)
			}
			return fn
		},
	}
	return wrap(bucket, bucketHandler)
}
