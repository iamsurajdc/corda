package net.corda.nodeapi.internal.tracing

import co.paralleluniverse.strands.Strand
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.*
import io.jaegertracing.internal.samplers.ConstSampler
import io.opentracing.Scope
import io.opentracing.Span
import io.opentracing.Tracer
import io.opentracing.log.Fields
import io.opentracing.tag.Tags
import net.corda.core.context.InvocationOrigin
import net.corda.core.internal.FlowStateMachine
import java.util.concurrent.ConcurrentHashMap

class CordaTracer private constructor(private val tracer: Tracer) {

    class Builder {
        var endpoint: String = "http://localhost:14268/api/traces"
            private set

        var serviceName: String = "Corda"
            private set

        var flushInterval: Int = 200
            private set

        var logSpans: Boolean = true
            private set

        fun withEndpoint(endpoint: String): Builder {
            this.endpoint = endpoint
            return this
        }

        fun withServiceName(serviceName: String): Builder {
            this.serviceName = serviceName
            return this
        }

        fun withFlushInterval(interval: Int): Builder {
            this.flushInterval = interval
            return this
        }

        fun withLogSpans(logSpans: Boolean): Builder {
            this.logSpans = logSpans
            return this
        }
    }

    companion object {

        private fun createTracer(name: String): CordaTracer {
            val builder = Builder()
                    .withServiceName("Corda ($name)")
            val sampler = SamplerConfiguration.fromEnv()
                    .withType(ConstSampler.TYPE)
                    .withParam(1)
            val sender = SenderConfiguration.fromEnv()
                    .withEndpoint(builder.endpoint)
            val reporter = ReporterConfiguration.fromEnv()
                    .withSender(sender)
                    .withLogSpans(builder.logSpans)
                    .withFlushInterval(builder.flushInterval)
            val tracer: Tracer = Configuration(builder.serviceName)
                    .withSampler(sampler)
                    .withReporter(reporter)
                    .tracer
            return CordaTracer(tracer)
        }

        private val tracers: ConcurrentHashMap<String, CordaTracer> = ConcurrentHashMap()

        val current: CordaTracer
            get() = getTracer(identity)

        fun getTracer(name: String): CordaTracer {
            return tracers.getOrPut(name) {
                createTracer(name)
            }
        }

        fun Span?.tag(key: String, value: Any?) {
            val span = this ?: return
            when (value) {
                is Boolean -> span.setTag(key, value)
                is Number -> span.setTag(key, value)
                else -> span.setTag(key, value?.toString())
            }
        }

        fun Span?.error(message: String, exception: Throwable? = null) {
            val span = this ?: return
            Tags.ERROR.set(span, true)
            if (exception != null) {
                span.log(mapOf(
                        Fields.EVENT to "error",
                        Fields.ERROR_OBJECT to exception,
                        Fields.MESSAGE to message
                ))
            } else {
                span.log(mapOf(
                        Fields.EVENT to "error",
                        Fields.MESSAGE to message
                ))
            }
        }

        fun Span?.finish() = this?.finish()

        private val identity: String
            get() = flow?.ourIdentity?.name?.organisation ?: flow?.ourIdentity?.name.toString() ?: "Unknown"

        private val flow: FlowStateMachine<*>?
            get() = Strand.currentStrand() as? FlowStateMachine<*>

        private val FlowStateMachine<*>.flowId: String
            get() = this.id.uuid.toString()

        private fun Tracer.SpanBuilder.decorate() {
            flow?.apply {
                withTag("flow-id", flowId)
                withTag("our-identity", ourIdentity.toString())
                withTag("fiber-id", Strand.currentStrand().id)
                withTag("thread-id", Thread.currentThread().id)
                withTag("session-id", context.trace.sessionId.value)
                withTag("invocation-id", context.trace.invocationId.value)
                withTag("origin", when (context.origin) {
                    is InvocationOrigin.Peer -> "peer(${(context.origin as InvocationOrigin.Peer).party})"
                    is InvocationOrigin.RPC -> "rpc(user=${context.origin.principal().name}, ${(context.origin as InvocationOrigin.RPC).actor.owningLegalIdentity})"
                    is InvocationOrigin.Scheduled -> "scheduled(${(context.origin as InvocationOrigin.Scheduled).scheduledState.ref})"
                    is InvocationOrigin.Service -> "service(${(context.origin as InvocationOrigin.Service).owningLegalIdentity})"
                    is InvocationOrigin.Shell -> "shell"
                    else -> context.origin.toString()
                })
            }
        }

        var rootSpan: Span? = null

        private fun createRootSpanIfNeeded(): Span? {
            if (rootSpan != null) {
                return rootSpan
            }
            rootSpan = current.tracer.buildSpan("Execution").start()
            current.tracer.scopeManager().activate(rootSpan, true)
            return rootSpan
        }

        fun terminate() {
            rootSpan?.finish()
        }

    }

    private var flowSpans: ConcurrentHashMap<String, Pair<Span, Scope>> = ConcurrentHashMap()

    fun <T> flowSpan(action: (Span, FlowStateMachine<*>) -> T): T? {
        return flow?.let { flow ->
            val (span, _) = flowSpans.getOrPut(flow.flowId) {
                val tracer = tracer
                val span = tracer.buildSpan(flow.logic.toString()).apply {
                    createRootSpanIfNeeded()
                    withTag("parent-context", rootSpan!!.context().toString())
                    decorate()
                }.start()
                val scope = tracer.scopeManager().activate(span, true)
                span to scope
            }
            action(span, flow)
        }
    }

    fun span(name: String): Pair<Span, Scope>? {
        return flowSpan { parentSpan, _ ->
            val tracer = tracer
            val span = tracer.buildSpan(name).apply {
                withTag("parent-context", parentSpan.context().toString())
                decorate()

            }.start()
            val scope = tracer.scopeManager().activate(span, false)
            span?.let { it -> it to scope }
        }
    }

    fun <T> span(name: String, closeAutomatically: Boolean = true, block: (Span?) -> T): T {
        return span(name)?.let { (span, scope) ->
            try {
                block(span)
            } catch (ex: Exception) {
                Tags.ERROR.set(span, true)
                span.log(mapOf(
                        Fields.EVENT to "error",
                        Fields.ERROR_OBJECT to ex,
                        Fields.MESSAGE to ex.message
                ))
                throw ex
            } finally {
                if (closeAutomatically) {
                    span.finish()
                }
                scope.close()
            }
        } ?: block(null)
    }

    fun endFlow() {
        flow?.id?.uuid.toString().let { flowId ->
            val (span, scope) = flowSpans.remove(flowId) ?: null to null
            span?.finish()
            scope?.close()
        }
    }

}