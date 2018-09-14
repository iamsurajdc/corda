package net.corda.node.services.api

import com.codahale.metrics.MetricRegistry
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.nodeapi.internal.tracing.CordaTracer

/**
 * Provides access to various metrics and ways to notify monitoring services of things, for sysadmin purposes.
 * This is not an interface because it is too lightweight to bother mocking out.
 */
class MonitoringService(
        val metrics: MetricRegistry,
        val tracer: CordaTracer
) : SingletonSerializeAsToken()
