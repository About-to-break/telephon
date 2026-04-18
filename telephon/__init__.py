# internal/telemetry/__init__.py
"""
Telemetry module for NMT and VLLM services.

Provides lightweight metrics collection with separate HTTP server
for Prometheus scraping. Designed for minimal overhead in hot path.

Usage:
    from internal.telemetry import init_metrics, get_metrics

    # Initialize at service startup
    metrics = init_metrics(
        service_name="nmt_service",
        max_concurrent=32,
        prefetch_count=32
    )

    # Start HTTP server for Prometheus
    metrics.start(port=9090)

    # In request handler
    metrics.record_request_start()
    # ... process request ...
    metrics.record_request_end(service_time=0.5, wait_time=0.1, success=True)

    # Stop gracefully
    metrics.stop()

Endpoints:
    /health          - Health check
    /metrics/warm    - Fast metrics (active requests, totals)
    /metrics/stats   - Heavy metrics (percentiles, distributions)
"""

from .metrics_server import (
    SafeMetricsCollector,
    init_metrics,
    get_metrics,
    reset_metrics,
)

__all__ = [
    "SafeMetricsCollector",
    "init_metrics",
    "get_metrics",
    "reset_metrics",
]

__version__ = "0.1.0"