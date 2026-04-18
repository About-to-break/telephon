# internal/telemetry/metrics_server.py
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SafeMetricsCollector:
    """
    Полностью безопасный коллектор метрик.
    Использует отдельный поток для HTTP сервера.
    """

    def __init__(self, service_name: str, max_concurrent: int, prefetch_count: int):
        self.service_name = service_name
        self.max_concurrent = max_concurrent
        self.prefetch_count = prefetch_count

        # Поток для HTTP сервера метрик
        self._metrics_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()

        self._lock = threading.Lock()
        self._metrics_data = {
            'active_requests': 0,
            'total_requests': 0,
            'total_completed': 0,
            'total_errors': 0,
            'service_times': [],
            'wait_times': [],
        }

        self._port = 9090

    # === ASYNCIO МЕТОДЫ (вызываются из event loop) ===

    def record_request_start(self):
        """Вызывается из asyncio. Безопасно!"""
        with self._lock:
            self._metrics_data['active_requests'] += 1
            self._metrics_data['total_requests'] += 1

    def record_request_end(self, service_time: float, wait_time: float = 0.0, success: bool = True):
        """Вызывается из asyncio. Безопасно!"""
        with self._lock:
            self._metrics_data['active_requests'] -= 1

            if success:
                self._metrics_data['total_completed'] += 1
            else:
                self._metrics_data['total_errors'] += 1

            self._metrics_data['service_times'].append(service_time)
            if len(self._metrics_data['service_times']) > 1000:
                self._metrics_data['service_times'].pop(0)

            self._metrics_data['wait_times'].append(wait_time)
            if len(self._metrics_data['wait_times']) > 1000:
                self._metrics_data['wait_times'].pop(0)

    def start(self, port: int = 9090):
        """Запускает поток метрик"""
        self._port = port
        self._metrics_thread = threading.Thread(
            target=self._run_metrics_server,
            daemon=True,
            name="MetricsHTTP"
        )
        self._metrics_thread.start()
        logger.info(f"Metrics server started on port {port} (separate thread)")

    def stop(self):
        """Останавливает поток метрик"""
        self._shutdown_event.set()
        if self._metrics_thread:
            self._metrics_thread.join(timeout=5.0)
            logger.info("Metrics server stopped")

    # === THREADING МЕТОДЫ (вызываются из потока метрик) ===

    def _run_metrics_server(self):
        """Запускается в отдельном потоке"""
        collector = self

        def create_handler(*args, **kwargs):
            return MetricsHandler(collector, *args, **kwargs)

        class MetricsHandler(BaseHTTPRequestHandler):
            def __init__(self, collector_instance, *args, **kwargs):
                self.collector = collector_instance
                super().__init__(*args, **kwargs)

            def do_GET(self):
                try:
                    if self.path == '/metrics/warm':
                        metrics_text = self.collector._get_warm_metrics_safe()
                        self._send_response(200, metrics_text)

                    elif self.path == '/health':
                        self._send_response(200, "OK", content_type='text/plain')

                    elif self.path == '/metrics/stats':
                        metrics_text = self.collector._get_stats_safe()
                        self._send_response(200, metrics_text)

                    else:
                        self._send_response(404, "Not Found", content_type='text/plain')

                except Exception as e:
                    logger.error(f"Error in {self.path}: {e}", exc_info=True)
                    self._send_response(500, f"Internal Error: {e}", content_type='text/plain')

            def _send_response(self, code: int, body: str, content_type: str = 'text/plain; charset=utf-8'):
                """Отправляет HTTP ответ"""
                self.send_response(code)
                self.send_header('Content-Type', content_type)
                self.end_headers()
                self.wfile.write(body.encode('utf-8'))

            def log_message(self, format, *args):
                # Отключаем стандартное логирование HTTP запросов
                pass

        server = HTTPServer(('0.0.0.0', self._port), create_handler)
        server.timeout = 1.0

        while not self._shutdown_event.is_set():
            server.handle_request()

        server.server_close()

    def _get_warm_metrics_safe(self) -> str:
        """Безопасное получение warm метрик с блокировкой"""
        with self._lock:
            # Копируем данные под блокировкой
            active = self._metrics_data['active_requests']
            total = self._metrics_data['total_requests']
            completed = self._metrics_data['total_completed']
            errors = self._metrics_data['total_errors']
            free_slots = self.max_concurrent - active

        service_name = self.service_name

        # Формируем ответ без лишних пустых строк
        lines = [
            f"# HELP {service_name}_active_requests Currently active requests",
            f"# TYPE {service_name}_active_requests gauge",
            f'{service_name}_active_requests{{service="{service_name}"}} {active}',
            "",
            f"# HELP {service_name}_free_slots Free handler slots",
            f"# TYPE {service_name}_free_slots gauge",
            f'{service_name}_free_slots{{service="{service_name}"}} {free_slots}',
            "",
            f"# HELP {service_name}_requests_total Total requests received",
            f"# TYPE {service_name}_requests_total counter",
            f'{service_name}_requests_total{{service="{service_name}"}} {total}',
            "",
            f"# HELP {service_name}_requests_completed Successfully completed requests",
            f"# TYPE {service_name}_requests_completed counter",
            f'{service_name}_requests_completed{{service="{service_name}"}} {completed}',
            "",
            f"# HELP {service_name}_requests_errors Failed requests",
            f"# TYPE {service_name}_requests_errors counter",
            f'{service_name}_requests_errors{{service="{service_name}"}} {errors}',
            "",
            f"# HELP {service_name}_max_concurrent Maximum concurrent requests",
            f"# TYPE {service_name}_max_concurrent gauge",
            f'{service_name}_max_concurrent{{service="{service_name}"}} {self.max_concurrent}',
            "",
            f"# HELP {service_name}_prefetch_count RabbitMQ prefetch count",
            f"# TYPE {service_name}_prefetch_count gauge",
            f'{service_name}_prefetch_count{{service="{service_name}"}} {self.prefetch_count}',
            "",
        ]

        return "\n".join(lines)

    def _get_stats_safe(self) -> str:
        """Получение расширенных статистических показателей"""
        with self._lock:
            # Копируем данные под блокировкой
            completed = self._metrics_data['total_completed']
            service_times = self._metrics_data['service_times'].copy()
            wait_times = self._metrics_data['wait_times'].copy()

        # Если нет данных - возвращаем нули
        if not service_times:
            service_name = self.service_name
            lines = [
                f"# HELP {service_name}_service_time_seconds Service time distribution",
                f"# TYPE {service_name}_service_time_seconds summary",
                f'{service_name}_service_time_seconds{{quantile="0.5",service="{service_name}"}} 0.0',
                f'{service_name}_service_time_seconds{{quantile="0.95",service="{service_name}"}} 0.0',
                f'{service_name}_service_time_seconds{{quantile="0.99",service="{service_name}"}} 0.0',
                f'{service_name}_service_time_seconds_sum{{service="{service_name}"}} 0.0',
                f'{service_name}_service_time_seconds_count{{service="{service_name}"}} 0',
                "",
                f"# HELP {service_name}_wait_time_seconds Queue wait time distribution",
                f"# TYPE {service_name}_wait_time_seconds summary",
                f'{service_name}_wait_time_seconds{{quantile="0.5",service="{service_name}"}} 0.0',
                f'{service_name}_wait_time_seconds{{quantile="0.95",service="{service_name}"}} 0.0',
                f'{service_name}_wait_time_seconds_sum{{service="{service_name}"}} 0.0',
                f'{service_name}_wait_time_seconds_count{{service="{service_name}"}} 0',
                "",
            ]
            return "\n".join(lines)

        # Тяжелые вычисления БЕЗ блокировки
        avg_service_time = sum(service_times) / len(service_times)
        avg_wait_time = sum(wait_times) / len(wait_times) if wait_times else 0.0

        # Квантили
        sorted_times = sorted(service_times)
        n = len(sorted_times)
        p50_service = sorted_times[int(n * 0.5)]
        p95_service = sorted_times[min(int(n * 0.95), n - 1)]
        p99_service = sorted_times[min(int(n * 0.99), n - 1)]

        p50_wait = p95_wait = 0.0
        if wait_times:
            sorted_wait = sorted(wait_times)
            n_wait = len(sorted_wait)
            p50_wait = sorted_wait[int(n_wait * 0.5)]
            p95_wait = sorted_wait[min(int(n_wait * 0.95), n_wait - 1)]

        service_name = self.service_name

        lines = [
            f"# HELP {service_name}_service_time_seconds Service time distribution",
            f"# TYPE {service_name}_service_time_seconds summary",
            f'{service_name}_service_time_seconds{{quantile="0.5",service="{service_name}"}} {p50_service:.3f}',
            f'{service_name}_service_time_seconds{{quantile="0.95",service="{service_name}"}} {p95_service:.3f}',
            f'{service_name}_service_time_seconds{{quantile="0.99",service="{service_name}"}} {p99_service:.3f}',
            f'{service_name}_service_time_seconds_sum{{service="{service_name}"}} {avg_service_time * completed:.3f}',
            f'{service_name}_service_time_seconds_count{{service="{service_name}"}} {completed}',
            "",
            f"# HELP {service_name}_wait_time_seconds Queue wait time distribution",
            f"# TYPE {service_name}_wait_time_seconds summary",
            f'{service_name}_wait_time_seconds{{quantile="0.5",service="{service_name}"}} {p50_wait:.6f}',
            f'{service_name}_wait_time_seconds{{quantile="0.95",service="{service_name}"}} {p95_wait:.6f}',
            f'{service_name}_wait_time_seconds_sum{{service="{service_name}"}} {avg_wait_time * completed:.6f}',
            f'{service_name}_wait_time_seconds_count{{service="{service_name}"}} {completed}',
            "",
        ]

        return "\n".join(lines)


# Глобальный экземпляр
_collector: Optional[SafeMetricsCollector] = None
_initialized = False


def init_metrics(service_name: str, max_concurrent: int, prefetch_count: int) -> SafeMetricsCollector:
    """Инициализирует глобальный коллектор (только один раз)"""
    global _collector, _initialized

    if not _initialized:
        _collector = SafeMetricsCollector(service_name, max_concurrent, prefetch_count)
        _initialized = True

    return _collector


def get_metrics() -> Optional[SafeMetricsCollector]:
    """Возвращает глобальный коллектор"""
    return _collector


def reset_metrics():
    """Сбрасывает глобальный коллектор (для тестов)"""
    global _collector, _initialized
    _collector = None
    _initialized = False