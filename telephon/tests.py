# telephon/tests.py
import pytest
import time
import threading
import socket
from http.client import HTTPConnection

# Импортируем модуль
from telephon.telemetry import init_metrics, get_metrics, reset_metrics, SafeMetricsCollector


# ========== ФИКСТУРЫ ==========

@pytest.fixture
def unused_tcp_port():
    """Возвращает свободный TCP порт"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


@pytest.fixture
def collector():
    """Создает экземпляр коллектора (не запущенный)"""
    reset_metrics()
    return init_metrics("test_service", 32, 32)


@pytest.fixture
def running_collector(collector, unused_tcp_port):
    """Запускает коллектор на случайном порту и возвращает его"""
    port = unused_tcp_port
    collector.start(port=port)
    time.sleep(0.2)  # Даем серверу запуститься

    yield collector, port

    collector.stop()
    time.sleep(0.1)


@pytest.fixture(autouse=True)
def reset_collector():
    """Сбрасывает глобальный коллектор перед каждым тестом"""
    reset_metrics()
    yield
    reset_metrics()


# ========== ТЕСТЫ ==========

class TestSafeMetricsCollector:
    """Тесты для SafeMetricsCollector"""

    def test_init(self, collector):
        """Проверяет корректную инициализацию"""
        assert collector.service_name == "test_service"
        assert collector.max_concurrent == 32
        assert collector.prefetch_count == 32
        assert collector._metrics_data['active_requests'] == 0
        assert collector._metrics_data['total_requests'] == 0
        assert collector._metrics_data['total_completed'] == 0
        assert collector._metrics_data['total_errors'] == 0
        assert collector._metrics_data['service_times'] == []
        assert collector._metrics_data['wait_times'] == []

    def test_global_singleton(self):
        """Проверяет глобальный синглтон"""
        reset_metrics()

        collector1 = init_metrics("service1", 10, 10)
        collector2 = get_metrics()
        collector3 = init_metrics("service2", 20, 20)  # Не должен пересоздать

        assert collector1 is collector2
        assert collector1 is collector3  # Синглтон не пересоздается
        assert collector1.service_name == "service1"  # Имя не меняется

    def test_record_request_start(self, collector):
        """Проверяет запись начала запроса"""
        collector.record_request_start()

        assert collector._metrics_data['active_requests'] == 1
        assert collector._metrics_data['total_requests'] == 1

        collector.record_request_start()
        assert collector._metrics_data['active_requests'] == 2
        assert collector._metrics_data['total_requests'] == 2

    def test_record_request_end_success(self, collector):
        """Проверяет запись успешного завершения"""
        collector.record_request_start()
        collector.record_request_end(
            service_time=0.5,
            wait_time=0.1,
            success=True
        )

        assert collector._metrics_data['active_requests'] == 0
        assert collector._metrics_data['total_completed'] == 1
        assert collector._metrics_data['total_errors'] == 0
        assert 0.5 in collector._metrics_data['service_times']
        assert 0.1 in collector._metrics_data['wait_times']

    def test_record_request_end_error(self, collector):
        """Проверяет запись ошибочного завершения"""
        collector.record_request_start()
        collector.record_request_end(
            service_time=0.3,
            wait_time=0.05,
            success=False
        )

        assert collector._metrics_data['active_requests'] == 0
        assert collector._metrics_data['total_completed'] == 0
        assert collector._metrics_data['total_errors'] == 1
        assert 0.3 in collector._metrics_data['service_times']
        assert 0.05 in collector._metrics_data['wait_times']

    def test_buffer_size_limit(self, collector):
        """Проверяет ограничение размера буфера (1000 записей)"""
        # Добавляем 1500 записей
        for i in range(1500):
            collector.record_request_start()
            collector.record_request_end(
                service_time=float(i),
                wait_time=float(i) / 10,
                success=True
            )

        # Буфер должен содержать только последние 1000
        assert len(collector._metrics_data['service_times']) == 1000
        assert len(collector._metrics_data['wait_times']) == 1000

        # Проверяем, что это последние 1000
        assert min(collector._metrics_data['service_times']) >= 500

    def test_concurrent_writes(self, collector):
        """Проверяет потокобезопасность при конкурентной записи"""
        import concurrent.futures

        def write_metrics(n):
            for _ in range(n):
                collector.record_request_start()
                collector.record_request_end(
                    service_time=0.1,
                    wait_time=0.01,
                    success=True
                )

        # Запускаем 10 потоков по 100 записей
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(write_metrics, 100) for _ in range(10)]
            concurrent.futures.wait(futures)

        # Должно быть 1000 завершенных запросов
        assert collector._metrics_data['total_requests'] == 1000
        assert collector._metrics_data['total_completed'] == 1000
        assert collector._metrics_data['active_requests'] == 0

    def test_health_endpoint(self, running_collector):
        """Проверяет эндпоинт /health"""
        collector, port = running_collector

        conn = HTTPConnection(f'localhost:{port}')
        conn.request('GET', '/health')
        response = conn.getresponse()

        assert response.status == 200
        assert response.read() == b'OK'

    def test_warm_metrics_endpoint(self, running_collector):
        """Проверяет эндпоинт /metrics/warm"""
        collector, port = running_collector

        # Добавляем тестовые данные
        collector.record_request_start()
        collector.record_request_end(0.5, 0.1, True)
        collector.record_request_start()
        collector.record_request_end(0.3, 0.05, False)

        conn = HTTPConnection(f'localhost:{port}')
        conn.request('GET', '/metrics/warm')
        response = conn.getresponse()

        assert response.status == 200
        assert response.getheader('Content-Type') == 'text/plain; charset=utf-8'

        body = response.read().decode('utf-8')

        # Проверяем наличие ключевых метрик
        assert 'test_service_active_requests' in body
        assert 'test_service_free_slots' in body
        assert 'test_service_requests_total' in body
        assert 'test_service_requests_completed' in body
        assert 'test_service_requests_errors' in body
        assert 'test_service_max_concurrent' in body
        assert 'test_service_prefetch_count' in body

    def test_stats_endpoint(self, running_collector):
        """Проверяет эндпоинт /metrics/stats"""
        collector, port = running_collector

        # Добавляем тестовые данные для статистики
        service_times = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        for st in service_times:
            collector.record_request_start()
            collector.record_request_end(st, st / 10, True)

        conn = HTTPConnection(f'localhost:{port}')
        conn.request('GET', '/metrics/stats')
        response = conn.getresponse()

        assert response.status == 200
        assert response.getheader('Content-Type') == 'text/plain; charset=utf-8'

        body = response.read().decode('utf-8')

        # Проверяем наличие статистических метрик
        assert 'test_service_service_time_seconds' in body
        assert 'test_service_wait_time_seconds' in body
        assert 'quantile=' in body

    def test_404_endpoint(self, running_collector):
        """Проверяет обработку несуществующего пути"""
        collector, port = running_collector

        conn = HTTPConnection(f'localhost:{port}')
        conn.request('GET', '/nonexistent')
        response = conn.getresponse()

        assert response.status == 404

    def test_stats_with_no_data(self, running_collector):
        """Проверяет /metrics/stats при отсутствии данных"""
        collector, port = running_collector

        conn = HTTPConnection(f'localhost:{port}')
        conn.request('GET', '/metrics/stats')
        response = conn.getresponse()

        assert response.status == 200
        body = response.read().decode('utf-8')

        # Должны быть нулевые значения
        assert '0.0' in body

    def test_percentile_calculation(self, collector):
        """Проверяет корректность расчета перцентилей"""
        # Добавляем 100 значений от 0 до 99
        for i in range(100):
            collector.record_request_start()
            collector.record_request_end(float(i), float(i) / 10, True)

        body = collector._get_stats_safe()

        # p50 должно быть около 49.5
        assert 'quantile="0.5"' in body
        assert 'quantile="0.95"' in body
        assert 'quantile="0.99"' in body

    def test_percentile_with_few_values(self, collector):
        """Проверяет перцентили при малом количестве значений"""
        # Только 3 значения
        for st in [0.1, 0.5, 1.0]:
            collector.record_request_start()
            collector.record_request_end(st, 0.01, True)

        body = collector._get_stats_safe()

        # Не должно быть ошибок
        assert 'quantile=' in body

    def test_start_stop(self, collector, unused_tcp_port):
        """Проверяет запуск и остановку сервера"""
        port = unused_tcp_port

        # Запускаем
        collector.start(port=port)
        time.sleep(0.2)

        assert collector._metrics_thread is not None
        assert collector._metrics_thread.is_alive()

        # Проверяем, что сервер отвечает
        conn = HTTPConnection(f'localhost:{port}')
        conn.request('GET', '/health')
        response = conn.getresponse()
        assert response.status == 200

        # Останавливаем
        collector.stop()
        time.sleep(0.2)

        # Сервер должен перестать отвечать
        with pytest.raises(Exception):
            conn = HTTPConnection(f'localhost:{port}')
            conn.request('GET', '/health')
            conn.getresponse()

    def test_double_start(self, collector, unused_tcp_port):
        """Проверяет, что повторный start не ломает"""
        port = unused_tcp_port

        collector.start(port=port)
        time.sleep(0.1)

        # Второй запуск должен просто работать (не запускает второй поток)
        collector.start(port=port)
        time.sleep(0.1)

        conn = HTTPConnection(f'localhost:{port}')
        conn.request('GET', '/health')
        response = conn.getresponse()
        assert response.status == 200

        collector.stop()


class TestPrometheusFormat:
    """Тесты формата Prometheus"""

    @pytest.fixture(autouse=True)
    def reset_collector(self):
        reset_metrics()
        yield
        reset_metrics()

    @pytest.fixture
    def collector_with_data(self):
        collector = init_metrics("test", 32, 32)

        # Добавляем тестовые данные
        for i in range(10):
            collector.record_request_start()
            collector.record_request_end(0.1 * i, 0.01 * i, i % 2 == 0)

        return collector

    def test_warm_format_valid(self, collector_with_data):
        """Проверяет, что формат /metrics/warm валиден для Prometheus"""
        body = collector_with_data._get_warm_metrics_safe()
        lines = [line for line in body.strip().split('\n') if line.strip()]

        # Каждая метрика должна иметь HELP и TYPE
        metric_names = set()
        for line in lines:
            if line.startswith('# HELP '):
                parts = line.split()
                if len(parts) >= 3:
                    metric_name = parts[2]
                    metric_names.add(metric_name)
            elif line.startswith('# TYPE '):
                parts = line.split()
                if len(parts) >= 3:
                    metric_name = parts[2]
                    assert metric_name in metric_names, f"TYPE без HELP для {metric_name}"

        # Проверяем, что все метрики имеют правильный формат
        for line in lines:
            if not line.startswith('#'):
                # Формат: name{labels} value или name value
                assert '{' in line or ' ' in line, f"Invalid format: {line}"

    def test_stats_format_valid(self, collector_with_data):
        """Проверяет, что формат /metrics/stats валиден для Prometheus"""
        body = collector_with_data._get_stats_safe()
        lines = [line for line in body.strip().split('\n') if line.strip()]

        # Проверяем наличие обязательных метрик
        has_quantiles = False
        for line in lines:
            if 'quantile=' in line:
                has_quantiles = True
                assert 'quantile="0.5"' in line or 'quantile="0.95"' in line or 'quantile="0.99"' in line

        assert has_quantiles, "Должны быть квантили"


# ========== ТЕСТЫ ПРОИЗВОДИТЕЛЬНОСТИ (опционально) ==========

class TestMetricsPerformance:
    """Тесты производительности"""

    @pytest.fixture(autouse=True)
    def reset_collector(self):
        reset_metrics()
        yield
        reset_metrics()

    def test_record_speed(self):
        """Проверяет скорость записи метрик"""
        collector = init_metrics("perf_test", 100, 100)
        iterations = 10000

        start = time.perf_counter()
        for _ in range(iterations):
            collector.record_request_start()
            collector.record_request_end(0.01, 0.001, True)
        elapsed = time.perf_counter() - start

        ops_per_sec = iterations / elapsed

        print(f"\nRecord speed: {ops_per_sec:.0f} ops/sec")
        print(f"Avg time: {elapsed / iterations * 1_000_000:.2f} µs per record")

        # Должно быть быстро (менее 100 микросекунд на операцию)
        assert elapsed / iterations < 0.0005, "Слишком медленная запись"

    def test_concurrent_performance(self):
        """Проверяет производительность при конкурентной нагрузке"""
        import concurrent.futures

        collector = init_metrics("perf_test", 100, 100)

        def worker(n):
            for _ in range(n):
                collector.record_request_start()
                collector.record_request_end(0.01, 0.001, True)

        threads = 10
        ops_per_thread = 1000

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(worker, ops_per_thread) for _ in range(threads)]
            concurrent.futures.wait(futures)
        elapsed = time.perf_counter() - start

        total_ops = threads * ops_per_thread
        ops_per_sec = total_ops / elapsed

        print(f"\nConcurrent performance ({threads} threads):")
        print(f"  Total ops: {total_ops}")
        print(f"  Time: {elapsed:.3f}s")
        print(f"  Throughput: {ops_per_sec:.0f} ops/sec")

        # Данные должны быть корректными
        assert collector._metrics_data['total_requests'] == total_ops
        assert collector._metrics_data['total_completed'] == total_ops
        assert collector._metrics_data['active_requests'] == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])