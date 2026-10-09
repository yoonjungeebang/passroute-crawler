"""CloudWatch Embedded Metric Format(EMF) 기반 성능 메트릭 발행.

EMF 는 구조화된 JSON 로그를 CloudWatch Logs 에 출력하면,
CloudWatch 가 자동으로 메트릭을 추출한다. 별도 API 호출이 없으므로
추가 비용과 지연이 발생하지 않는다.

사용법::

    from core.metrics import MetricsLogger

    metrics = MetricsLogger(function_name="search_api")
    metrics.set_dimension("Source", "jumpit")

    t0 = time.monotonic()
    result = embed_text(query)
    metrics.put_duration("EmbeddingDuration", t0)

    metrics.put_metric("ResultCount", len(results), "Count")
    metrics.flush()  # 반드시 호출하여 로그 출력

참고:
    - https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html
    - Lambda 의 LogFormat: JSON 설정(template.yaml Globals)과 호환됨
"""
import json
import logging
import time

from config import load_metrics_config

logger = logging.getLogger(__name__)

_METRICS_CONFIG = load_metrics_config()
_NAMESPACE: str = _METRICS_CONFIG["namespace"]


class MetricsLogger:
    """단일 Lambda 호출 내에서 메트릭을 수집하고, flush() 시 EMF JSON 을 stdout 에 출력한다."""

    def __init__(self, function_name: str):
        self._function_name = function_name
        self._metrics: list[dict] = []
        self._values: dict[str, float] = {}
        self._dimensions: dict[str, str] = {"FunctionName": function_name}

    def set_dimension(self, key: str, value: str) -> None:
        """추가 차원을 설정한다. flush() 전에 호출."""
        self._dimensions[key] = value

    def put_metric(self, name: str, value: float, unit: str = "None") -> None:
        """메트릭 값을 기록한다."""
        self._metrics.append({"Name": name, "Unit": unit})
        self._values[name] = value

    def put_duration(self, name: str, start_time: float) -> float:
        """start_time(time.monotonic()) 부터 현재까지의 경과 시간을 밀리초 단위로 기록한다.

        단조 시계(monotonic clock)를 사용해야 NTP 보정에 의한 시간 점프 영향을 받지 않는다.

        Returns:
            경과 시간(밀리초)
        """
        elapsed_ms = (time.monotonic() - start_time) * 1000
        self.put_metric(name, round(elapsed_ms, 1), "Milliseconds")
        return elapsed_ms

    def put_count(self, name: str, value: int) -> None:
        """카운트 메트릭을 기록한다."""
        self.put_metric(name, value, "Count")

    def flush(self) -> None:
        """수집된 메트릭을 EMF JSON 으로 stdout 에 출력한다.

        메트릭이 없으면 아무것도 출력하지 않는다.
        """
        if not self._metrics:
            return

        dimension_keys = list(self._dimensions.keys())

        emf = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": _NAMESPACE,
                        "Dimensions": [dimension_keys],
                        "Metrics": self._metrics,
                    }
                ],
            },
        }

        # 차원 값과 메트릭 값을 최상위에 배치 (EMF 스펙 요구사항)
        emf.update(self._dimensions)
        emf.update(self._values)

        # EMF 는 반드시 stdout 으로 출력해야 CloudWatch 가 인식한다.
        # Lambda 의 JSON LogFormat 과 충돌하지 않도록 print 사용.
        print(json.dumps(emf))

        # 재사용 방지
        self._metrics = []
        self._values = {}
