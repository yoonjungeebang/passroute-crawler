"""설정 파일 로딩 모듈."""

from pathlib import Path

import yaml

# config 디렉토리 경로
_CONFIG_DIR = Path(__file__).parent


def load_yaml(filename: str) -> dict:
    """config/ 디렉토리에서 YAML 파일을 읽어 dict로 반환한다."""
    path = _CONFIG_DIR / filename
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_naver_news_config() -> dict:
    """네이버 뉴스 설정을 로드한다.

    Returns:
        {
            "search_suffixes": tuple[str, ...],
            "exclude_news_title_keywords": tuple[str, ...],
        }
    """
    raw = load_yaml("naver_news.yaml")

    return {
        "api_url": raw["api_url"],
        "retention_days": int(raw["retention_days"]),
        "api_delay": float(raw["api_delay"]),
        "max_display": int(raw["max_display"]),
        "api_timeout": int(raw["api_timeout"]),
        "search_suffixes": tuple(raw["search_suffixes"]),
        "exclude_news_title_keywords": tuple(raw["exclude_news_title_keywords"]),
    }


def load_tech_blog_config() -> dict:
    """기술 블로그 설정을 로드한다."""
    raw = load_yaml("tech_blog.yaml")

    return {
        "min_content_length": int(raw["min_content_length"]),
        "page_timeout": int(raw["page_timeout"]),
        "feeds": raw["feeds"],
    }


def load_embedding_config() -> dict:
    """임베딩 모델 설정을 로드한다.

    model_dir 은 환경변수 EMBEDDING_MODEL_DIR 로 오버라이드 가능.
    """
    import os

    raw = load_yaml("embedding.yaml")
    model_dir = os.environ.get("EMBEDDING_MODEL_DIR") or raw["model_dir"]

    return {
        "model_name": raw["model_name"],
        "model_dir": model_dir,
        "onnx_path": os.path.join(model_dir, raw["onnx_filename"]),
        "tokenizer_path": os.path.join(model_dir, raw["tokenizer_dirname"]),
        "max_chunk_tokens": int(raw["max_chunk_tokens"]),
        "inference_batch_size": int(raw["inference_batch_size"]),
    }


def load_metrics_config() -> dict:
    """CloudWatch 메트릭 설정을 로드한다."""
    raw = load_yaml("metrics.yaml")

    return {
        "namespace": raw["namespace"],
    }


def load_crawler_config() -> dict:
    """크롤러 공통 설정을 로드한다."""
    raw = load_yaml("crawler.yaml")

    return {
        "delay_min": float(raw["delay_min"]),
        "delay_max": float(raw["delay_max"]),
        "max_pages": int(raw["max_pages"]),
        "stale_page_threshold": int(raw["stale_page_threshold"]),
        "robots_cache_ttl": int(raw["robots_cache_ttl"]),
    }


def load_circuit_breaker_config() -> dict:
    """서킷 브레이커 설정을 로드한다."""
    raw = load_yaml("circuit_breaker.yaml")

    return {
        "failure_threshold": int(raw["failure_threshold"]),
        "recovery_timeout": float(raw["recovery_timeout"]),
    }


def load_search_config() -> dict:
    """네이버 실시간 검색 설정을 로드한다."""
    raw = load_yaml("search.yaml")

    return {
        "news_url": raw["news_url"],
        "webkr_url": raw["webkr_url"],
        "timeout": int(raw["timeout"]),
        "retry_total": int(raw["retry_total"]),
        "retry_backoff_factor": float(raw["retry_backoff_factor"]),
        "min_description_length": int(raw["min_description_length"]),
    }


def load_api_config() -> dict:
    """API 핸들러 설정을 로드한다."""
    raw = load_yaml("api.yaml")

    return {
        "max_query_length": int(raw["max_query_length"]),
        "max_company_length": int(raw["max_company_length"]),
        "max_keywords": int(raw["max_keywords"]),
        "blog_crawl_delay": float(raw["blog_crawl_delay"]),
        "search_news_display": int(raw["search_news_display"]),
        "webkr_keyword_display": int(raw["webkr_keyword_display"]),
        "webkr_default_display": int(raw["webkr_default_display"]),
    }
