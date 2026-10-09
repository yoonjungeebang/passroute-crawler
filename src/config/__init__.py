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
        "search_suffixes": tuple(raw["search_suffixes"]),
        "exclude_news_title_keywords": tuple(raw["exclude_news_title_keywords"]),
    }


def load_tech_blog_config() -> dict:
    """기술 블로그 설정을 로드한다."""
    raw = load_yaml("tech_blog.yaml")

    return {
        "min_content_length": int(raw["min_content_length"]),
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
