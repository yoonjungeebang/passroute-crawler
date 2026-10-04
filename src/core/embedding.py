"""ONNX 양자화 임베딩 모듈. Lambda 에서 JD 임베딩을 계산하고 S3 에 저장할 때 사용."""
import logging
import os

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "snunlp/KR-SBERT-V40K-klueNLI-augSTS"
_MODEL_DIR = os.environ.get("EMBEDDING_MODEL_DIR", "/app/models")
_ONNX_MODEL_PATH = os.path.join(_MODEL_DIR, "kr-sbert-uint8.onnx")
_TOKENIZER_PATH = os.path.join(_MODEL_DIR, "tokenizer")
_MAX_CHUNK_TOKENS = 510  # 512 - [CLS] - [SEP]

_session = None
_tokenizer = None
_input_names = None
_np = None


def _load_model():
    """ONNX 모델과 토크나이저를 1회 로딩. Lambda 웜 스타트 시 재사용."""
    global _session, _tokenizer, _input_names, _np

    if _session is not None:
        return

    import numpy as np
    import onnxruntime as ort
    from transformers import AutoTokenizer

    _np = np
    _tokenizer = AutoTokenizer.from_pretrained(_TOKENIZER_PATH)
    _session = ort.InferenceSession(
        _ONNX_MODEL_PATH, providers=["CPUExecutionProvider"],
    )
    _input_names = {inp.name for inp in _session.get_inputs()}
    logger.info("ONNX 임베딩 로드 완료: %s", _ONNX_MODEL_PATH)


def _chunk_by_paragraphs(text: str) -> list[str]:
    """문단 단위로 텍스트를 분할. 각 chunk 가 _MAX_CHUNK_TOKENS 를 넘지 않도록 그룹핑."""
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    if not paragraphs:
        return [text or " "]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_tokens = 0

    for para in paragraphs:
        para_tokens = len(_tokenizer.encode(para, add_special_tokens=False))

        if para_tokens > _MAX_CHUNK_TOKENS:
            if current_lines:
                chunks.append("\n".join(current_lines))
                current_lines = []
                current_tokens = 0
            chunks.append(para)
            continue

        if current_tokens + para_tokens > _MAX_CHUNK_TOKENS and current_lines:
            chunks.append("\n".join(current_lines))
            current_lines = []
            current_tokens = 0

        current_lines.append(para)
        current_tokens += para_tokens

    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks or [text or " "]


_INFERENCE_BATCH_SIZE = 8


def _infer_batch(chunks: list[str]) -> tuple:
    """chunk 리스트를 토크나이즈 + ONNX 추론하여 (chunk_embeddings, token_counts) 반환."""
    np = _np
    encoded = _tokenizer(
        chunks, return_tensors="np", padding=True,
        truncation=True, max_length=512,
    )

    ort_inputs = {
        name: encoded[name]
        for name in _input_names
        if name in encoded
    }
    outputs = _session.run(None, ort_inputs)
    token_embeddings = outputs[0]

    attention_mask = encoded["attention_mask"]
    mask_expanded = np.expand_dims(attention_mask, axis=-1)
    sum_embeddings = np.sum(token_embeddings * mask_expanded, axis=1)
    sum_mask = np.clip(np.sum(mask_expanded, axis=1), a_min=1e-9, a_max=None)
    chunk_embeddings = sum_embeddings / sum_mask

    token_counts = np.sum(attention_mask, axis=1)
    return chunk_embeddings, token_counts


def embed_text(text: str) -> list[float]:
    """텍스트를 문단 청킹 + 미니배치 추론 + 토큰 수 가중 평균 임베딩으로 변환."""
    _load_model()
    np = _np

    chunks = _chunk_by_paragraphs(text)

    all_chunk_embeddings: list = []
    all_token_counts: list = []

    for i in range(0, len(chunks), _INFERENCE_BATCH_SIZE):
        batch = chunks[i:i + _INFERENCE_BATCH_SIZE]
        chunk_embs, tok_counts = _infer_batch(batch)
        all_chunk_embeddings.append(chunk_embs)
        all_token_counts.append(tok_counts)

    chunk_embeddings = np.concatenate(all_chunk_embeddings, axis=0)
    token_counts = np.concatenate(all_token_counts, axis=0)

    weights = token_counts / np.sum(token_counts)
    weighted_embedding = np.sum(
        chunk_embeddings * weights[:, np.newaxis], axis=0,
    )

    norm = max(float(np.linalg.norm(weighted_embedding)), 1e-9)
    return (weighted_embedding / norm).tolist()


def build_document(raw_text: str, tech_stack: tuple[str, ...]) -> str:
    """raw_text + tech_stack 을 임베딩 대상 document 문자열로 조합."""
    parts: list[str] = []
    if raw_text:
        parts.append(raw_text)
    if tech_stack:
        parts.append(f"[기술스택]\n{', '.join(tech_stack)}")
    return "\n\n".join(parts)
