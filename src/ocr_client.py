"""Google Cloud Vision OCR 모듈. Lambda 에서 이미지 JD 텍스트를 추출한다."""
import base64
import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

VISION_API_URL = "https://vision.googleapis.com/v1/images:annotate"
_SCOPES = ["https://www.googleapis.com/auth/cloud-vision"]

_credentials = None


def _get_credentials():
    """GCP 서비스 계정 인증 정보를 로드하고 토큰을 갱신한다."""
    global _credentials

    from google.auth.transport.requests import Request
    from google.oauth2 import service_account

    if _credentials is None:
        sa_key_b64 = os.environ.get("GCP_SA_KEY_B64", "")
        sa_info = json.loads(base64.b64decode(sa_key_b64))
        _credentials = service_account.Credentials.from_service_account_info(
            sa_info, scopes=_SCOPES,
        )
        logger.info("GCP 서비스 계정 로드 완료: %s", sa_info.get("client_email"))

    if not _credentials.valid:
        _credentials.refresh(Request())

    return _credentials


def call_ocr(images_b64: list[str]) -> str:
    """
    이미지 목록을 Cloud Vision API 로 텍스트 추출.
    각 이미지의 OCR 결과를 줄바꿈으로 합산하여 반환한다.
    """
    credentials = _get_credentials()

    image_requests = [
        {
            "image": {"content": img_b64},
            "features": [{"type": "TEXT_DETECTION"}],
            "imageContext": {"languageHints": ["ko"]},
        }
        for img_b64 in images_b64
    ]

    resp = requests.post(
        VISION_API_URL,
        json={"requests": image_requests},
        headers={"Authorization": f"Bearer {credentials.token}"},
        timeout=30,
    )
    resp.raise_for_status()

    texts: list[str] = []
    for i, annotation in enumerate(resp.json().get("responses", [])):
        error = annotation.get("error")
        if error:
            logger.warning(
                "OCR 이미지 %d/%d 실패: code=%s %s",
                i + 1, len(images_b64), error.get("code"), error.get("message"),
            )
            continue

        full_text = annotation.get("fullTextAnnotation", {}).get("text", "")
        texts.append(full_text)
        logger.info(
            "OCR 이미지 %d/%d 완료: %d자", i + 1, len(images_b64), len(full_text),
        )

    return "\n".join(t for t in texts if t)
