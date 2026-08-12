"""OCR 엔진 비교 테스트 — 동일 채용공고 이미지로 3가지 OCR 엔진 품질 비교.

사용법:
    GCP_SA_KEY_B64=$(base64 < /path/to/service-account.json) \
    python scripts/compare_ocr_engines.py

테스트 대상: 현대 오토에버 2026 3분기 신입사원 채용 (이미지 2장)
비교 엔진: PaddleOCR ONNX / Tesseract / Google Cloud Vision
"""
import base64
import os
import sys
import time
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

IMG_DIR = Path("/tmp/ocr_test_images")
# 중복 제거: 1번(=3번), 2번(=4번) 이미지만 사용
IMAGE_FILES = ["hyundai_autoever_1.png", "hyundai_autoever_2.png"]


def load_images() -> list[tuple[str, bytes]]:
    images = []
    for name in IMAGE_FILES:
        path = IMG_DIR / name
        images.append((name, path.read_bytes()))
    return images


def test_cloud_vision(images: list[tuple[str, bytes]]) -> str:
    """Google Cloud Vision API TEXT_DETECTION."""
    from ocr_client import call_ocr

    images_b64 = [base64.b64encode(data).decode() for _, data in images]
    return call_ocr(images_b64)


def test_paddleocr_onnx(images: list[tuple[str, bytes]]) -> str:
    """PaddleOCR ONNX (rapidocr-onnxruntime) — Lambda 2차 시도에서 사용한 방식."""
    from rapidocr_onnxruntime import RapidOCR
    import numpy as np
    from PIL import Image
    import io

    ocr = RapidOCR()
    all_texts = []

    for name, data in images:
        img = Image.open(io.BytesIO(data))
        img_np = np.array(img)
        result, _ = ocr(img_np)
        lines = []
        if result:
            for line in result:
                lines.append(line[1])
        all_texts.append("\n".join(lines))

    return "\n".join(all_texts)


def test_tesseract(images: list[tuple[str, bytes]]) -> str:
    """Tesseract OCR 한국어 (전처리 적용)."""
    import pytesseract
    from PIL import Image, ImageEnhance, ImageFilter
    import io

    all_texts = []

    for name, data in images:
        img = Image.open(io.BytesIO(data))
        # 전처리: 그레이스케일 → 대비 강화 → 이진화
        img = img.convert("L")
        img = ImageEnhance.Contrast(img).enhance(2.0)
        img = img.point(lambda x: 0 if x < 128 else 255)
        # 2배 확대
        img = img.resize((img.width * 2, img.height * 2), Image.LANCZOS)

        text = pytesseract.image_to_string(img, lang="kor", config="--psm 6")
        all_texts.append(text)

    return "\n".join(all_texts)


def print_result(engine_name: str, text: str, elapsed: float):
    print(f"\n{'=' * 70}")
    print(f"  {engine_name}")
    print(f"  추출 글자 수: {len(text)}자 | 소요 시간: {elapsed:.1f}초")
    print(f"{'=' * 70}")
    # 앞 1500자만 출력
    preview = text[:1500]
    print(preview)
    if len(text) > 1500:
        print(f"\n... (이하 {len(text) - 1500}자 생략)")
    print()


def main():
    print("=" * 70)
    print("  현대 오토에버 채용공고 — OCR 엔진 비교 테스트")
    print("  이미지: 2장 (채용 포스터 + 상세 모집내용)")
    print("=" * 70)

    images = load_images()
    print(f"\n이미지 로드 완료: {len(images)}장")
    for name, data in images:
        print(f"  {name} ({len(data):,} bytes)")

    # 1. PaddleOCR ONNX
    print("\n[1/3] PaddleOCR ONNX 테스트 중...")
    t0 = time.time()
    try:
        paddle_text = test_paddleocr_onnx(images)
        paddle_time = time.time() - t0
        print_result("PaddleOCR ONNX (rapidocr-onnxruntime)", paddle_text, paddle_time)
    except Exception as e:
        print(f"  PaddleOCR ONNX 실패: {e}\n")
        paddle_text = ""
        paddle_time = 0

    # 2. Tesseract
    print("[2/3] Tesseract OCR 테스트 중...")
    t0 = time.time()
    try:
        tess_text = test_tesseract(images)
        tess_time = time.time() - t0
        print_result("Tesseract OCR (한국어, 전처리 적용)", tess_text, tess_time)
    except Exception as e:
        print(f"  Tesseract 실패: {e}\n")
        tess_text = ""
        tess_time = 0

    # 3. Cloud Vision
    if os.environ.get("GCP_SA_KEY_B64"):
        print("[3/3] Google Cloud Vision 테스트 중...")
        t0 = time.time()
        try:
            vision_text = test_cloud_vision(images)
            vision_time = time.time() - t0
            print_result("Google Cloud Vision API", vision_text, vision_time)
        except Exception as e:
            print(f"  Cloud Vision 실패: {e}\n")
            vision_text = ""
            vision_time = 0
    else:
        print("[3/3] Cloud Vision 스킵 (GCP_SA_KEY_B64 미설정)")
        vision_text = ""
        vision_time = 0

    # 요약
    print("\n" + "=" * 70)
    print("  비교 요약")
    print("=" * 70)
    print(f"  {'엔진':<30} {'추출 글자 수':>12} {'소요 시간':>10}")
    print(f"  {'-'*30} {'-'*12} {'-'*10}")
    print(f"  {'PaddleOCR ONNX':<30} {len(paddle_text):>10}자 {paddle_time:>8.1f}초")
    print(f"  {'Tesseract (전처리 적용)':<30} {len(tess_text):>10}자 {tess_time:>8.1f}초")
    print(f"  {'Google Cloud Vision':<30} {len(vision_text):>10}자 {vision_time:>8.1f}초")
    print()


if __name__ == "__main__":
    main()
