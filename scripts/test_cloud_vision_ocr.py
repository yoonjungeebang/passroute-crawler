"""Cloud Vision OCR 로컬 테스트 — 잡코리아 이미지 JD를 크롤링하여 OCR 결과를 확인한다.

사용법:
    GCP_SA_KEY_B64=$(base64 < /path/to/service-account.json) \
    python scripts/test_cloud_vision_ocr.py [max_scan]

    max_scan: 이미지 JD를 찾기 위해 탐색할 최대 공고 수 (기본 40)
"""
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

import base64
import json
import logging
import os

import requests

from crawler.base import ImageJobDetail
from crawler.jobkorea import JobKoreaCrawler
from ocr_client import call_ocr
from parser.jobkorea import remove_noise_sections

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    if not os.environ.get("GCP_SA_KEY_B64"):
        print("GCP_SA_KEY_B64 환경변수가 필요합니다.")
        print("실행 예: GCP_SA_KEY_B64=$(base64 < service-account.json) python scripts/test_cloud_vision_ocr.py")
        return 1

    max_scan = int(sys.argv[1]) if len(sys.argv) >= 2 else 40

    crawler = JobKoreaCrawler()

    print("\n" + "=" * 70)
    print("  Cloud Vision OCR 로컬 테스트")
    print("=" * 70)

    print("\n[1단계] 잡코리아 목록 페이지 크롤링...")
    refs = crawler.fetch_listings_page(1)
    print(f"  목록 {len(refs)}건 조회 완료\n")

    image_jd_count = 0
    text_jd_count = 0
    skip_count = 0

    for i, ref in enumerate(refs[:max_scan]):
        print(f"[{i+1}/{min(max_scan, len(refs))}] {ref.company_name} | {ref.title}")
        try:
            detail = crawler.fetch_detail(ref)
        except Exception as e:
            print(f"  → 상세 크롤링 실패: {e}")
            skip_count += 1
            continue

        if detail is None:
            print(f"  → 스킵 (본문 없음)")
            skip_count += 1
            continue

        if not isinstance(detail, ImageJobDetail):
            text_jd_count += 1
            print(f"  → 텍스트 JD ({len(detail.raw_text)}자)")
            continue

        # 이미지 JD 발견
        image_jd_count += 1
        print(f"  → 이미지 JD 발견! (이미지 {len(detail.images_b64)}장)")
        print(f"  → Cloud Vision API 호출 중...")

        try:
            ocr_text = call_ocr(list(detail.images_b64))
            cleaned = remove_noise_sections(ocr_text)
        except Exception as e:
            print(f"  → OCR 실패: {e}")
            continue

        print(f"\n{'=' * 70}")
        print(f"  기업: {detail.company_name}")
        print(f"  제목: {detail.title}")
        print(f"  URL:  {detail.url}")
        print(f"  이미지 수: {len(detail.images_b64)}장")
        print(f"  tech_stack: {', '.join(detail.tech_stack) if detail.tech_stack else '(없음)'}")
        print(f"{'=' * 70}")

        print(f"\n--- OCR 원본 텍스트 ({len(ocr_text)}자) ---")
        print(ocr_text[:3000])
        if len(ocr_text) > 3000:
            print(f"\n... (이하 {len(ocr_text) - 3000}자 생략)")

        print(f"\n--- 노이즈 제거 후 ({len(cleaned)}자) ---")
        print(cleaned[:3000])
        if len(cleaned) > 3000:
            print(f"\n... (이하 {len(cleaned) - 3000}자 생략)")
        print()

    # 요약
    print("\n" + "=" * 70)
    print("  테스트 결과 요약")
    print("=" * 70)
    print(f"  탐색 공고 수: {min(max_scan, len(refs))}건")
    print(f"  텍스트 JD:    {text_jd_count}건")
    print(f"  이미지 JD:    {image_jd_count}건")
    print(f"  스킵:         {skip_count}건")
    print(f"  이미지 JD 비율: {image_jd_count / max(1, text_jd_count + image_jd_count) * 100:.1f}%")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
