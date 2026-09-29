"""이미지 JD의 OCR 처리 후 실제 저장되는 데이터 형태를 보여준다.

흐름: 이미지 JD → Tesseract OCR → remove_noise_sections → JobDetail → S3 JSON
"""
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

import base64
import io
import json
import logging

import pytesseract
from PIL import Image, ImageOps

from crawler.base import ImageJobDetail
from crawler.jobkorea import JobKoreaCrawler
from parser.jobkorea import remove_noise_sections

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def preprocess(pil_img: Image.Image) -> Image.Image:
    img = pil_img.convert("L")
    img = ImageOps.autocontrast(img, cutoff=5)
    img = img.point(lambda x: 255 if x > 180 else 0)
    img = img.resize((img.width * 2, img.height * 2), Image.LANCZOS)
    return img


def ocr_image(img_b64: str) -> str:
    raw = base64.b64decode(img_b64)
    pil_img = Image.open(io.BytesIO(raw)).convert("RGB")
    preprocessed = preprocess(pil_img)
    return pytesseract.image_to_string(preprocessed, lang="kor+eng", config="--psm 6 --oem 1")


def main() -> int:
    max_scan = int(sys.argv[1]) if len(sys.argv) >= 2 else 30
    crawler = JobKoreaCrawler()

    print("=== 이미지 JD 탐색 중 ===")
    refs = crawler.fetch_listings_page(1)
    print(f"목록 {len(refs)}건\n")

    for i, ref in enumerate(refs[:max_scan]):
        print(f"[{i+1}] {ref.company_name} | {ref.title}")
        try:
            detail = crawler.fetch_detail(ref)
        except Exception as e:
            print(f"  -> 실패: {e}\n")
            continue

        if not isinstance(detail, ImageJobDetail):
            print(f"  -> 텍스트 JD (스킵)\n")
            continue

        print(f"  -> 이미지 JD ({len(detail.images_b64)}장) — OCR 처리 중...")

        # Step 1: OCR (Tesseract)
        texts = []
        for img_b64 in detail.images_b64:
            text = ocr_image(img_b64)
            if text.strip():
                texts.append(text.strip())
        ocr_raw = "\n".join(texts)

        # Step 2: 노이즈 제거
        cleaned = remove_noise_sections(ocr_raw)

        # Step 3: 실제 S3에 저장되는 JSON 형태
        stored_data = {
            "source": detail.source,
            "external_id": detail.external_id,
            "url": detail.url,
            "company_name": detail.company_name,
            "title": detail.title,
            "raw_text": cleaned,
            "tech_stack": list(detail.tech_stack),
            "deadline": detail.deadline,
            "crawled_at": detail.crawled_at,
            "embedding": "[768차원 벡터 - 생략]",
        }

        print(f"\n{'='*60}")
        print("S3에 저장되는 JSON (parsed/jobkorea/{external_id}.json)")
        print(f"{'='*60}")
        print(json.dumps(stored_data, ensure_ascii=False, indent=2))

        print(f"\n{'='*60}")
        print("PostgreSQL에 저장되는 document (임베딩 대상 텍스트)")
        print(f"{'='*60}")
        tech_section = "\n\n[기술스택]\n" + ", ".join(detail.tech_stack) if detail.tech_stack else ""
        document = cleaned + tech_section
        print(document[:2000])
        if len(document) > 2000:
            print(f"\n... (이하 {len(document) - 2000}자 생략)")

        print(f"\n{'='*60}")
        print("비교: OCR 원본 vs 노이즈 제거 후")
        print(f"{'='*60}")
        print(f"OCR 원본: {len(ocr_raw)}자")
        print(f"노이즈 제거 후: {len(cleaned)}자 ({len(cleaned)/max(len(ocr_raw),1)*100:.0f}%)")

        break

    return 0


if __name__ == "__main__":
    sys.exit(main())
