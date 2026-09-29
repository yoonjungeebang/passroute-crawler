"""로컬 스모크 테스트 — 잡코리아 크롤링 단독 실행.

PostgreSQL / SQS / Lambda 없이 ``JobKoreaCrawler`` 만 직접 호출하여
목록 1페이지 + 첫 공고 상세를 사람 눈으로 확인한다.

사용 예:
    .venv/bin/python scripts/local_smoke.py                  # 1페이지 목록 + 첫 1건 상세
    .venv/bin/python scripts/local_smoke.py 3                # 3페이지까지 목록
    .venv/bin/python scripts/local_smoke.py 1 5              # 1페이지 + 앞 5건 상세

요청 간 1~2.5초 딜레이가 끼므로 5건 상세 = 약 10~15초 소요.
"""
import logging
import sys
from pathlib import Path

# src/ 를 sys.path 에 추가 (Lambda 런타임과 동일한 import 경로)
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

from crawler.jobkorea import JobKoreaCrawler  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main() -> int:
    page = int(sys.argv[1]) if len(sys.argv) >= 2 else 1
    detail_limit = int(sys.argv[2]) if len(sys.argv) >= 3 else 1

    crawler = JobKoreaCrawler()

    print(f"\n=== 목록 조회: 20개 카테고리 통합, page={page} ===")
    refs = crawler.fetch_listings_page(page)
    print(f"총 {len(refs)}건")
    for ref in refs[:10]:
        print(f"  - [{ref.external_id}] {ref.company_name} | {ref.title} [{ref.career_level}]")
        print(f"    {ref.url}")

    if not refs:
        print("목록이 비어 있어 상세 조회를 건너뜁니다.")
        return 0

    print(f"\n=== 상세 조회 (앞 {detail_limit}건) ===")
    skipped = 0
    saved = 0
    for ref in refs[:detail_limit]:
        print(f"\n--- [{ref.external_id}] {ref.company_name} | {ref.title} ---")
        try:
            detail = crawler.fetch_detail(ref)
        except Exception as e:
            print(f"[실패] {type(e).__name__}: {e}")
            continue

        if detail is None:
            print("[스킵] 이미지 JD")
            skipped += 1
            continue

        saved += 1
        print(f"  company  : {detail.company_name}")
        print(f"  title    : {detail.title}")
        print(f"  deadline : {detail.deadline}")
        print(f"  tech     : {detail.tech_stack}")
        print(f"  raw_text : {_preview(detail.raw_text)}")

    print(f"\n=== 요약: 저장 가능 {saved}건 / 스킵 {skipped}건 ===")
    return 0


def _preview(text: str, limit: int = 200) -> str:
    if not text:
        return "(없음)"
    text = text.replace("\n", " ⏎ ")
    return text[:limit] + ("…" if len(text) > limit else "")


if __name__ == "__main__":
    sys.exit(main())
