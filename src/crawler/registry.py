"""source 이름 → 크롤러 매핑. 새 크롤러는 CRAWLER_CLASSES 에 등록."""
from typing import Iterator

from crawler.base import JobCrawler
from crawler.jumpit import JumpitCrawler
from crawler.programmers import ProgrammersCrawler
from crawler.rallit import RallitCrawler
from crawler.superookie import SuperookieCrawler

CRAWLER_CLASSES: list[type[JobCrawler]] = [
    JumpitCrawler,
    ProgrammersCrawler,
    RallitCrawler,
    SuperookieCrawler,
]

_CRAWLER_BY_SOURCE: dict[str, type[JobCrawler]] = {
    cls.source: cls for cls in CRAWLER_CLASSES
}


def get_crawler(source: str) -> JobCrawler:
    cls = _CRAWLER_BY_SOURCE.get(source)
    if cls is None:
        raise ValueError(f"알 수 없는 크롤러 source: {source!r}")
    return cls()


def iter_sources() -> Iterator[str]:
    for cls in CRAWLER_CLASSES:
        yield cls.source
