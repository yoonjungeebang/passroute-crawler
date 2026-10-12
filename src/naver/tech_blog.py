"""기술 블로그 본문 추출 모듈.

도메인 → RSS 피드 URL 매핑과, RSS/페이지 크롤링을 통한 본문 추출 기능을 제공한다.
면접 방 생성 시 실시간 수집 파이프라인에서 사용된다.
"""
import logging
from dataclasses import dataclass

import feedparser
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_tech_blog_config
from core.normalize import strip_html

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BlogFeed:
    """RSS/Atom 피드 1개."""
    company_name: str
    feed_url: str


# ── 설정 파일에서 로드 ──

_BLOG_CONFIG = load_tech_blog_config()
_MIN_CONTENT_LENGTH: int = _BLOG_CONFIG["min_content_length"]
_PAGE_TIMEOUT: int = _BLOG_CONFIG["page_timeout"]
_DEFAULT_FEEDS: tuple[BlogFeed, ...] = tuple(
    BlogFeed(f["company_name"], f["feed_url"])
    for f in _BLOG_CONFIG["feeds"]
)

def _build_domain_to_feed_map() -> dict[str, str]:
    """도메인 → RSS 피드 URL 매핑을 생성한다."""
    mapping: dict[str, str] = {}
    for feed in _DEFAULT_FEEDS:
        from urllib.parse import urlparse  # noqa: C0415
        domain = urlparse(feed.feed_url).netloc
        # medium.com은 여러 기업이 같은 도메인을 사용하므로 피드 URL 자체를 키로 사용
        if domain == "medium.com":
            mapping[feed.feed_url] = feed.feed_url
        else:
            mapping[domain] = feed.feed_url
    return mapping


_DOMAIN_TO_FEED: dict[str, str] = _build_domain_to_feed_map()


def find_rss_feed_url(article_url: str) -> str | None:
    """글 URL의 도메인으로 RSS 피드 URL을 찾는다. 없으면 None."""
    from urllib.parse import urlparse  # noqa: C0415
    domain = urlparse(article_url).netloc

    if domain in _DOMAIN_TO_FEED:
        return _DOMAIN_TO_FEED[domain]

    # medium.com 글은 경로에서 피드 URL 추측
    if domain == "medium.com":
        path = urlparse(article_url).path
        parts = path.strip("/").split("/")
        if parts:
            candidate = f"https://medium.com/feed/{parts[0]}"
            if candidate in _DOMAIN_TO_FEED:
                return candidate

    return None


def fetch_content_from_rss(feed_url: str, article_url: str) -> str:
    """RSS 피드에서 특정 글의 본문을 찾아 반환한다. 없으면 빈 문자열."""
    try:
        parsed = feedparser.parse(
            feed_url,
            agent="Mozilla/5.0 (compatible; passroute-bot/1.0)",
        )
    except Exception:
        logger.exception("RSS 피드 파싱 에러: %s", feed_url)
        return ""

    for entry in parsed.entries:
        entry_url = entry.get("link", "")
        if entry_url.rstrip("/") == article_url.rstrip("/"):
            content = _extract_rss_content(entry)
            if content and len(content) >= _MIN_CONTENT_LENGTH:
                return content

    logger.info("RSS 피드에 해당 글 없음: feed=%s, url=%s", feed_url, article_url)
    return ""


_HTTP_HEADERS = {
    "User-Agent": (
        "passroute-bot/1.0 "
        "(+https://github.com/yoonjungeebang/passroute-crawler; yezanee@gmail.com)"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "identity",
}


def _extract_rss_content(entry: dict) -> str:
    """RSS 엔트리에서 콘텐츠를 추출한다.

    우선순위: content:encoded (본문 전체) > summary/description
    """
    if hasattr(entry, "content") and entry.content:
        for c in entry.content:
            text = strip_html(c.get("value", ""))
            if len(text) >= _MIN_CONTENT_LENGTH:
                return text

    summary_raw = entry.get("summary") or entry.get("description") or ""
    return strip_html(summary_raw)


def _make_session() -> requests.Session:
    """기술 블로그 수집용 세션 생성: 재시도 정책 + 공통 헤더."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update(_HTTP_HEADERS)
    return session


def _fetch_page_content(session: requests.Session, url: str) -> str:
    """URL 에서 본문 텍스트를 추출한다. trafilatura → BeautifulSoup 순으로 시도."""
    import trafilatura  # noqa: C0415

    try:
        resp = session.get(url, timeout=_PAGE_TIMEOUT)
        resp.raise_for_status()
    except Exception:
        logger.exception("페이지 요청 실패: %s", url)
        return ""

    resp.encoding = resp.apparent_encoding

    # 1차: trafilatura로 본문 추출
    text = trafilatura.extract(resp.text)
    if text and len(text) >= _MIN_CONTENT_LENGTH:
        return text

    # 2차: BeautifulSoup으로 HTML 파싱 후 특정 태그에서 추출
    soup = BeautifulSoup(resp.text, "html.parser")
    for selector in ["article", "[class*=content]", "[class*=post]", "main"]:
        el = soup.select_one(selector)
        if el and len(el.get_text(strip=True)) >= _MIN_CONTENT_LENGTH:
            return el.get_text(separator="\n", strip=True)

    return ""
