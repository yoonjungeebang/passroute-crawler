"""기술 블로그 본문 추출 모듈.

도메인 → RSS 피드 URL 매핑과, RSS/페이지 크롤링을 통한 본문 추출 기능을 제공한다.
면접 방 생성 시 실시간 수집 파이프라인(api.company_collect)에서 사용된다.
"""
import logging
from dataclasses import dataclass

import feedparser
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_tech_blog_config
from parser.common import strip_html

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
        # urlparse: URL을 구성 요소로 분해하는 함수
        # urlparse("https://d2.naver.com/d2.atom").netloc → "d2.naver.com"
        # netloc: URL의 도메인 부분
        from urllib.parse import urlparse  # noqa: C0415
        domain = urlparse(feed.feed_url).netloc
        # medium.com은 피드 URL 자체를 키로 (여러 기업이 같은 도메인)
        if domain == "medium.com":
            mapping[feed.feed_url] = feed.feed_url
        else:
            mapping[domain] = feed.feed_url
    return mapping


# 모듈 로딩 시점에 한 번 실행되어 딕셔너리를 만들어 둔다.
_DOMAIN_TO_FEED: dict[str, str] = _build_domain_to_feed_map()


def find_rss_feed_url(article_url: str) -> str | None:
    """글 URL의 도메인으로 RSS 피드 URL을 찾는다. 없으면 None.

    반환 타입 str | None: 문자열을 반환하거나 None을 반환할 수 있다는 뜻.
    """
    from urllib.parse import urlparse  # noqa: C0415
    domain = urlparse(article_url).netloc

    # 딕셔너리의 in 연산자: 키가 존재하는지 확인
    if domain in _DOMAIN_TO_FEED:
        return _DOMAIN_TO_FEED[domain]

    # medium.com 글은 경로에서 피드 URL 추측
    if domain == "medium.com":
        # .path: URL의 경로 부분. "https://medium.com/daangn/post" → "/daangn/post"
        path = urlparse(article_url).path
        # strip("/"): 앞뒤의 "/" 제거. "/daangn/post" → "daangn/post"
        # split("/"): "/"로 나눈다. "daangn/post" → ["daangn", "post"]
        parts = path.strip("/").split("/")
        if parts:  # 리스트가 비어있지 않으면
            candidate = f"https://medium.com/feed/{parts[0]}"  # parts[0]: 리스트의 첫 번째 요소
            if candidate in _DOMAIN_TO_FEED:
                return candidate

    return None  # 아무것도 못 찾으면 None 반환


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
        # URL 매칭 (쿼리 파라미터 무시, 후행 슬래시 무시)
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
    """RSS 엔트리에서 최대한 풍부한 콘텐츠를 추출한다.

    우선순위: content:encoded (본문 전체) > summary/description
    """
    # hasattr(객체, 속성이름): 객체에 해당 속성이 존재하는지 확인. True/False 반환.
    # feedparser 는 content:encoded 를 entry.content 리스트에 넣는다
    if hasattr(entry, "content") and entry.content:
        for c in entry.content:
            text = strip_html(c.get("value", ""))
            if len(text) >= _MIN_CONTENT_LENGTH:
                return text

    # A or B or C: 왼쪽부터 평가해서 True스러운 첫 번째 값을 반환.
    # summary가 없으면 description을, 그것도 없으면 빈 문자열을 사용.
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
    # trafilatura: 웹 페이지에서 본문 텍스트를 자동 추출하는 라이브러리.
    # 광고, 네비게이션 등을 제거하고 본문만 깔끔하게 추출해 준다.
    import trafilatura  # noqa: C0415

    try:
        resp = session.get(url, timeout=_PAGE_TIMEOUT)
        resp.raise_for_status()
    except Exception:
        logger.exception("페이지 요청 실패: %s", url)
        return ""  # 빈 문자열 반환 = 실패

    # apparent_encoding: 응답 본문을 분석해서 추측한 인코딩 (한글 깨짐 방지)
    resp.encoding = resp.apparent_encoding

    # 1차 시도: trafilatura 로 본문 추출
    text = trafilatura.extract(resp.text)  # resp.text: 응답 본문을 문자열로
    if text and len(text) >= _MIN_CONTENT_LENGTH:
        return text

    # 2차 시도: BeautifulSoup 으로 HTML 파싱 후 특정 태그에서 추출
    # "html.parser": 파이썬 내장 HTML 파서 사용
    soup = BeautifulSoup(resp.text, "html.parser")
    # CSS 선택자로 요소 찾기:
    #   "article": <article> 태그
    #   "[class*=content]": class 속성에 "content"가 포함된 요소
    for selector in ["article", "[class*=content]", "[class*=post]", "main"]:
        # select_one(): CSS 선택자에 매칭되는 첫 번째 요소를 반환. 없으면 None.
        el = soup.select_one(selector)
        # get_text(): HTML 태그를 제거하고 텍스트만 추출
        #   strip=True: 각 텍스트 조각의 앞뒤 공백 제거
        #   separator="\n": 태그 사이에 줄바꿈을 넣어 구분
        if el and len(el.get_text(strip=True)) >= _MIN_CONTENT_LENGTH:
            return el.get_text(separator="\n", strip=True)

    return ""
