"""주요 IT 기업 기술 블로그 수집 모듈.

22개 RSS/Atom 피드 + SK 데보션(HTML 크롤링)으로 기술 블로그 글을 수집한다.
RSS 요약이 잘린 경우 trafilatura → BeautifulSoup 순으로 본문을 직접 추출한다.
"""
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import feedparser
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core import KST
from parser.common import strip_html

logger = logging.getLogger(__name__)

BLOG_RETENTION_DAYS = 365
_MAX_FETCH_CONTENT = 5
_FEED_FILTER_DAYS = timedelta(days=365)

# ── 블로그 피드 설정 ──


@dataclass(frozen=True)
class BlogFeed:
    """RSS/Atom 피드 1개."""
    company_name: str
    feed_url: str


_DEFAULT_FEEDS: tuple[BlogFeed, ...] = (
    BlogFeed("네이버", "https://d2.naver.com/d2.atom"),
    BlogFeed("카카오", "https://tech.kakao.com/feed"),
    BlogFeed("카카오페이", "https://tech.kakaopay.com/rss.xml"),
    BlogFeed("카카오뱅크", "https://tech.kakaobank.com/index.xml"),
    BlogFeed("토스", "https://toss.tech/rss.xml"),
    BlogFeed("우아한형제들", "https://techblog.woowahan.com/feed"),
    BlogFeed("당근", "https://medium.com/feed/daangn"),
    BlogFeed("쿠팡", "https://medium.com/feed/coupang-engineering"),
    BlogFeed("라인", "https://techblog.lycorp.co.jp/ko/feed/index.xml"),
    BlogFeed("삼성전자", "https://techblog.samsung.com/rss"),
    BlogFeed("LG U+", "https://techblog.uplus.co.kr/feed"),
    BlogFeed("KT Cloud", "https://tech.ktcloud.com/feed"),
    BlogFeed("NHN", "https://meetup.nhncloud.com/rss"),
    BlogFeed("마켓컬리", "https://helloworld.kurly.com/rss.xml"),
    BlogFeed("올리브영", "https://oliveyoung.tech/rss.xml"),
    BlogFeed("무신사", "https://medium.com/feed/musinsa-tech"),
    BlogFeed("뱅크샐러드", "https://blog.banksalad.com/rss.xml"),
    BlogFeed("야놀자", "https://medium.com/feed/yanoljacloud-tech"),
    BlogFeed("쏘카", "https://tech.socarcorp.kr/feed.xml"),
    BlogFeed("넷마블", "https://netmarble.engineering/feed/"),
    BlogFeed("지마켓", "https://dev.gmarket.com/rss"),
    BlogFeed("11번가", "https://11st-tech.github.io/rss/"),
)

# ── SK 데보션 (HTML 크롤링) ──

_DEVOCEAN_LIST_URL = "https://devocean.sk.com/blog/index.do?p=BLOG"
_DEVOCEAN_DETAIL_URL = "https://devocean.sk.com/blog/techBoardDetail.do?ID={board_id}"
_DEVOCEAN_COMPANY = "SK"
_DEVOTEE_MARKER = "DEVOTEE 요약"

_HTTP_HEADERS = {
    "User-Agent": (
        "passroute-bot/1.0 "
        "(+https://github.com/yoonjungeebang/passroute-crawler; yezanee@gmail.com)"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "identity",
}

# ── 피드 요청 간격 (초) ──

_DEFAULT_REQUEST_DELAY = 0.5


@dataclass(frozen=True)
class BlogArticle:
    """블로그 글 1건."""
    company_name: str
    title: str
    summary: str
    url: str
    pub_date: datetime
    collected_at: str


def _parse_pub_date(entry: dict) -> datetime:
    """feedparser 엔트리에서 발행일을 추출한다."""
    published = entry.get("published") or entry.get("updated") or ""
    if not published:
        return datetime.now(KST)
    try:
        return parsedate_to_datetime(published)
    except (ValueError, TypeError):
        pass
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            from calendar import timegm
            return datetime.fromtimestamp(timegm(entry.published_parsed), tz=timezone.utc)
    except (ValueError, TypeError, OverflowError):
        pass
    return datetime.now(KST)


def _is_truncated(text: str) -> bool:
    """본문 앞부분만 잘려서 들어온 요약인지 판별한다."""
    return text.endswith("…") or text.endswith("...")


def _make_external_id(url: str) -> str:
    """URL 에서 결정적 external_id 를 생성."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _parse_devocean_date(date_str: str) -> datetime:
    """데보션 날짜 문자열(YY.MM.DD)을 datetime 으로 변환."""
    try:
        return datetime.strptime(date_str.strip(), "%y.%m.%d").replace(tzinfo=KST)
    except (ValueError, AttributeError):
        return datetime.now(KST)


# ── 직무 카테고리 키워드 매핑 ──

_JOB_CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "백엔드개발자": (
        "백엔드", "backend", "서버 개발", "Spring", "JPA", "Hibernate",
        "Django", "FastAPI", "NestJS", "gRPC", "REST API", "MSA",
        "마이크로서비스", "microservice", "트랜잭션", "transaction",
    ),
    "프론트엔드개발자": (
        "프론트엔드", "frontend", "React", "Vue", "Angular", "Next.js",
        "Nuxt", "웹뷰", "CSS", "컴포넌트", "UI ", "UX ",
        "디자인 시스템", "design system",
    ),
    "웹개발자": (
        "웹 개발", "web dev", "풀스택", "full-stack", "fullstack",
        "HTML", "웹 성능", "웹 최적화",
    ),
    "앱개발자": (
        "iOS", "Android", "Swift", "Kotlin", "Flutter", "React Native",
        "모바일", "mobile", "앱 개발",
    ),
    "데이터엔지니어": (
        "데이터 엔지니어", "data engineer", "Kafka", "Spark", "Airflow",
        "ETL", "데이터 파이프라인", "data pipeline", "Flink", "Hadoop",
        "데이터 웨어하우스", "StarRocks", "Presto", "Hive",
    ),
    "데이터사이언티스트": (
        "데이터 사이언", "data scien", "데이터 분석", "data analy",
        "A/B 테스트", "AB테스트", "추천 시스템", "recommendation",
        "통계", "statistic", "지표", "metric",
    ),
    "소프트웨어개발자": (
        "소프트웨어 개발", "software dev", "리팩토링", "refactor",
        "코드 리뷰", "code review", "아키텍처", "architecture",
        "모노레포", "monorepo", "레거시", "legacy",
    ),
    "게임개발자": (
        "게임 개발", "game dev", "Unity", "Unreal", "렌더링", "rendering",
        "게임 서버", "게임 클라이언트",
    ),
    "AI/ML엔지니어": (
        "AI", "ML", "딥러닝", "deep learning", "LLM", "GPT", "Claude",
        "모델 학습", "model training", "신경망", "neural",
        "transformer", "파인튜닝", "fine-tun", "임베딩", "embedding",
        "RAG", "벡터", "vector",
    ),
    "클라우드엔지니어": (
        "AWS", "GCP", "Azure", "Kubernetes", "k8s", "Docker",
        "클라우드", "cloud", "인프라", "infra", "DevOps", "데브옵스",
        "Terraform", "CI/CD", "SRE", "모니터링", "monitoring",
    ),
    "MLOps엔지니어": (
        "MLOps", "모델 서빙", "model serving", "SageMaker",
        "ML 파이프라인", "ML pipeline", "모델 배포", "model deploy",
        "ONNX", "TensorRT",
    ),
    "AI서비스개발자": (
        "AI 서비스", "AI 에이전트", "AI agent", "챗봇", "chatbot",
        "프롬프트", "prompt", "생성형 AI", "generative AI",
        "바이브코딩", "vibe coding", "AI 코딩", "Copilot",
    ),
}


def _classify_job_categories(text: str) -> list[str]:
    """텍스트에서 키워드를 찾아 관련 직무 카테고리를 반환한다."""
    text_lower = text.lower()
    categories: list[str] = []
    for category, keywords in _JOB_CATEGORY_KEYWORDS.items():
        for kw in keywords:
            kw_lower = kw.lower()
            if len(kw_lower) <= 3:
                if re.search(rf"\b{re.escape(kw_lower)}\b", text_lower):
                    categories.append(category)
                    break
            elif kw_lower in text_lower:
                categories.append(category)
                break
    return categories


def _deadline_from_pub_date(pub_date: datetime) -> int:
    """pub_date + 365일을 Unix timestamp 로 변환."""
    expiry = pub_date + timedelta(days=BLOG_RETENTION_DAYS)
    return int(expiry.timestamp())


def _has_meaningful_content(text: str) -> bool:
    """추출된 텍스트가 네비게이션 노이즈가 아닌 실제 본문인지 판별."""
    avg_line_len = len(text) / max(text.count("\n") + 1, 1)
    return avg_line_len > 30


def _make_session() -> requests.Session:
    """기술 블로그 수집용 세션 생성: 재시도 정책 + 공통 헤더."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update(_HTTP_HEADERS)
    return session


# 모듈 레벨 세션 (본문 추출용)
_session = _make_session()


def _fetch_page_content(url: str) -> str:
    """URL 에서 본문 텍스트를 추출한다. trafilatura → BeautifulSoup 순으로 시도."""
    import trafilatura  # noqa: C0415

    try:
        resp = _session.get(url, timeout=15)
        resp.raise_for_status()
    except Exception:
        logger.exception("페이지 요청 실패: %s", url)
        return ""

    resp.encoding = resp.apparent_encoding

    text = trafilatura.extract(resp.text)
    if text and len(text) > 200 and _has_meaningful_content(text):
        return text

    soup = BeautifulSoup(resp.text, "html.parser")

    # Nuxt/Next.js JSON 데이터에서 본문 추출 (카카오 등)
    for script in soup.find_all("script"):
        script_text = script.get_text()
        if '"content":' not in script_text or len(script_text) < 5000:
            continue
        arr_start = script_text.find('[["ShallowReactive"')
        if arr_start < 0:
            continue
        try:
            data = json.loads(script_text[arr_start:])
            content_idx_match = re.search(r'"content":\s*(\d+)', script_text)
            if content_idx_match:
                idx = int(content_idx_match.group(1))
                if isinstance(data, list) and len(data) > idx:
                    content = data[idx]
                    if isinstance(content, str) and len(content) > 100:
                        return strip_html(content)
        except (json.JSONDecodeError, IndexError, ValueError):
            pass

    # 일반 article/content 영역 추출
    for selector in ["article", "[class*=content]", "[class*=post]", "main"]:
        el = soup.select_one(selector)
        if el and len(el.get_text(strip=True)) > 200:
            return el.get_text(separator="\n", strip=True)

    return ""


class TechBlogCollector:
    """RSS/Atom 피드 + SK 데보션으로 기업 기술 블로그 글을 수집한다."""

    def __init__(
        self,
        *,
        feeds: tuple[BlogFeed, ...] | None = None,
        request_delay: float = _DEFAULT_REQUEST_DELAY,
    ):
        self.feeds = feeds or _DEFAULT_FEEDS
        self.request_delay = request_delay
        self.session = _make_session()

    def _fetch_feed(self, feed: BlogFeed) -> list[BlogArticle]:
        """한 피드의 글을 수집한다."""
        now_iso = datetime.now(KST).isoformat()
        now = datetime.now(KST)

        try:
            parsed = feedparser.parse(
                feed.feed_url,
                agent="Mozilla/5.0 (compatible; passroute-bot/1.0)",
            )
        except Exception:
            logger.exception("피드 파싱 실패: %s (%s)", feed.company_name, feed.feed_url)
            return []

        if parsed.bozo and not parsed.entries:
            logger.warning(
                "피드 오류 (항목 없음): %s (%s) — %s",
                feed.company_name, feed.feed_url, parsed.bozo_exception,
            )
            return []

        entries = parsed.entries

        # 20건 초과 피드는 최근 2년 이내 글만
        if len(entries) > 20:
            cutoff = now - _FEED_FILTER_DAYS
            entries = [
                e for e in entries
                if _parse_pub_date(e) >= cutoff
            ]

        articles: list[BlogArticle] = []
        fetch_count = 0

        for entry in entries:
            url = entry.get("link", "")
            if not url:
                continue

            title = strip_html(entry.get("title", ""))
            if not title:
                continue

            summary_raw = entry.get("summary") or entry.get("description") or ""
            summary = strip_html(summary_raw)

            needs_fetch = not summary or _is_truncated(summary)

            if needs_fetch and fetch_count < _MAX_FETCH_CONTENT:
                content = _fetch_page_content(url)
                if content:
                    summary = content
                else:
                    summary = ""
                fetch_count += 1
                time.sleep(self.request_delay)
            elif needs_fetch:
                summary = ""

            articles.append(BlogArticle(
                company_name=feed.company_name,
                title=title,
                summary=summary,
                url=url,
                pub_date=_parse_pub_date(entry),
                collected_at=now_iso,
            ))

        return articles

    def _fetch_devocean(self) -> list[BlogArticle]:
        """SK 데보션 블로그를 HTML 크롤링으로 수집한다."""
        now_iso = datetime.now(KST).isoformat()

        try:
            resp = self.session.get(_DEVOCEAN_LIST_URL, timeout=15)
            resp.raise_for_status()
        except Exception:
            logger.exception("데보션 목록 페이지 요청 실패")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        board_elements = soup.find_all(attrs={"data-board-id": True})

        seen_ids: set[str] = set()
        board_ids: list[str] = []
        titles: dict[str, str] = {}
        dates: dict[str, str] = {}

        for el in board_elements:
            bid = el.get("data-board-id")
            if bid in seen_ids:
                continue
            seen_ids.add(bid)
            board_ids.append(bid)

            card = el
            for _ in range(5):
                if card.parent:
                    card = card.parent
                if len(card.get_text(strip=True)) > 50:
                    break

            title_el = card.find(class_=re.compile(r"tit|title|subject"))
            titles[bid] = title_el.get_text(strip=True) if title_el else ""

            date_el = card.find(class_=re.compile(r"date|time"))
            dates[bid] = date_el.get_text(strip=True) if date_el else ""

        articles: list[BlogArticle] = []
        for bid in board_ids:
            title = titles.get(bid, "")
            if not title:
                continue

            detail_url = _DEVOCEAN_DETAIL_URL.format(board_id=bid)
            summary = self._fetch_devocean_summary(detail_url)
            if not summary:
                continue

            pub_date = _parse_devocean_date(dates.get(bid, ""))

            articles.append(BlogArticle(
                company_name=_DEVOCEAN_COMPANY,
                title=title,
                summary=summary,
                url=detail_url,
                pub_date=pub_date,
                collected_at=now_iso,
            ))

            time.sleep(self.request_delay)

        return articles

    def _fetch_devocean_summary(self, detail_url: str) -> str:
        """데보션 상세 페이지에서 DEVOTEE 요약을 추출한다."""
        try:
            resp = self.session.get(detail_url, timeout=15)
            resp.raise_for_status()
        except Exception:
            logger.exception("데보션 상세 페이지 요청 실패: %s", detail_url)
            return ""

        soup = BeautifulSoup(resp.text, "html.parser")
        view = soup.select_one(".sub-view-cont")
        if not view:
            return ""

        text = view.get_text(separator="\n", strip=True)
        if _DEVOTEE_MARKER not in text:
            return ""

        idx = text.index(_DEVOTEE_MARKER) + len(_DEVOTEE_MARKER)
        after = text[idx:].strip()

        lines: list[str] = []
        for line in after.split("\n"):
            stripped = line.strip()
            if not stripped:
                if lines:
                    break
                continue
            lines.append(stripped)

        return " ".join(lines)

    def collect_all(self) -> list[BlogArticle]:
        """전체 피드 + SK 데보션에서 블로그 글을 수집한다. URL 기준 전역 중복 제거."""
        all_articles: list[BlogArticle] = []
        global_seen_urls: set[str] = set()

        for feed in self.feeds:
            articles = self._fetch_feed(feed)
            for article in articles:
                if article.url in global_seen_urls:
                    continue
                global_seen_urls.add(article.url)
                all_articles.append(article)

            logger.info("feed=%s: %d건 수집", feed.company_name, len(articles))

            time.sleep(self.request_delay)

        devocean_articles = self._fetch_devocean()
        for article in devocean_articles:
            if article.url in global_seen_urls:
                continue
            global_seen_urls.add(article.url)
            all_articles.append(article)

        logger.info("feed=SK 데보션: %d건 수집", len(devocean_articles))

        logger.info(
            "전체 블로그 수집 완료: %d건 (RSS %d개 + SK 데보션)",
            len(all_articles), len(self.feeds),
        )
        return all_articles


def blog_article_to_detail_dict(article: BlogArticle) -> dict:
    """BlogArticle 을 S3 저장용 dict 로 변환. JobDetail 호환 형식."""
    content = f"{article.title}\n\n{article.summary}" if article.summary else article.title
    categories = _classify_job_categories(content)

    raw_text = content
    if categories:
        raw_text += f"\n\n[직무]\n{', '.join(categories)}"

    return {
        "source": "tech_blog",
        "external_id": _make_external_id(article.url),
        "url": article.url,
        "company_name": article.company_name,
        "title": article.title,
        "raw_text": raw_text,
        "tech_stack": [],
        "deadline": _deadline_from_pub_date(article.pub_date),
        "crawled_at": article.collected_at,
        "career_level": "",
    }
