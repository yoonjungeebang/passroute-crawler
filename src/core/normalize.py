"""텍스트 정규화 유틸리티."""

import hashlib
import html
import re
import unicodedata

_CORP_SUFFIXES = re.compile(
    r"(?:주식회사|\(주\)|㈜|co\.,?\s*ltd\.?|inc\.?|corp\.?|ltd\.?|llc\.?)",
    re.IGNORECASE,
)


def make_external_id(url: str) -> str:
    """URL을 해시해서 고정 길이(16자) ID를 만든다.

    같은 URL이면 항상 같은 ID가 나오므로, 중복 크롤링을 방지할 수 있다.
    뉴스/기술 블로그처럼 사이트가 고유 ID를 제공하지 않을 때 사용한다.
    """
    return hashlib.sha256(url.encode()).hexdigest()[:16]


_RE_HTML_TAG = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    """HTML이 섞인 텍스트를 깨끗한 일반 텍스트로 변환한다.

    처리 순서:
      1. HTML 태그 제거: <b>, <div> 등 삭제
      2. HTML 엔티티 디코딩: &amp; → &, &lt; → < 등 원래 문자로 복원
      3. 유니코드 정규화(NFKC): 눈에 보이지 않는 특수 공백(\\xa0 등)을 일반 공백으로 통일
    """
    cleaned = _RE_HTML_TAG.sub("", text)
    cleaned = html.unescape(cleaned)
    cleaned = unicodedata.normalize("NFKC", cleaned)
    return cleaned.strip()


def normalize_company_name(name: str) -> str:
    """회사명을 정규화한다.

    1. NFKC 유니코드 정규화 (전각 -> 반각 등)
    2. 소문자 변환
    3. 법인 표기 제거: (주), 주식회사, Inc., Corp., Ltd., Co., Ltd., LLC
    4. 한글/영문/숫자만 남기고 나머지 제거
    """
    s = unicodedata.normalize("NFKC", name)
    s = s.lower()
    s = _CORP_SUFFIXES.sub("", s)
    s = re.sub(r"[^a-z0-9가-힣]", "", s)
    return s


def normalize_title(title: str) -> str:
    """공고 제목을 정규화한다.

    1. NFKC 유니코드 정규화
    2. 소문자 변환
    3. 한글/영문/숫자만 남기고 나머지 제거
    """
    s = unicodedata.normalize("NFKC", title)
    s = s.lower()
    s = re.sub(r"[^a-z0-9가-힣]", "", s)
    return s
