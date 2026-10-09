"""크롤러 추상 클래스 + 페이지 순회 공통 로직."""

# ── 표준 라이브러리 import ──
# import 문은 다른 모듈(파일)의 코드를 가져올 때 사용한다.
import logging            # 로그 출력을 위한 표준 라이브러리
import os                 # 운영체제 환경변수 읽기 등에 사용
import random             # 랜덤 숫자 생성 (여기서는 크롤링 딜레이에 사용)
import time               # 시간 관련 함수 (sleep 등)

# from A import B 형태: A 모듈에서 B만 꺼내 쓰겠다는 뜻
from abc import ABC, abstractmethod   # ABC: 추상 클래스를 만들기 위한 베이스 클래스
                                       # abstractmethod: 자식 클래스가 반드시 구현해야 할 메서드를 표시하는 데코레이터
from dataclasses import dataclass      # @dataclass 데코레이터: 클래스에 __init__, __eq__ 등을 자동 생성
from datetime import datetime          # 날짜/시간 처리 클래스
from typing import ClassVar            # ClassVar: "이 변수는 인스턴스가 아니라 클래스 자체의 변수"라고 표시할 때 사용

# ── 외부 패키지 import (pip install 로 설치한 것들) ──
import requests                              # HTTP 요청을 보내는 라이브러리 (웹 API 호출에 사용)
from requests.adapters import HTTPAdapter    # HTTP 연결 설정을 커스텀하기 위한 어댑터
from urllib3.util.retry import Retry         # HTTP 요청 실패 시 재시도 정책 설정

# ── 프로젝트 내부 모듈 import ──
from config import load_crawler_config
from core.circuit_breaker import CircuitOpenError, get_breaker
# CircuitOpenError: 서킷 브레이커가 열려있을 때 발생하는 예외
# get_breaker: 서킷 브레이커 인스턴스를 가져오는 함수
# (서킷 브레이커 = 외부 서비스가 계속 실패하면 일정 시간 요청을 차단하는 패턴)

# __name__은 현재 모듈의 이름을 담고 있는 특수 변수.
# getLogger(__name__)으로 모듈별 로거를 생성하면 로그에서 어느 파일에서 나온 로그인지 구분 가능.
logger = logging.getLogger(__name__)


# @dataclass 는 데코레이터(decorator)라고 부른다.
# 데코레이터는 클래스나 함수 위에 @를 붙여서 기능을 추가하는 문법이다.
# @dataclass 를 붙이면 아래 적힌 변수들을 인자로 받는 __init__이 자동으로 만들어진다.
# frozen=True 옵션: 한번 만들면 값을 바꿀 수 없게 만든다 (불변 객체).
#   예) ref.title = "새 제목" 하면 에러가 난다.
@dataclass(frozen=True)
class JobListingRef:
    """목록 페이지에서 추출한 공고 식별 정보."""

    # 타입 힌트(type hint): 변수명 뒤에 : str 처럼 적어서 어떤 타입인지 표시한다.
    # 파이썬은 실행 시 타입을 강제하지 않지만, 코드 읽기와 IDE 자동완성에 도움이 된다.
    source: str           # 출처 사이트 이름 (예: "jumpit", "programmers")
    external_id: str      # 외부 사이트에서의 고유 ID
    url: str              # 공고 URL
    title: str            # 공고 제목
    company_name: str     # 회사 이름
    career_level: str = ""  # 경력 수준. = "" 은 기본값(default value).
                             # 값을 안 넘기면 빈 문자열이 들어간다.


@dataclass(frozen=True)
class JobDetail:
    """상세 페이지에서 추출한 JD 데이터. 구조화는 AI 서버에서 처리."""
    source: str
    external_id: str
    url: str
    company_name: str
    title: str
    raw_text: str
    tech_stack: tuple[str, ...]   # tuple[str, ...] 은 "문자열로 이루어진 튜플" 타입.
                                   # 튜플은 리스트와 비슷하지만 한번 만들면 수정 불가(불변).
                                   # ...은 "길이 상관없이"라는 뜻.
    deadline: int                  # 마감일 (Unix timestamp, 정수)
    crawled_at: str                # 크롤링 시각 (ISO 형식 문자열)
    career_level: str = ""

    # __post_init__: @dataclass 가 __init__을 자동 생성한 뒤, 그 직후에 호출되는 특수 메서드.
    # 데이터 유효성 검증(validation)을 하기에 좋은 위치다.
    def __post_init__(self):
        # for 루프: ("source", "external_id", ...) 튜플의 각 요소를 field_name에 하나씩 넣으며 반복
        for field_name in ("source", "external_id", "url", "company_name", "title"):
            # getattr(객체, 속성이름): 객체에서 해당 이름의 속성 값을 꺼낸다.
            # self.source 와 getattr(self, "source") 는 같은 결과.
            # 변수 이름이 문자열로 주어질 때 유용하다.
            value = getattr(self, field_name)

            # strip(): 문자열 앞뒤의 공백을 제거. "  hello  ".strip() → "hello"
            # not str(value).strip(): 빈 문자열이면 True (파이썬에서 빈 문자열은 False로 평가)
            if value is None or not str(value).strip():
                # raise: 예외(에러)를 일으킨다. 프로그램 실행이 여기서 중단됨.
                # ValueError: "값이 잘못됐다"는 뜻의 내장 예외 클래스.
                # f-string: f"..." 안에서 {변수}를 쓰면 변수 값이 문자열에 삽입된다.
                raise ValueError(
                    f"JobDetail.{field_name}이 비어있음: "
                    f"source={self.source}, id={self.external_id}"
                )

        # len(): 문자열이나 리스트의 길이(개수)를 반환한다.
        if len(self.raw_text.strip()) < 20:
            raise ValueError(
                f"raw_text가 너무 짧음({len(self.raw_text.strip())}자): "
                f"source={self.source}, id={self.external_id}"
            )


# ── 설정 파일에서 로드 ──

_CRAWLER_CONFIG = load_crawler_config()
DEFAULT_DELAY_MIN: float = _CRAWLER_CONFIG["delay_min"]
DEFAULT_DELAY_MAX: float = _CRAWLER_CONFIG["delay_max"]
DEFAULT_MAX_PAGES: int = _CRAWLER_CONFIG["max_pages"]
DEFAULT_STALE_PAGE_THRESHOLD: int = _CRAWLER_CONFIG["stale_page_threshold"]


# 함수 정의: def 함수이름(매개변수) -> 반환타입:
# *, 뒤에 오는 매개변수들은 "키워드 전용 인자"다.
#   → 호출할 때 반드시 이름을 지정해야 한다: make_crawler_session(extra_headers={...})
#   → make_crawler_session({...}) 이렇게 위치만으로 넘기면 에러.
# dict | None: dict 또는 None이 올 수 있다는 타입 힌트. 파이썬 3.10+ 문법.
# -> requests.Session: 이 함수가 requests.Session 객체를 반환한다는 의미.
def make_crawler_session(*, extra_headers: dict | None = None) -> requests.Session:
    """표준 크롤러 세션 생성: 재시도 정책 + 공통 헤더."""

    # Session: HTTP 연결을 재사용하는 객체. 매번 새로 연결하는 것보다 효율적.
    session = requests.Session()

    # Retry: HTTP 요청 실패 시 자동으로 재시도하는 정책 설정.
    #   total=3: 최대 3번 재시도
    #   backoff_factor=1: 재시도 간격을 점점 늘림 (1초, 2초, 4초...)
    #   status_forcelist: 이 HTTP 상태 코드를 받으면 재시도 (429=너무 많은 요청, 5xx=서버 에러)
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])

    # mount: "이 URL 패턴으로 시작하는 요청에는 이 어댑터를 써라"
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))

    # headers.update(): 세션의 기본 HTTP 헤더를 설정.
    # 모든 요청에 이 헤더가 자동으로 포함된다.
    session.headers.update({
        "Accept": "application/json",                           # JSON 형식 응답을 원한다고 서버에 알림
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",  # 한국어 우선 요청
        "User-Agent": (                                          # 크롤러 신원을 밝히는 헤더
            "passroute-bot/1.0 "
            "(+https://github.com/yoonjungeebang/passroute-crawler; yezanee@gmail.com)"
        ),
    })

    # if extra_headers: → extra_headers 가 None 이 아니고 빈 딕셔너리도 아니면 True
    if extra_headers:
        session.headers.update(extra_headers)
    return session


# class JobCrawler(ABC): ABC를 상속(inheritance)하는 클래스.
# ABC = Abstract Base Class (추상 베이스 클래스).
# 추상 클래스는 직접 인스턴스를 만들 수 없고, 반드시 자식 클래스에서 상속해서 써야 한다.
# 상속: 부모 클래스의 모든 기능을 물려받고, 필요한 부분만 재정의(오버라이드)하는 것.
class JobCrawler(ABC):

    # ClassVar[str]: "이 변수는 클래스 전체가 공유하는 변수"라는 의미.
    # 인스턴스마다 다른 값이 아니라, JumpitCrawler.source = "jumpit" 처럼 클래스에 하나.
    source: ClassVar[str]
    base_url: ClassVar[str] = ""

    # __init__: 생성자(constructor). 객체가 만들어질 때 자동으로 호출된다.
    # self: 파이썬 메서드의 첫 번째 인자는 항상 self (자기 자신을 가리킴).
    #        자바의 this와 비슷하지만 파이썬은 명시적으로 적어야 한다.
    # *: 이 뒤의 매개변수들은 키워드 전용 (이름을 지정해서만 전달 가능)
    def __init__(
        self,
        *,
        delay_min: float | None = None,
        delay_max: float | None = None,
        max_pages: int | None = None,
        stale_page_threshold: int = DEFAULT_STALE_PAGE_THRESHOLD,
        exclude_title_keywords: tuple[str, ...] = (),  # () 은 빈 튜플 (기본값)
    ):
        # self.source.upper(): 문자열을 대문자로 변환. "jumpit" → "JUMPIT"
        prefix = self.source.upper()

        # 삼항 연산자(conditional expression): A if 조건 else B
        # "조건이 참이면 A, 거짓이면 B"
        # os.environ.get(): 환경변수를 읽는다. 없으면 두 번째 인자(기본값)를 반환.
        self.delay_min = delay_min if delay_min is not None else float(os.environ.get(f"{prefix}_DELAY_MIN", DEFAULT_DELAY_MIN))
        self.delay_max = delay_max if delay_max is not None else float(os.environ.get(f"{prefix}_DELAY_MAX", DEFAULT_DELAY_MAX))
        self.max_pages = max_pages if max_pages is not None else int(os.environ.get(f"{prefix}_MAX_PAGES", DEFAULT_MAX_PAGES))
        self.stale_page_threshold = stale_page_threshold
        self.exclude_title_keywords = exclude_title_keywords

        # _breaker: 이름 앞에 _ (밑줄 1개)가 붙으면 "내부용"이라는 관례.
        # 외부에서 접근은 가능하지만, "직접 쓰지 말라"는 의도.
        self._breaker = get_breaker(self.source)
        self.session = make_crawler_session()

    # @abstractmethod: 이 메서드는 자식 클래스가 반드시 구현해야 한다.
    # 구현하지 않으면 인스턴스를 만들 때 TypeError가 발생.
    # ... (Ellipsis): "본문은 없다"는 뜻. pass 와 비슷한 역할.
    @abstractmethod
    def fetch_listings_page(self, page: int) -> list[JobListingRef]: ...

    @abstractmethod
    def fetch_detail(self, ref: JobListingRef) -> JobDetail | None: ...

    def collect_listings(self) -> list[JobListingRef]:
        """전체 목록 페이지를 순회해 공고를 모은다. stale 감지로 조기 종료."""

        # list[JobListingRef] = []: 빈 리스트를 만들되, "이 리스트에는 JobListingRef가 들어간다"고 표시
        all_refs: list[JobListingRef] = []

        # set: 집합 자료형. 중복을 허용하지 않는다.
        #   리스트: [1, 2, 2, 3] → 중복 허용
        #   셋:    {1, 2, 3}   → 중복 불가, 순서 없음
        #   in 검사가 리스트보다 훨씬 빠르다 (O(1) vs O(n)).
        seen_ids: set[str] = set()
        stale_pages = 0

        # range(1, N+1): 1부터 N까지의 숫자를 순서대로 생성.
        # range(5) → 0,1,2,3,4  /  range(1, 5) → 1,2,3,4
        for page in range(1, self.max_pages + 1):

            # try/except: 예외 처리. try 블록 안에서 에러가 나면 except 블록이 실행된다.
            try:
                # with 문: "컨텍스트 매니저"를 사용하는 문법.
                # 블록에 들어갈 때와 나올 때 자동으로 설정/정리 작업을 해준다.
                # 여기서는 self._breaker 가 서킷 브레이커 역할을 한다.
                with self._breaker:
                    refs = self.fetch_listings_page(page)
            except CircuitOpenError:
                # 서킷이 열려있으면 (= 요청 차단 상태) 로그를 남기고 중단
                logger.warning("서킷 OPEN, 목록 수집 조기 종료: source=%s", self.source)
                break   # break: for 루프를 즉시 빠져나간다
            except Exception:
                # Exception: 모든 일반 예외의 부모 클래스. 어떤 에러든 잡는다.
                # logger.exception(): 에러 메시지 + 스택 트레이스(에러 발생 위치)를 함께 출력
                logger.exception("목록 요청 실패: source=%s page=%d", self.source, page)
                break

            # not refs: 빈 리스트 []는 파이썬에서 False로 평가됨.
            # 즉 "결과가 없으면" 루프 종료.
            if not refs:
                break

            added = 0
            for ref in refs:
                # in 연산자: seen_ids 집합에 ref.external_id가 이미 있는지 확인
                if ref.external_id in seen_ids:
                    continue  # continue: 이번 반복을 건너뛰고 다음 ref로 넘어감
                if self._is_excluded_title(ref.title):
                    continue

                seen_ids.add(ref.external_id)   # set 에 항목 추가
                all_refs.append(ref)             # 리스트 끝에 항목 추가
                added += 1                       # += 1 은 added = added + 1 의 줄임

            # logger.info(): %s, %d 는 각각 문자열, 정수 포매팅 (C 스타일)
            logger.info(
                "source=%s page=%d: 조회 %d건, 신규 %d건 (누적 %d건)",
                self.source, page, len(refs), added, len(all_refs),
            )

            if added == 0:
                stale_pages += 1
                if stale_pages >= self.stale_page_threshold:
                    logger.info("source=%s: stale %d페이지 연속, 조기 종료", self.source, stale_pages)
                    break
            else:
                stale_pages = 0

            self._delay()

        return all_refs  # return: 함수의 결과값을 돌려보낸다

    def _is_excluded_title(self, title: str) -> bool:
        """제목에 제외 키워드가 포함되어 있는지 확인. -> bool: 반환 타입이 True/False."""
        if not self.exclude_title_keywords:
            return False  # 제외 키워드가 없으면 항상 False (제외하지 않음)

        lowered = title.lower()  # lower(): 문자열을 소문자로 변환. "ABC" → "abc"

        # any(): 괄호 안의 조건들 중 하나라도 True면 True를 반환.
        #   반대로 all()은 모든 조건이 True여야 True.
        # (... for kw in ...): 제너레이터 표현식. 리스트 컴프리헨션과 비슷하지만
        #   [] 대신 ()을 쓰며, 값을 한 번에 다 만들지 않고 필요할 때마다 하나씩 만든다.
        # "kw.lower() in lowered": 키워드가 제목에 포함되어 있는지 (부분 문자열 검사)
        return any(kw.lower() in lowered for kw in self.exclude_title_keywords)

    def _delay(self) -> None:
        """크롤링 간 대기. -> None: 반환값이 없다는 뜻."""
        # random.uniform(a, b): a 이상 b 이하의 랜덤 실수를 반환
        # time.sleep(초): 지정한 시간만큼 프로그램을 일시 정지
        time.sleep(random.uniform(self.delay_min, self.delay_max))

    # @staticmethod: 정적 메서드. self를 받지 않는다.
    # 즉 인스턴스의 상태(속성)에 접근하지 않는, 독립적인 유틸리티 함수.
    # 클래스에 논리적으로 묶어두고 싶을 때 사용한다.
    # 호출: JobCrawler._parse_deadline("2024-12-31") 또는 인스턴스에서도 호출 가능.
    @staticmethod
    def _parse_deadline(deadline_str: str) -> int:
        """ISO 8601 마감일 문자열을 Unix timestamp 로 변환. 파싱 실패 시 0(상시채용)."""
        if not deadline_str:  # 빈 문자열이면 0 반환
            return 0
        try:
            # datetime.fromisoformat(): "2024-12-31T23:59:59+09:00" 같은 ISO 형식 문자열을 datetime으로 변환
            # .replace("Z", "+00:00"): UTC를 나타내는 "Z"를 파이썬이 이해하는 형식으로 치환
            dt = datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
            # .timestamp(): datetime 을 Unix 타임스탬프(1970년 1월 1일부터 지난 초 수)로 변환
            # int(): 소수점 이하 버림
            return int(dt.timestamp())
        except (ValueError, TypeError):
            # 여러 예외를 한 번에 잡을 때 튜플로 묶는다: except (A, B)
            return 0

    @staticmethod
    def _parse_career(data: dict) -> str:
        """minCareer/maxCareer 필드로 경력 구간 문자열을 생성한다."""
        # dict.get(키, 기본값): 딕셔너리에서 키에 해당하는 값을 가져온다.
        # 키가 없으면 기본값(-1)을 반환. data["minCareer"]와 달리 KeyError가 나지 않는다.
        min_career = data.get("minCareer", -1)
        max_career = data.get("maxCareer", -1)
        if min_career <= 0 and max_career <= 0:
            return "신입"
        if min_career <= 0:
            return f"신입~{max_career}년"
        return f"{min_career}~{max_career}년"
