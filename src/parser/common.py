"""사이트 무관한 파싱 공용 상수/헬퍼.

여러 크롤러가 공유하는 텍스트 정리 함수와 정규화 맵을 모은다.
사이트별 파서는 이 모듈을 import해서 기술스택 이름을 일관되게 정규화한다.
"""
import hashlib       # 해시 함수 라이브러리. 문자열을 고정 길이의 해시값으로 변환할 때 사용.
import html          # HTML 엔티티(&amp; → & 등)를 디코딩하는 라이브러리.
import re            # 정규표현식(Regular Expression). 문자열 패턴 매칭/치환에 사용.
import unicodedata   # 유니코드 문자 정보와 정규화 함수 제공.

# dict[str, str]: "키도 문자열, 값도 문자열인 딕셔너리" 타입 힌트
# 딕셔너리(dict): 키-값 쌍의 자료구조. {"키1": "값1", "키2": "값2"}
# 기술스택 이름 정규화 맵 (소문자 키 → 표준형 값)
TECH_NAME_MAP: dict[str, str] = {
    # 언어
    "java": "Java",
    "javascript": "JavaScript",
    "typescript": "TypeScript",
    "python": "Python",
    "파이썬": "Python",
    "자바": "Java",
    "자바스크립트": "JavaScript",
    "타입스크립트": "TypeScript",
    "c": "C",
    "c++": "C++",
    "c#": "C#",
    "kotlin": "Kotlin",
    "swift": "Swift",
    "go": "Go",
    "golang": "Go",
    "rust": "Rust",
    "ruby": "Ruby",
    "php": "PHP",
    "scala": "Scala",
    "r": "R",
    "dart": "Dart",
    # 프레임워크/라이브러리
    "spring": "Spring",
    "spring boot": "Spring Boot",
    "springboot": "Spring Boot",
    "django": "Django",
    "flask": "Flask",
    "fastapi": "FastAPI",
    "react": "React",
    "react.js": "React",
    "reactjs": "React",
    "vue": "Vue",
    "vue.js": "Vue",
    "vuejs": "Vue",
    "angular": "Angular",
    "next.js": "Next.js",
    "nextjs": "Next.js",
    "node.js": "Node.js",
    "nodejs": "Node.js",
    "nestjs": "NestJS",
    "nest.js": "NestJS",
    "express": "Express",
    ".net": ".NET",
    "dotnet": ".NET",
    "tensorflow": "TensorFlow",
    "pytorch": "PyTorch",
    "pandas": "Pandas",
    "numpy": "NumPy",
    # DB/데이터 스토어
    "mysql": "MySQL",
    "postgresql": "PostgreSQL",
    "postgres": "PostgreSQL",
    "oracle": "Oracle",
    "mongodb": "MongoDB",
    "redis": "Redis",
    "elasticsearch": "Elasticsearch",
    "mariadb": "MariaDB",
    "dynamodb": "DynamoDB",
    "kafka": "Kafka",
    "rabbitmq": "RabbitMQ",
    # 클라우드/인프라
    "aws": "AWS",
    "gcp": "GCP",
    "azure": "Azure",
    "docker": "Docker",
    "kubernetes": "Kubernetes",
    "k8s": "Kubernetes",
    "terraform": "Terraform",
    "jenkins": "Jenkins",
    "linux": "Linux",
    "nginx": "Nginx",
    # 툴
    "git": "Git",
    "github": "GitHub",
    "gitlab": "GitLab",
    "jira": "Jira",
    "figma": "Figma",
    "confluence": "Confluence",
}


def normalize_tech_name(name: str) -> str:
    """기술스택 이름을 표준형으로 변환. 맵에 없으면 공백만 trim 한 원본을 반환."""
    stripped = name.strip()  # strip(): 앞뒤 공백 제거
    # TECH_NAME_MAP.get(키, 기본값): 키가 맵에 있으면 매핑된 값 반환, 없으면 기본값(stripped) 반환
    return TECH_NAME_MAP.get(stripped.lower(), stripped)


def make_external_id(url: str) -> str:
    """URL 에서 결정적 external_id 를 생성."""
    # url.encode(): 문자열을 바이트로 변환 (해시 함수는 바이트를 입력으로 받음)
    # hashlib.sha256(): SHA-256 해시 생성 (같은 입력이면 항상 같은 결과)
    # .hexdigest(): 해시값을 16진수 문자열로 반환
    # [:16]: 슬라이싱. 문자열의 처음 16자만 잘라낸다.
    #   문자열[시작:끝] → 시작 인덱스부터 끝 인덱스 전까지
    #   [:16] → 처음부터 16번째 전까지 (0~15번 인덱스)
    return hashlib.sha256(url.encode()).hexdigest()[:16]


# re.compile(): 정규표현식 패턴을 미리 컴파일해 놓으면 반복 사용 시 성능이 좋아진다.
# r"..." : raw 문자열. 백슬래시(\)를 이스케이프하지 않는다. 정규식에서 자주 사용.
# 패턴 <[^>]+> 의 의미:
#   < : < 문자 그대로
#   [^>]+ : > 가 아닌 문자가 1개 이상 (+)
#   > : > 문자 그대로
#   → HTML 태그 (예: <b>, <div class="a">) 에 매칭
_RE_HTML_TAG = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    """HTML 태그 제거 + 엔티티 디코딩 + 유니코드 정규화.

    NFKC 정규화로 non-breaking space(\\xa0) 등 특수 공백 문자를 일반 공백으로 변환한다.
    NFKD 는 한글을 자모로 분해하므로 NFKC 를 사용한다.
    """
    # .sub("", text): 패턴에 매칭되는 부분을 빈 문자열로 치환 (= 삭제)
    cleaned = _RE_HTML_TAG.sub("", text)
    # html.unescape(): HTML 엔티티를 원래 문자로 변환
    #   "&amp;" → "&", "&lt;" → "<", "&#39;" → "'"
    cleaned = html.unescape(cleaned)
    # unicodedata.normalize("NFKC", ...): 유니코드 정규화
    #   \xa0 (non-breaking space) → 일반 공백으로 변환
    #   전각 문자 → 반각 문자로 변환 등
    cleaned = unicodedata.normalize("NFKC", cleaned)
    return cleaned.strip()
