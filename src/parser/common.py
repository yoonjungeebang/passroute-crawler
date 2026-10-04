"""사이트 무관한 파싱 공용 상수/헬퍼.

여러 크롤러가 공유하는 텍스트 정리 함수와 정규화 맵을 모은다.
사이트별 파서는 이 모듈을 import해서 기술스택 이름을 일관되게 정규화한다.
"""
import html
import re
import unicodedata

# 기술스택 이름 정규화 맵 (소문자 키 → 표준형 값)
# 잡코리아는 `JAVA`, `Java` 등 대소문자가 섞여 들어오므로 메타데이터 필터 일관성을 위해 통일한다.
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
    stripped = name.strip()
    return TECH_NAME_MAP.get(stripped.lower(), stripped)


_RE_HTML_TAG = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    """HTML 태그 제거 + 엔티티 디코딩 + 유니코드 정규화.

    NFKC 정규화로 non-breaking space(\\xa0) 등 특수 공백 문자를 일반 공백으로 변환한다.
    NFKD 는 한글을 자모로 분해하므로 NFKC 를 사용한다.
    """
    cleaned = _RE_HTML_TAG.sub("", text)
    cleaned = html.unescape(cleaned)
    cleaned = unicodedata.normalize("NFKC", cleaned)
    return cleaned.strip()
