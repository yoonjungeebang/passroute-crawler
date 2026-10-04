f"""pgvector 유사도 검색 모듈. 임베딩 벡터로 채용공고를 검색한다."""
import logging

logger = logging.getLogger(__name__)

_SEARCH_QUERY = """
SELECT url, source, company_name, title, document, career_level, tech_stack, deadline,
       1 - (embedding <=> %(embedding)s::vector) AS similarity
FROM job_descriptions
WHERE embedding IS NOT NULL
  AND (deadline = 0 OR deadline > EXTRACT(EPOCH FROM NOW()))
ORDER BY embedding <=> %(embedding)s::vector
LIMIT %(limit)s
"""

_SEARCH_BY_COMPANY = """
SELECT url, source, company_name, title, document, career_level, tech_stack, deadline,
       1 - (embedding <=> %(embedding)s::vector) AS similarity
FROM job_descriptions
WHERE embedding IS NOT NULL
  AND company_name ILIKE %(company)s
  AND (deadline = 0 OR deadline > EXTRACT(EPOCH FROM NOW()))
ORDER BY embedding <=> %(embedding)s::vector
LIMIT %(limit)s
"""


def search_jobs(conn, embedding: list[float], *, limit: int = 10, company: str | None = None) -> list[dict]:
    """임베딩 벡터로 채용공고를 유사도 검색한다."""
    embedding_str = f"[{','.join(str(v) for v in embedding)}]"

    if company:
        query = _SEARCH_BY_COMPANY
        params = {"embedding": embedding_str, "limit": limit, "company": f"%{company}%"}
    else:
        query = _SEARCH_QUERY
        params = {"embedding": embedding_str, "limit": limit}

    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]
