"""회사명/제목 정규화 단위 테스트."""

from core.normalize import normalize_company_name, normalize_title


def test_removes_parenthesized_ju():
    assert normalize_company_name("(주)카카오") == "카카오"


def test_removes_jusikhwesa():
    assert normalize_company_name("카카오 주식회사") == "카카오"


def test_removes_circled_ju():
    assert normalize_company_name("㈜네이버") == "네이버"


def test_removes_inc():
    assert normalize_company_name("Google Inc.") == "google"


def test_removes_corp():
    assert normalize_company_name("Kakao Corp.") == "kakao"


def test_removes_co_ltd():
    assert normalize_company_name("Samsung Co., Ltd.") == "samsung"


def test_removes_ltd():
    assert normalize_company_name("Toss Ltd.") == "toss"


def test_removes_llc():
    assert normalize_company_name("OpenAI LLC") == "openai"


def test_strips_special_chars_and_spaces():
    assert normalize_company_name("카카오 페이") == "카카오페이"


def test_lowercases():
    assert normalize_company_name("NAVER") == "naver"


def test_nfkc_fullwidth():
    assert normalize_company_name("\uff2e\uff21\uff36\uff25\uff32") == "naver"


def test_empty_string():
    assert normalize_company_name("") == ""


def test_only_special_chars():
    assert normalize_company_name("---") == ""


def test_complex_name():
    assert normalize_company_name("(주) 카카오페이 Corp.") == "카카오페이"


# ── normalize_title ──


def test_title_lowercases():
    assert normalize_title("Backend Developer") == "backenddeveloper"


def test_title_strips_special_chars():
    assert normalize_title("[신입/경력] 백엔드 개발자") == "신입경력백엔드개발자"


def test_title_nfkc():
    assert normalize_title("Ｐｙｔｈｏｎ 개발자") == "python개발자"


def test_title_empty():
    assert normalize_title("") == ""
