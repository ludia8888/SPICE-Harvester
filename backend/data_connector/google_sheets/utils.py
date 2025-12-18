"""
Google Sheets Connector - Utility Functions
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse


def extract_sheet_id(sheet_url: str) -> str:
    """
    Google Sheets URL에서 Sheet ID 추출

    Args:
        sheet_url: Google Sheets URL

    Returns:
        Sheet ID

    Raises:
        ValueError: 유효하지 않은 URL 형식
    """
    # Pattern: https://docs.google.com/spreadsheets/d/{SHEET_ID}/...
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError(f"Cannot extract sheet ID from URL: {sheet_url}")

    return match.group(1)


def extract_gid(sheet_url: str) -> Optional[str]:
    """
    Google Sheets URL에서 GID (worksheet ID) 추출

    Args:
        sheet_url: Google Sheets URL

    Returns:
        GID or None
    """
    # Try to extract from fragment (#gid=0)
    if "#gid=" in sheet_url:
        match = re.search(r"#gid=(\d+)", sheet_url)
        if match:
            return match.group(1)

    # Try to extract from query params (?gid=0)
    parsed = urlparse(sheet_url)
    if parsed.query:
        params = parse_qs(parsed.query)
        if "gid" in params:
            return params["gid"][0]

    return None


def build_sheets_api_url(sheet_id: str, range_name: str = "Sheet1") -> str:
    """
    Google Sheets API v4 URL 생성

    Args:
        sheet_id: Google Sheets ID
        range_name: 워크시트 이름 또는 범위

    Returns:
        API URL
    """
    base_url = "https://sheets.googleapis.com/v4/spreadsheets"
    return f"{base_url}/{sheet_id}/values/{range_name}"


def build_sheets_metadata_url(sheet_id: str) -> str:
    """
    Google Sheets 메타데이터 API URL 생성

    Args:
        sheet_id: Google Sheets ID

    Returns:
        Metadata API URL
    """
    base_url = "https://sheets.googleapis.com/v4/spreadsheets"
    return f"{base_url}/{sheet_id}"


def calculate_data_hash(data: List[List[Any]]) -> str:
    """
    데이터의 해시값 계산 (변경 감지용)

    Args:
        data: 2차원 데이터 배열

    Returns:
        SHA256 해시값
    """
    # Convert data to JSON string for consistent hashing
    data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(data_str.encode("utf-8")).hexdigest()


def normalize_sheet_data(raw_data: List[List[Any]]) -> Tuple[List[str], List[List[str]]]:
    """
    Google Sheets 원시 데이터 정규화

    Args:
        raw_data: API에서 받은 원시 데이터

    Returns:
        (columns, rows) 튜플
    """
    if not raw_data:
        return [], []

    # 첫 번째 행을 컬럼으로 처리
    columns = [str(cell) for cell in raw_data[0]] if raw_data else []

    # 나머지 행을 데이터로 처리
    rows = []
    for row in raw_data[1:]:
        # 모든 셀을 문자열로 변환하고, 부족한 셀은 빈 문자열로 채움
        normalized_row = []
        for i in range(len(columns)):
            if i < len(row):
                normalized_row.append(str(row[i]))
            else:
                normalized_row.append("")
        rows.append(normalized_row)

    return columns, rows


def validate_api_key(api_key: str) -> bool:
    """
    Google API 키 형식 검증

    Args:
        api_key: API 키

    Returns:
        유효성 여부
    """
    # Google API keys are typically 39 characters
    # Format: AIza[0-9A-Za-z\-_]{35}
    pattern = r"^AIza[0-9A-Za-z\-_]{35}$"
    return bool(re.match(pattern, api_key))


def format_datetime_iso(dt: datetime) -> str:
    """
    datetime을 ISO 8601 형식으로 변환

    Args:
        dt: datetime 객체

    Returns:
        ISO 8601 형식 문자열
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def parse_range_notation(range_str: str) -> Tuple[str, Optional[str]]:
    """
    A1 notation 범위 파싱

    Args:
        range_str: "Sheet1!A1:Z100" 형식의 범위

    Returns:
        (sheet_name, cell_range) 튜플
    """
    if "!" in range_str:
        parts = range_str.split("!", 1)
        return parts[0], parts[1]
    else:
        return range_str, None


def convert_column_letter_to_index(letter: str) -> int:
    """
    Excel 컬럼 문자를 인덱스로 변환 (A=0, B=1, ..., Z=25, AA=26, ...)

    Args:
        letter: 컬럼 문자 (예: "A", "AA")

    Returns:
        0부터 시작하는 인덱스
    """
    result = 0
    for char in letter.upper():
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def convert_index_to_column_letter(index: int) -> str:
    """
    인덱스를 Excel 컬럼 문자로 변환

    Args:
        index: 0부터 시작하는 인덱스

    Returns:
        컬럼 문자
    """
    result = ""
    index += 1  # 1-based for calculation

    while index > 0:
        index -= 1
        result = chr(index % 26 + ord("A")) + result
        index //= 26

    return result


def sanitize_worksheet_name(name: str) -> str:
    """
    워크시트 이름 정규화 (특수문자 제거)

    Args:
        name: 원본 워크시트 이름

    Returns:
        정규화된 이름
    """
    # Remove special characters that might cause issues
    sanitized = re.sub(r"[^\w\s\-_가-힣]", "", name)
    # Replace multiple spaces with single space
    sanitized = re.sub(r"\s+", " ", sanitized)
    return sanitized.strip()


def estimate_data_size(rows: List[List[Any]]) -> dict:
    """
    데이터 크기 추정

    Args:
        rows: 데이터 행

    Returns:
        크기 정보 딕셔너리
    """
    total_cells = 0
    total_chars = 0
    empty_cells = 0

    for row in rows:
        for cell in row:
            total_cells += 1
            cell_str = str(cell)
            if not cell_str or cell_str.isspace():
                empty_cells += 1
            else:
                total_chars += len(cell_str)

    return {
        "total_cells": total_cells,
        "empty_cells": empty_cells,
        "filled_cells": total_cells - empty_cells,
        "total_characters": total_chars,
        "average_cell_length": (
            total_chars / (total_cells - empty_cells) if total_cells > empty_cells else 0
        ),
    }
