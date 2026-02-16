"""
Dataroma Portfolio Scraper
- 시작 페이지에서 모든 구루(Portfolio Manager) 링크를 수집
- 각 구루의 상세 페이지에서 Q4 2025 데이터만 필터링
- Recent Activity가 Buy 또는 Add인 종목만 추출
- 결과를 dataroma_q4_2025_buys.xlsx 엑셀 파일로 저장
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import sys

BASE_URL = "https://www.dataroma.com"
MANAGERS_URL = f"{BASE_URL}/m/managers.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.dataroma.com/m/home.php",
}

OUTPUT_FILE = "dataroma_q4_2025_buys.xlsx"


def fetch_page(url: str, session: requests.Session) -> BeautifulSoup | None:
    """URL을 요청하고 BeautifulSoup 객체를 반환한다."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        print(f"  [ERROR] 요청 실패: {url} -> {e}")
        return None


def get_manager_links(session: requests.Session) -> list[dict]:
    """managers.php 페이지에서 구루 이름과 상세 페이지 URL을 수집한다."""
    print("=" * 60)
    print("1단계: Manager 리스트 수집 중...")
    print("=" * 60)

    soup = fetch_page(MANAGERS_URL, session)
    if soup is None:
        print("[FATAL] 매니저 리스트 페이지를 불러올 수 없습니다.")
        sys.exit(1)

    managers = []

    # managers.php 는 #port_body 안에 <a> 링크로 구루 목록이 있음
    # 링크 형식: /m/holdings.php?m=XXX
    for link in soup.select("a[href*='holdings.php?m=']"):
        href = link.get("href", "")
        name = link.get_text(strip=True)
        if not href or not name:
            continue
        full_url = BASE_URL + href if href.startswith("/") else href
        managers.append({"name": name, "url": full_url})

    # 중복 제거 (같은 URL이 여러 번 나올 수 있음)
    seen = set()
    unique = []
    for m in managers:
        if m["url"] not in seen:
            seen.add(m["url"])
            unique.append(m)

    print(f"  -> 총 {len(unique)}명의 매니저를 발견했습니다.\n")
    return unique


def is_q4_2025(soup: BeautifulSoup) -> bool:
    """페이지에 Q4 2025 (또는 2025-12-31 / Dec 2025 등) 데이터가 있는지 확인한다."""
    # 페이지 상단 텍스트에서 기간 정보를 찾음
    # 가능한 패턴: "Q4 2025", "Period: Q4 2025", "Portfolio date: 31 Dec 2025",
    #              "12/31/2025", "2025-12-31" 등
    page_text = soup.get_text(" ", strip=True)

    patterns = [
        r"Q4\s*2025",
        r"(?:31|30)\s*Dec(?:ember)?\s*2025",
        r"Dec(?:ember)?\s*(?:31|30),?\s*2025",
        r"12[/\-]31[/\-]2025",
        r"2025[/\-]12[/\-]31",
        r"Portfolio\s+date[:\s]+.*2025[/\-]12",
    ]

    for pattern in patterns:
        if re.search(pattern, page_text, re.IGNORECASE):
            return True
    return False


def parse_holdings(soup: BeautifulSoup, manager_name: str) -> list[dict]:
    """
    보유 종목 테이블(#grid)을 파싱하여
    Recent Activity가 Buy 또는 Add인 종목만 반환한다.
    """
    results = []

    table = soup.find("table", id="grid")
    if table is None:
        # id가 grid가 아닐 수 있으므로 다른 방법으로 시도
        table = soup.find("table", class_="holdings")
        if table is None:
            # 모든 테이블에서 "Stock" 헤더가 있는 것을 찾기
            for t in soup.find_all("table"):
                header_text = t.get_text(" ", strip=True)[:200]
                if "Stock" in header_text and "Activity" in header_text:
                    table = t
                    break

    if table is None:
        print(f"    [WARN] {manager_name}: 보유종목 테이블을 찾지 못했습니다.")
        return results

    # 헤더 행에서 컬럼 인덱스를 파악
    header_row = table.find("tr")
    if header_row is None:
        return results

    headers = []
    for th in header_row.find_all(["th", "td"]):
        headers.append(th.get_text(" ", strip=True).lower())

    # 컬럼 인덱스 매핑
    col_map = {}
    for i, h in enumerate(headers):
        if "stock" in h:
            col_map["stock"] = i
        elif "activity" in h:
            col_map["activity"] = i
        elif "portfolio" in h and "%" in h:
            col_map["portfolio_pct"] = i
        elif "reported" in h or ("price" in h and "reported" in h):
            col_map["price"] = i
        elif "price" in h and "price" not in col_map:
            col_map["price"] = i

    # 데이터 행 순회
    rows = table.find_all("tr")[1:]  # 헤더 제외
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # 종목명과 티커 추출
        stock_text = ""
        ticker = ""

        stock_idx = col_map.get("stock", 1)
        if stock_idx < len(cells):
            stock_cell = cells[stock_idx]
            # 티커는 보통 링크 안에 있거나 별도 span에 있음
            # 또는 "Stock Name - TKR" 형식
            stock_text = stock_cell.get_text(" ", strip=True)
            # 링크에서 티커 추출 시도
            stock_link = stock_cell.find("a")
            if stock_link:
                stock_text = stock_link.get_text(" ", strip=True)
                # href에서 sym= 파라미터로 티커 추출
                link_href = stock_link.get("href", "")
                sym_match = re.search(r"[?&]sym=([A-Z.]+)", link_href, re.IGNORECASE)
                if sym_match:
                    ticker = sym_match.group(1).upper()

        # 티커를 텍스트에서 추출 시도 (괄호 안이나 대시 뒤)
        if not ticker:
            ticker_match = re.search(r"[-–]\s*([A-Z]{1,5})\b", stock_text)
            if ticker_match:
                ticker = ticker_match.group(1)

        # Recent Activity 확인
        activity_idx = col_map.get("activity", 4)
        activity_text = ""
        if activity_idx < len(cells):
            activity_text = cells[activity_idx].get_text(" ", strip=True)

        # Buy 또는 Add인 경우만 필터
        activity_lower = activity_text.lower()
        if not ("buy" in activity_lower or "add" in activity_lower):
            continue

        # Portfolio %
        pct_text = ""
        pct_idx = col_map.get("portfolio_pct", 2)
        if pct_idx < len(cells):
            pct_text = cells[pct_idx].get_text(" ", strip=True)

        # Price
        price_text = ""
        price_idx = col_map.get("price", 5)
        if price_idx < len(cells):
            price_text = cells[price_idx].get_text(" ", strip=True)

        results.append({
            "Manager": manager_name,
            "Stock": stock_text.strip(),
            "Ticker": ticker,
            "Recent Activity": activity_text,
            "Portfolio %": pct_text,
            "Reported Price": price_text,
        })

    return results


def main():
    print()
    print("*" * 60)
    print("  Dataroma Q4 2025 Buy/Add 포트폴리오 스크래퍼")
    print("*" * 60)
    print()

    session = requests.Session()
    session.headers.update(HEADERS)

    # 1) 매니저 리스트 수집
    managers = get_manager_links(session)
    if not managers:
        print("[FATAL] 매니저를 찾을 수 없습니다. 종료합니다.")
        sys.exit(1)

    # 2) 개별 페이지 순회
    print("=" * 60)
    print("2단계: 개별 매니저 페이지 순회 (Q4 2025 필터링)")
    print("=" * 60)

    all_records = []
    q4_count = 0
    skip_count = 0

    for i, mgr in enumerate(managers, 1):
        name = mgr["name"]
        url = mgr["url"]
        print(f"  [{i}/{len(managers)}] {name} 확인 중...", end=" ")

        time.sleep(random.uniform(1, 3))

        soup = fetch_page(url, session)
        if soup is None:
            print("-> ERROR (요청 실패)")
            continue

        if not is_q4_2025(soup):
            print("-> Skip (Q4 2025 아님)")
            skip_count += 1
            continue

        # Q4 2025 데이터 확인 -> Buy/Add 종목 추출
        records = parse_holdings(soup, name)
        if records:
            all_records.extend(records)
            print(f"-> Found! ({len(records)}개 Buy/Add 종목)")
            q4_count += 1
        else:
            print("-> Q4 2025이지만 Buy/Add 종목 없음")
            q4_count += 1

    # 3) 결과 요약 및 엑셀 저장
    print()
    print("=" * 60)
    print("3단계: 결과 저장")
    print("=" * 60)
    print(f"  - Q4 2025 매니저 수: {q4_count}")
    print(f"  - Skip된 매니저 수: {skip_count}")
    print(f"  - 총 Buy/Add 레코드 수: {len(all_records)}")

    if all_records:
        df = pd.DataFrame(all_records)
        # 컬럼 순서 정렬
        columns = ["Manager", "Stock", "Ticker", "Recent Activity",
                    "Portfolio %", "Reported Price"]
        df = df[[c for c in columns if c in df.columns]]

        df.to_excel(OUTPUT_FILE, index=False, sheet_name="Q4 2025 Buys")
        print(f"\n  -> 결과가 '{OUTPUT_FILE}'에 저장되었습니다!")
    else:
        print("\n  -> 조건에 맞는 데이터가 없습니다. 엑셀 파일을 생성하지 않습니다.")

    print()
    print("완료!")


if __name__ == "__main__":
    main()
