import traceback
import requests
import pandas as pd
from io import StringIO

url = "https://finance.naver.com/sise/sise_deal_rank.naver"

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
}

try:
    print("[DEBUG] requesting page...")
    resp = requests.get(url, headers=headers, timeout=20)
    print(f"[DEBUG] status_code: {resp.status_code}")
    print(f"[DEBUG] final_url: {resp.url}")
    print(f"[DEBUG] content length: {len(resp.text)}")

    resp.raise_for_status()

    print("[DEBUG] parsing html tables...")
    tables = pd.read_html(StringIO(resp.text))
    print(f"총 테이블 개수: {len(tables)}")

    for i, df in enumerate(tables):
        print(f"\n===== 테이블 {i} =====")
        print("컬럼:", list(df.columns))
        print(df.head(5).to_string())

except Exception as e:
    print("[ERROR] 예외 발생")
    print(repr(e))
    traceback.print_exc()
    raise
