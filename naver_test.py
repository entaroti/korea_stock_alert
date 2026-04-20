import requests
import pandas as pd
from io import StringIO

print("🔥 START")

url = "https://finance.naver.com/sise/sise_deal_rank.naver"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://finance.naver.com/"
}

res = requests.get(url, headers=headers)
tables = pd.read_html(StringIO(res.text))

print(f"총 테이블 개수: {len(tables)}")

# 👉 모든 테이블 컬럼 확인
for i, df in enumerate(tables):
    print(f"\n===== 테이블 {i} =====")
    print(df.head(3))
    print("컬럼:", df.columns)
