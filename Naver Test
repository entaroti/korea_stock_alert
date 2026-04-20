import pandas as pd

url = "https://finance.naver.com/sise/sise_deal_rank.naver"

tables = pd.read_html(url, encoding="euc-kr")

print(f"총 테이블 개수: {len(tables)}")

for i, df in enumerate(tables):
    print(f"\n===== 테이블 {i} =====")
    print(df.head())
