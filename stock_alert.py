import os, requests, pandas as pd, FinanceDataReader as fdr
from datetime import datetime, timedelta

# 1. 어제와 그제 날짜
today = datetime.now().date()
yesterday = today - timedelta(days=1)
before_yesterday = today - timedelta(days=2)

# 2. 코스피 종목 전체
krx = fdr.StockListing('KOSPI')

alert_list = []

# 3. 각 종목별 거래량 비교
for code, name in zip(krx['Code'], krx['Name']):
    try:
        df = fdr.DataReader(code, before_yesterday, today)
        if len(df) >= 2:
            vol_y = df['Volume'][-1]
            vol_b = df['Volume'][-2]
            if vol_y >= 5 * vol_b:
                alert_list.append(f"{name} ({code}) : {vol_y:,}주 ↑")
    except:
        continue

# 4. 텔레그램으로 전송
token = os.environ[7552444236:AAEsq2Rqi4kLdyfBtYGqDqdT8Eww5GwJfOk]
chat_id = os.environ[8070597541]

if alert_list:
    text = "📊 거래량 5배 이상 급등 종목:\n" + "\n".join(alert_list)
else:
    text = "📭 거래량 급등 종목이 없습니다."

requests.post(
    f"https://api.telegram.org/bot{token}/sendMessage",
    data={"chat_id": chat_id, "text": text}
)
