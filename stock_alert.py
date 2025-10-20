# volume_alert.py
import os
import requests
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta

def send_message(message: str):
    """텔레그램으로 메시지를 전송합니다."""
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Telegram API는 chat_id와 text를 받아 메시지를 보냅니다:contentReference[oaicite:3]{index=3}.
    requests.post(url, data={"chat_id": chat_id, "text": message})

def fetch_candidates():
    """전일 거래량이 전전일 대비 5배 이상인 종목 목록을 반환합니다."""
    today = datetime.now().date()
    # 최근 7일 데이터를 가져와 주말/공휴일을 건너뜁니다.
    start_date = today - timedelta(days=7)

    # 코스피와 코스닥 전체 종목 목록
    kospi = fdr.StockListing("KOSPI")
    kosdaq = fdr.StockListing("KOSDAQ")

    candidates = []
    # 두 시장을 모두 반복합니다.
    for listing_df in [kospi, kosdaq]:
        for code, name in zip(listing_df["Code"], listing_df["Name"]):
            try:
                df = fdr.DataReader(code, start_date, today)
                # 데이터가 최소 두 거래일 이상인지 확인
                if len(df) >= 2:
                    vol_yesterday = df["Volume"].iloc[-1]
                    vol_before = df["Volume"].iloc[-2]
                    if vol_before > 0 and vol_yesterday >= 5 * vol_before:
                        candidates.append(f"{name} ({code}) : {vol_yesterday:,}주")
            except Exception as e:
                # 예: 상장폐지/거래정지 등으로 데이터 조회 실패시 건너뜀
                continue
    return candidates

def main():
    candidates = fetch_candidates()
    if candidates:
        message = "📊 전일 거래량이 전전일 대비 5배 이상 증가한 종목 목록\n" + "\n".join(candidates)
    else:
        message = "📭 전일 대비 거래량이 5배 이상 증가한 종목이 없습니다."
    send_message(message)

if __name__ == "__main__":
    main()
