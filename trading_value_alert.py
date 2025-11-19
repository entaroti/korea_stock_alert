# tading_value_alert.py  → pykrx 기반 "거래대금 급증" 알림 봇
import os
import requests
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock


def send_message(message: str):
    """텔레그램으로 메시지를 전송합니다."""
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("CHAT_ID")

    if not token or not chat_id:
        print("❌ TELEGRAM_TOKEN 또는 CHAT_ID 환경변수가 설정되지 않았습니다.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }
    resp = requests.post(url, data=payload)
    if resp.status_code != 200:
        print("❌ 텔레그램 전송 실패:", resp.text)
    else:
        print("✅ 텔레그램 전송 성공")


def fetch_candidates():
    """
    전일 거래대금이 전전일 대비 5배 이상인 종목 목록을 반환합니다.
    - 거래대금은 pykrx의 KRX 공식 '거래대금' 컬럼 사용 (원 단위)
    - KOSPI + KOSDAQ 전체 종목 대상
    """
    today = datetime.now().date()
    start_date = today - timedelta(days=7)  # 최근 7일치만 조회 (주말 포함일 기준)
    start = start_date.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    # 코스피, 코스닥 종목 리스트 (KRX 기준)
    kospi_codes = stock.get_market_ticker_list(market="KOSPI")
    kosdaq_codes = stock.get_market_ticker_list(market="KOSDAQ")
    tickers = kospi_codes + kosdaq_codes

    candidates = []

    for code in tickers:
        try:
            # 최근 며칠간 일별 OHLCV + 거래대금
            df = stock.get_market_ohlcv_by_date(start, end, code)
            if df is None or df.empty:
                continue

            # 최소 2거래일 이상 있어야 전일/전전일 비교 가능
            if df.shape[0] < 2:
                continue

            # KRX 공식 거래대금 (원 단위)
            trading_value = df["거래대금"]

            val_yesterday = float(trading_value.iloc[-1])
            val_before = float(trading_value.iloc[-2])

            if val_before <= 0:
                continue

            # 조건: 전일 거래대금이 전전일 대비 5배 이상
            if val_yesterday >= 5 * val_before:
                name = stock.get_market_ticker_name(code)
                candidates.append(
                    {
                        "code": code,
                        "name": name,
                        "trading_value": val_yesterday,  # 원 단위
                    }
                )

        except Exception:
            # 정지/상폐/예외 종목은 그냥 스킵
            continue

    if not candidates:
        return []

    # 전일 거래대금 기준 내림차순 정렬
    candidates = sorted(
        candidates, key=lambda x: x["trading_value"], reverse=True
    )
    return candidates


def main():
    candidates = fetch_candidates()

    if candidates:
        header = "번호\t종목명\t전일거래대금(억)"
        lines = [header]

        for idx, c in enumerate(candidates, start=1):
            # 원 → 억 단위 변환
            tv_eok = c["trading_value"] / 1e8
            line = f"{idx:02d}\t{c['name']} ({c['code']})\t{tv_eok:,.1f}"
            lines.append(line)

        table = "\n".join(lines)
        message = (
            "📊 전일 거래대금이 전전일 대비 5배 이상 증가한 종목 목록\n"
            "```text\n"
            f"{table}\n"
            "```"
        )
    else:
        message = "📭 전일 대비 거래대금이 5배 이상 증가한 종목이 없습니다."

    send_message(message)


if __name__ == "__main__":
    main()
