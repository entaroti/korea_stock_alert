import os
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------------------------------
# 1. 텔레그램 메시지 보내기 함수
# ---------------------------------
def send_telegram_message(text: str):
    if TELEGRAM_BOT_TOKEN is None or TELEGRAM_CHAT_ID is None:
        print("텔레그램 설정이 안 되어 있습니다.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"  # 코드블럭 쓰려고
    }
    resp = requests.post(url, data=payload)
    if resp.status_code != 200:
        print("텔레그램 전송 실패:", resp.text)


# ---------------------------------
# 2. 최근 2주(10거래일) 우상향 종목 찾기
#    + 거래대금 10일/5일 평균 계산
# ---------------------------------
def get_uptrend_stocks(market="ALL", top_n=30):
    """
    market: "KOSPI", "KOSDAQ", "ALL"
    top_n: 상위 몇 종목까지 보여줄지
    """
    today = datetime.today()
    start_date = today - timedelta(days=30)  # 여유 있게 30일치 불러오기
    start = start_date.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    # 1) 전 종목 코드 리스트
    if market == "KOSPI":
        tickers = stock.get_market_ticker_list(market="KOSPI")
    elif market == "KOSDAQ":
        tickers = stock.get_market_ticker_list(market="KOSDAQ")
    else:
        tickers = stock.get_market_ticker_list(market="KOSPI") + \
                  stock.get_market_ticker_list(market="KOSDAQ")

    results = []

    for code in tickers:
        try:
            # 2) 시가/고가/저가/종가/거래대금 데이터 가져오기
            df = stock.get_market_ohlcv_by_date(start, end, code)
            if df.shape[0] < 15:
                continue  # 데이터가 너무 적으면 패스

            closes = df['종가']
            trading_value = df['거래대금']  # 원 단위

            if len(closes) < 11:
                continue

            # 최근 10거래일
            last_10_close = closes.tail(10)
            last_10_tv = trading_value.tail(10)

            # 최근 5거래일
            last_5_tv = trading_value.tail(5)

            # (1) 10거래일 수익률
            ret_10d = last_10_close.iloc[-1] / last_10_close.iloc[0] - 1

            # (2) 이동평균
            ma5 = closes.rolling(5).mean()
            ma10 = closes.rolling(10).mean()

            if ma10.isna().all() or ma5.isna().all():
                continue

            ma5_last = ma5.iloc[-1]
            ma10_last = ma10.iloc[-1]
            ma5_5days_ago = ma5.iloc[-6]  # 5거래일 전

            # 우상향 조건
            cond_price_up = ret_10d > 0             # 10일 수익률 플러스
            cond_ma_position = ma5_last > ma10_last # 단기선 > 중기선
            cond_ma_slope = ma5_last > ma5_5days_ago# 단기선 우상향

            if cond_price_up and cond_ma_position and cond_ma_slope:
                name = stock.get_market_ticker_name(code)

                avg_tv_10d = last_10_tv.mean()  # 최근 10영업일 일평균 거래대금
                avg_tv_5d = last_5_tv.mean()    # 최근 5영업일 일평균 거래대금

                results.append({
                    "code": code,
                    "name": name,
                    "ret_10d": ret_10d,
                    "avg_tv_10d": avg_tv_10d,
                    "avg_tv_5d": avg_tv_5d
                })

        except Exception as e:
            # 개별 종목 오류는 무시하고 넘어가기
            continue

    # 3) 수익률 기준 정렬
    df_res = pd.DataFrame(results)
    if df_res.empty:
        return df_res

    df_res = df_res.sort_values(by="ret_10d", ascending=False).reset_index(drop=True)

    # 상위 N개만
    return df_res.head(top_n)


# ---------------------------------
# 3. 메인: 종목 뽑아서 텔레그램으로 보내기
#    5열 포맷: 번호 / 종목명 / 수익률 / 10일평균거래대금 / 5일평균거래대금
# ---------------------------------
def main():
    df_up = get_uptrend_stocks(market="ALL", top_n=30)

    if df_up.empty:
        send_telegram_message("최근 2주 우상향 조건을 만족하는 종목이 없습니다.")
        return

    # 엑셀처럼 5열 탭 정렬 → 코드블럭으로 보내기
    lines = []
    lines.append("📈 *최근 2주 우상향 종목 Top 30*")
    lines.append("_(조건: 10거래일 수익률>0, 5일선>10일선, 5일선 우상향)_\n")

    header = "번호\t종목명\t수익률(10일)\t10일평균거래대금(억)\t5일평균거래대금(억)"
    table_lines = [header]

    for i, row in df_up.iterrows():
        rank = i + 1
        name = row["name"]
        ret_10d = row["ret_10d"] * 100

        # 원 → 억 단위로 변환
        tv10 = row["avg_tv_10d"] / 1e8
        tv5 = row["avg_tv_5d"] / 1e8

        line = (
            f"{rank:02d}\t"
            f"{name}\t"
            f"{ret_10d:.2f}%\t"
            f"{tv10:,.1f}\t"
            f"{tv5:,.1f}"
        )
        table_lines.append(line)

    # 텔레그램에서 표 정렬을 위해 코드블럭 사용
    text = "\n".join(lines) + "\n```text\n" + "\n".join(table_lines) + "\n```"
    send_telegram_message(text)


if __name__ == "__main__":
    main()
