# uptrend_alert.py
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock


def send_message(message: str):
    """텔레그램으로 메시지를 전송합니다."""
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("CHAT_ID")

    if not token or not chat_id:
        print("❌ TELEGRAM_TOKEN 또는 CHAT_ID 환경변수가 설정되지 않았습니다.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
        },
    )


def get_uptrend_stocks(market: str = "ALL", top_n: int = 30) -> pd.DataFrame:
    """
    최근 2주(10거래일) 우상향 종목 리스트를 반환합니다.
    - 조건:
      1) 최근 10거래일 수익률 > 0
      2) 5일 이동평균 > 10일 이동평균
      3) 5일 이동평균이 5거래일 전보다 상승 (우상향)
    - 반환 컬럼: code, name, ret_10d, avg_tv_10d, avg_tv_5d
    """

    today = datetime.now().date()
    # 이동평균/수익률 계산을 위해 여유 있게 30일치 조회
    start_date = today - timedelta(days=30)
    start = start_date.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    # 종목 리스트
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
            df = stock.get_market_ohlcv_by_date(start, end, code)
            # 최소 15거래일 이상 데이터 필요 (10일 수익률 + 이동평균 안정적 계산)
            if df.shape[0] < 15:
                continue

            closes = df["종가"]
            trading_value = df["거래대금"]  # 원 단위

            if len(closes) < 11:
                continue

            # 최근 10거래일 기준
            last_10_close = closes.tail(10)
            last_10_tv = trading_value.tail(10)
            last_5_tv = trading_value.tail(5)

            # (1) 10거래일 수익률
            ret_10d = last_10_close.iloc[-1] / last_10_close.iloc[0] - 1

            # (2) 이동평균
            ma5 = closes.rolling(5).mean()
            ma10 = closes.rolling(10).mean()

            if ma5.isna().all() or ma10.isna().all():
                continue

            ma5_last = ma5.iloc[-1]
            ma10_last = ma10.iloc[-1]
            # 5거래일 전 5일선 (인덱스상 -6)
            if len(ma5) < 6:
                continue
            ma5_5days_ago = ma5.iloc[-6]

            # 우상향 조건
            cond_price_up = ret_10d > 0              # 10거래일 수익률 플러스
            cond_ma_position = ma5_last > ma10_last  # 5일선 > 10일선
            cond_ma_slope = ma5_last > ma5_5days_ago # 5일선 우상향

            if cond_price_up and cond_ma_position and cond_ma_slope:
                name = stock.get_market_ticker_name(code)

                avg_tv_10d = float(last_10_tv.mean())  # 최근 10일 일평균 거래대금
                avg_tv_5d = float(last_5_tv.mean())    # 최근 5일 일평균 거래대금

                results.append(
                    {
                        "code": code,
                        "name": name,
                        "ret_10d": float(ret_10d),
                        "avg_tv_10d": avg_tv_10d,
                        "avg_tv_5d": avg_tv_5d,
                    }
                )

        except Exception:
            # 개별 종목 오류(정지/상폐 등)는 무시
            continue

    if not results:
        return pd.DataFrame()

    df_res = pd.DataFrame(results)
    # 10거래일 수익률 기준 내림차순 정렬
    df_res = df_res.sort_values(by="ret_10d", ascending=False).reset_index(drop=True)

    return df_res.head(top_n)


def main():
    df_up = get_uptrend_stocks(market="ALL", top_n=30)

    if df_up.empty:
        send_message("📭 최근 2주 우상향 조건을 만족하는 종목이 없습니다.")
        return

    # 5열 엑셀 스타일 표 (번호 / 종목명 / 수익률 / 10일평균거래대금 / 5일평균거래대금)
    header = "번호\t종목명\t수익률(10일)\t10일평균거래대금(억)\t5일평균거래대금(억)"
    table_lines = [header]

    for i, row in df_up.iterrows():
        rank = i + 1
        name = row["name"]
        ret_10d_pct = row["ret_10d"] * 100.0

        # 원 → 억 단위
        tv10_eok = row["avg_tv_10d"] / 1e8
        tv5_eok = row["avg_tv_5d"] / 1e8

        line = (
            f"{rank:02d}\t"
            f"{name}\t"
            f"{ret_10d_pct:.2f}%\t"
            f"{tv10_eok:,.1f}\t"
            f"{tv5_eok:,.1f}"
        )
        table_lines.append(line)

    table_text = "\n".join(table_lines)

    message = (
        "📈 *최근 2주 우상향 종목 Top 30*\n"
        "_(조건: 10거래일 수익률>0, 5일선>10일선, 5일선 우상향)_\n\n"
        "```text\n"
        f"{table_text}\n"
        "```"
    )

    send_message(message)


if __name__ == "__main__":
    main()
