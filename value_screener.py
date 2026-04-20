# value_screener.py

import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import os
import requests
import traceback


# -----------------------------
# 텔레그램 전송
# -----------------------------
def send_message(message):
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("CHAT_ID")

    if not token or not chat_id:
        print(message)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message[:4000],
        "parse_mode": "Markdown"
    }

    try:
        requests.post(url, data=payload)
    except:
        pass


# -----------------------------
# 데이터 수집 (전체 종목)
# -----------------------------
def get_candidates():
    today = datetime.now().date()
    start = today - timedelta(days=10)

    kospi = fdr.StockListing("KOSPI")
    kosdaq = fdr.StockListing("KOSDAQ")

    results = []

    for market in [kospi, kosdaq]:
        for _, row in market.iterrows():
            try:
                code = row["Code"]
                name = row["Name"]
                marcap = row["Marcap"] if "Marcap" in row else 0

                df = fdr.DataReader(code, start, today)

                if df is None or len(df) < 5:
                    continue

                df["Value"] = df["Close"] * df["Volume"]

                avg_value = df["Value"].iloc[:-1].mean()
                today_value = df["Value"].iloc[-1]

                if avg_value == 0:
                    continue

                if today_value > avg_value * 3:
                    change = (df["Close"].iloc[-1] / df["Close"].iloc[0] - 1) * 100

                    results.append({
                        "종목명": name,
                        "거래대금": today_value,
                        "수익률": change,
                        "시가총액": marcap
                    })

            except:
                continue

    return pd.DataFrame(results)


# -----------------------------
# 시총 분류
# -----------------------------
def split_by_cap4(df):
    g1 = df[df["시가총액"] < 2e11]
    g2 = df[(df["시가총액"] >= 2e11) & (df["시가총액"] < 5e11)]
    g3 = df[(df["시가총액"] >= 5e11) & (df["시가총액"] < 1e12)]
    g4 = df[df["시가총액"] >= 1e12]

    return g1, g2, g3, g4


def get_top(df):
    if df.empty:
        return df
    return df.sort_values(by="거래대금", ascending=False).head(10).reset_index(drop=True)


def format_table(title, df):
    if df.empty:
        return f"📭 {title}\n(조건 만족 종목 없음)"

    lines = []
    lines.append(f"📊 {title}")
    lines.append("```text")
    lines.append("| 번호 | 종목명 | 거래대금(억) | 수익률(%) | 시총(조) |")
    lines.append("|:----:|:--------|-------------:|----------:|---------:|")

    for i, row in df.iterrows():
        value_eok = row["거래대금"] / 1e8
        cap_jo = row["시가총액"] / 1e12 if row["시가총액"] else 0

        lines.append(
            f"| {i+1:02d} | {row['종목명']} | {value_eok:,.1f} | {row['수익률']:,.2f} | {cap_jo:,.2f} |"
        )

    lines.append("```")
    return "\n".join(lines)


# -----------------------------
# 메인
# -----------------------------
def main():
    try:
        send_message("🚀 Value Screener 시작")

        df = get_candidates()

        if df.empty:
            send_message("📭 조건 만족 종목 없음")
            return

        g1, g2, g3, g4 = split_by_cap4(df)

        groups = [
            ("시총 2천억 미만", g1),
            ("시총 2천억~5천억", g2),
            ("시총 5천억~1조", g3),
            ("시총 1조 이상", g4),
        ]

        for title, g in groups:
            send_message(format_table(title, get_top(g)))

    except Exception:
        err = traceback.format_exc()
        send_message(f"❌ 에러 발생\n{err[:3000]}")


if __name__ == "__main__":
    main()
