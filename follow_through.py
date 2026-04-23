# follow_through.py

import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import os
import requests

DATA_FILE = "candidates_yesterday.csv"


# -----------------------------
# 텔레그램
# -----------------------------
def send_message(message):
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("CHAT_ID")

    if not token or not chat_id:
        print(message)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, data={
        "chat_id": chat_id,
        "text": message[:4000],
        "parse_mode": "Markdown"
    })


# -----------------------------
# Step 1: 어제 후보 저장
# -----------------------------
def save_today_candidates():
    today = datetime.now().date()
    start = today - timedelta(days=5)

    kospi = fdr.StockListing("KOSPI")
    kosdaq = fdr.StockListing("KOSDAQ")

    results = []

    for market in [kospi, kosdaq]:
        for _, row in market.iterrows():
            try:
                code = row["Code"]
                name = row["Name"]

                df = fdr.DataReader(code, start, today)

                if df is None or len(df) < 3:
                    continue

                df["Value"] = df["Close"] * df["Volume"]

                v_d2 = df["Value"].iloc[-3]
                v_d1 = df["Value"].iloc[-2]

                if v_d2 == 0:
                    continue

                if v_d1 >= v_d2 * 5:
                    results.append({
                        "Code": code,
                        "Name": name,
                        "Close_yesterday": df["Close"].iloc[-2]
                    })

            except:
                continue

    df = pd.DataFrame(results)
    df.to_csv(DATA_FILE, index=False, encoding="utf-8-sig")

    send_message(f"📥 어제 후보 저장 완료: {len(df)}개")


# -----------------------------
# Step 2: 오늘 이어지는지 확인
# -----------------------------
def check_follow_through():
    try:
        df = pd.read_csv(DATA_FILE)
    except:
        send_message("❌ 어제 데이터 없음")
        return

    today = datetime.now().date()
    start = today - timedelta(days=3)

    results = []

    for _, row in df.iterrows():
        try:
            code = row["Code"]
            name = row["Name"]
            prev_close = row["Close_yesterday"]

            df_price = fdr.DataReader(code, start, today)

            if df_price is None or len(df_price) < 1:
                continue

            today_close = df_price["Close"].iloc[-1]

            if today_close > prev_close:
                change = (today_close / prev_close - 1) * 100

                results.append({
                    "종목명": name,
                    "수익률": change
                })

        except:
            continue

    if not results:
        send_message("📭 이어지는 종목 없음")
        return

    df_res = pd.DataFrame(results).sort_values(by="수익률", ascending=False)

    # 출력
    lines = []
    lines.append("📊 어제 급증 → 오늘 상승 종목")
    lines.append("```text")
    lines.append("| 번호 | 종목명 | 수익률(%) |")
    lines.append("|:----:|:--------|----------:|")

    for i, row in df_res.head(20).iterrows():
        lines.append(
            f"| {i+1:02d} | {row['종목명']} | {row['수익률']:.2f} |"
        )

    lines.append("```")

    send_message("\n".join(lines))


# -----------------------------
# 메인
# -----------------------------
def main():
    now = datetime.now()

    # 오전 3시 이전 → 저장
    if now.hour < 3:
        save_today_candidates()
    else:
        check_follow_through()


if __name__ == "__main__":
    main()
