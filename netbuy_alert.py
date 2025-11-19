# netbuy_alert.py
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
        data={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
    )


def get_recent_trading_window(n_days: int):
    """최근 n개 '영업일' 구간의 (start, end) 날짜 문자열(YYYYMMDD) 반환"""
    today = datetime.now().date()
    start_scan = today - timedelta(days=40)  # 여유 있게 40일 조회
    start = start_scan.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    # KOSPI 기준으로 최근 영업일 리스트 가져오기
    tv = stock.get_market_trading_value_by_date(start, end, "KOSPI")
    dates = tv.index  # DatetimeIndex

    if len(dates) < n_days:
        # 데이터가 너무 적으면 전체를 그냥 사용
        start_date = dates[0]
        end_date = dates[-1]
    else:
        start_date = dates[-n_days]
        end_date = dates[-1]

    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def get_netbuy_df(n_days: int, investor: str, top_n: int = 10) -> pd.DataFrame:
    """
    최근 n영업일 동안 특정 투자자의 종목별 순매수 상위 리스트 반환
    - investor: '외국인', '기관합계' 등
    - 순매수거래대금 > 0 인 종목만
    - KOSPI + KOSDAQ 전체 (market='ALL')
    """
    start, end = get_recent_trading_window(n_days)

    # KOSPI + KOSDAQ 한 번에
    df_all = stock.get_market_net_purchases_of_equities(start, end, "ALL", investor)
    if df_all is None or df_all.empty:
        return pd.DataFrame()

    # 순매수거래대금 > 0 인 종목만
    df_all = df_all[df_all["순매수거래대금"] > 0].copy()
    if df_all.empty:
        return pd.DataFrame()

    # 시가총액 붙이기 (end 기준)
    mc = stock.get_market_cap_by_ticker(end, market="ALL")[["시가총액"]]
    df_all = df_all.join(mc, how="left")

    # 정렬 및 상위 N개
    df_all = df_all.sort_values(by="순매수거래대금", ascending=False).head(top_n)

    # 인덱스(티커)는 여기선 안 쓰니 종목명, 순매수거래대금, 시가총액만 사용
    df_all = df_all[["종목명", "순매수거래대금", "시가총액"]].reset_index(drop=True)
    return df_all


def get_netbuy_df_combined(n_days: int, top_n: int = 10) -> pd.DataFrame:
    """
    최근 n영업일 동안 '외국인 + 기관합계' 순매수 상위 리스트
    """
    start, end = get_recent_trading_window(n_days)

    df_f = stock.get_market_net_purchases_of_equities(start, end, "ALL", "외국인")
    df_i = stock.get_market_net_purchases_of_equities(start, end, "ALL", "기관합계")

    if df_f is None:
        df_f = pd.DataFrame()
    if df_i is None:
        df_i = pd.DataFrame()

    # 없으면 빈 df
    if df_f.empty and df_i.empty:
        return pd.DataFrame()

    # 두 df를 티커 기준으로 outer join
    df = pd.DataFrame()

    if not df_f.empty:
        df["종목명"] = df_f["종목명"]
        df["외국인순매수"] = df_f["순매수거래대금"]
    if not df_i.empty:
        if "종목명" not in df.columns:
            df["종목명"] = df_i["종목명"]
        df["기관순매수"] = df_i["순매수거래대금"]

    df["외국인순매수"] = df.get("외국인순매수", 0).fillna(0)
    df["기관순매수"] = df.get("기관순매수", 0).fillna(0)
    df["합산순매수"] = df["외국인순매수"] + df["기관순매수"]

    # 합산 순매수 > 0 인 종목만
    df = df[df["합산순매수"] > 0].copy()
    if df.empty:
        return pd.DataFrame()

    # 시가총액 붙이기
    mc = stock.get_market_cap_by_ticker(end, market="ALL")[["시가총액"]]
    df = df.join(mc, how="left")

    df = df.sort_values(by="합산순매수", ascending=False).head(top_n)
    df = df[["종목명", "합산순매수", "시가총액"]].reset_index(drop=True)
    return df


def make_table_block(title: str, emoji: str, df: pd.DataFrame, col_net: str) -> str:
    """
    df: ['종목명', col_net, '시가총액']
    col_net: '순매수거래대금' or '합산순매수'
    """
    if df is None or df.empty:
        return f"{emoji} *{title}*\n(해당 조건을 만족하는 종목이 없습니다.)"

    header = "번호\t종목명\t순매수금액(억)\t시가총액(조)"
    lines = [f"{emoji} *{title}*", "```text", header]

    for i, row in df.iterrows():
        rank = i + 1
        name = row["종목명"]
        net_eok = row[col_net] / 1e8              # 억 단위
        mc_jo = 0.0
        if not pd.isna(row["시가총액"]):
            mc_jo = row["시가총액"] / 1e12       # 조 단위

        line = f"{rank:02d}\t{name}\t{net_eok:,.1f}\t{mc_jo:,.2f}"
        lines.append(line)

    lines.append("```")
    return "\n".join(lines)


def main():
    # 외국인/기관 5일, 10일 순매수
    df_f_5 = get_netbuy_df(5, "외국인", top_n=10)
    df_f_10 = get_netbuy_df(10, "외국인", top_n=10)
    df_i_5 = get_netbuy_df(5, "기관합계", top_n=10)
    df_i_10 = get_netbuy_df(10, "기관합계", top_n=10)

    df_fi_5 = get_netbuy_df_combined(5, top_n=10)
    df_fi_10 = get_netbuy_df_combined(10, top_n=10)

    blocks = []
    blocks.append(make_table_block("외국인 5일 순매수 Top 10", "🌍", df_f_5, "순매수거래대금"))
    blocks.append(make_table_block("외국인 10일 순매수 Top 10", "🌍", df_f_10, "순매수거래대금"))
    blocks.append(make_table_block("기관 5일 순매수 Top 10", "🏦", df_i_5, "순매수거래대금"))
    blocks.append(make_table_block("기관 10일 순매수 Top 10", "🏦", df_i_10, "순매수거래대금"))
    blocks.append(make_table_block("외국인+기관 5일 순매수 Top 10", "🤝", df_fi_5, "합산순매수"))
    blocks.append(make_table_block("외국인+기관 10일 순매수 Top 10", "🤝", df_fi_10, "합산순매수"))

    message = "\n\n".join(blocks)
    send_message(message)


if __name__ == "__main__":
    main()
