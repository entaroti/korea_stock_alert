# netbuy_alert.py
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock


# -----------------------------
# 1. 텔레그램 전송 함수 (디버그 포함)
# -----------------------------
def send_message(message: str):
    """텔레그램으로 메시지를 전송합니다 (디버그 로그 포함)."""
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("CHAT_ID")

    print(f"[DEBUG] TELEGRAM_TOKEN starts with: {str(token)[:10] if token else None}")
    print(f"[DEBUG] CHAT_ID: {chat_id}")

    if not token or not chat_id:
        print("❌ TELEGRAM_TOKEN 또는 CHAT_ID 환경변수가 설정되지 않았습니다.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }
    try:
        resp = requests.post(url, data=payload)
        print(f"[DEBUG] Telegram response status: {resp.status_code}")
        print(f"[DEBUG] Telegram response body: {resp.text}")
        if resp.status_code != 200:
            print("❌ 텔레그램 전송 실패")
        else:
            print("✅ 텔레그램 전송 성공")
    except Exception as e:
        print("❌ 텔레그램 전송 중 예외 발생:", repr(e))


# -----------------------------
# 2. 최근 n영업일 구간 계산
# -----------------------------
def get_recent_trading_window(n_days: int):
    """
    최근 n개 '영업일' 구간의 (start, end) 날짜 문자열(YYYYMMDD) 반환
    - end: 가장 최근 영업일(전일 기준)
    - start: 그로부터 n-1번째 영업일 전
    """
    today = datetime.now().date()
    start_scan = today - timedelta(days=40)  # 여유 있게 40일 조회
    start = start_scan.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    # KOSPI 기준으로 최근 영업일 리스트 가져오기
    tv = stock.get_market_trading_value_by_date(start, end, "KOSPI")
    dates = tv.index  # DatetimeIndex

    if len(dates) < n_days:
        start_date = dates[0]
        end_date = dates[-1]
    else:
        start_date = dates[-n_days]
        end_date = dates[-1]

    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


# -----------------------------
# 3. 투자자별 순매수 (외국인 / 기관 단일)
# -----------------------------
def get_netbuy_df(n_days: int, investor: str) -> pd.DataFrame:
    """
    최근 n영업일 동안 특정 투자자(외국인, 기관합계)의 종목별 순매수 리스트
    - investor: '외국인', '기관합계'
    - 순매수거래대금 > 0 인 종목만
    - KOSPI + KOSDAQ 전체 (market='ALL')
    반환컬럼: ['종목명', '순매수거래대금', '시가총액']
    """
    start, end = get_recent_trading_window(n_days)

    df_all = stock.get_market_net_purchases_of_equities(start, end, "ALL", investor)
    if df_all is None or df_all.empty:
        return pd.DataFrame()

    # 순매수 > 0
    df_all = df_all[df_all["순매수거래대금"] > 0].copy()
    if df_all.empty:
        return pd.DataFrame()

    # 시총 붙이기
    mc = stock.get_market_cap_by_ticker(end, market="ALL")[["시가총액"]]
    df_all = df_all.join(mc, how="left")

    df_all = df_all[["종목명", "순매수거래대금", "시가총액"]].reset_index(drop=True)
    return df_all


# -----------------------------
# 4. 외국인+기관 합산 순매수
# -----------------------------
def get_netbuy_df_combined(n_days: int) -> pd.DataFrame:
    """
    최근 n영업일 동안 '외국인 + 기관합계' 합산 순매수 리스트
    반환컬럼: ['종목명', '합산순매수', '시가총액']
    """
    start, end = get_recent_trading_window(n_days)

    df_f = stock.get_market_net_purchases_of_equities(start, end, "ALL", "외국인")
    df_i = stock.get_market_net_purchases_of_equities(start, end, "ALL", "기관합계")

    if df_f is None:
        df_f = pd.DataFrame()
    if df_i is None:
        df_i = pd.DataFrame()

    if df_f.empty and df_i.empty:
        return pd.DataFrame()

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

    # 합산 순매수 > 0
    df = df[df["합산순매수"] > 0].copy()
    if df.empty:
        return pd.DataFrame()

    mc = stock.get_market_cap_by_ticker(end, market="ALL")[["시가총액"]]
    df = df.join(mc, how="left")

    df = df[["종목명", "합산순매수", "시가총액"]].reset_index(drop=True)
    return df


# -----------------------------
# 5. 한 표(table) 포맷팅
# -----------------------------
def fmt_table(title: str, emoji: str, df: pd.DataFrame, col_net: str) -> str:
    """
    df: ['종목명', col_net, '시가총액']
    col_net: '순매수거래대금' 또는 '합산순매수'
    → 텔레그램에서 보기 좋게 '|' 기반 테이블 문자열 생성
    """
    if df is None or df.empty:
        return f"{emoji} *{title}*\n(해당 조건을 만족하는 종목이 없습니다.)"

    lines = []
    lines.append(f"{emoji} *{title}*")

    header = "| 번호 | 종목명 | 순매수금액(억) | 시가총액(조) |"
    sep    = "|:----:|:--------|---------------:|------------:|"
    lines.append("```text")
    lines.append(header)
    lines.append(sep)

    for i, row in df.iterrows():
        rank = f"{i+1:02d}"
        name = row["종목명"]

        net_eok = row[col_net] / 1e8          # 억 단위
        mc_jo = 0.0
        if not pd.isna(row["시가총액"]):
            mc_jo = row["시가총액"] / 1e12   # 조 단위

        line = (
            f"| {rank} "
            f"| {name} "
            f"| {net_eok:,.1f} "
            f"| {mc_jo:,.2f} |"
        )
        lines.append(line)

    lines.append("```")
    return "\n".join(lines)


# -----------------------------
# 6. 시총 3그룹으로 나누기
# -----------------------------
def split_by_cap3(df: pd.DataFrame, col_net: str, top_n: int = 10):
    """
    시총 기준으로 3그룹으로 나누기
    - 5조 이상
    - 1조 이상 5조 미만
    - 1조 미만
    각 그룹마다 col_net 기준 내림차순 Top N 반환
    """
    if df is None or df.empty:
        empty = (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        return empty

    df = df.copy()

    large = df[df["시가총액"] >= 5e12]                                # 5조 이상
    mid   = df[(df["시가총액"] >= 1e12) & (df["시가총액"] < 5e12)]    # 1~5조
    small = df[df["시가총액"] < 1e12]                                 # 1조 이하

    def sort_top(x: pd.DataFrame):
        if x.empty:
            return x
        return x.sort_values(by=col_net, ascending=False).head(top_n).reset_index(drop=True)

    return sort_top(large), sort_top(mid), sort_top(small)


# -----------------------------
# 7. 그룹별 메시지 빌더
# -----------------------------
def build_group_message(
    cap_title_prefix: str,
    df_f_5: pd.DataFrame,
    df_f_10: pd.DataFrame,
    df_i_5: pd.DataFrame,
    df_i_10: pd.DataFrame,
    df_fi_5: pd.DataFrame,
    df_fi_10: pd.DataFrame,
) -> str:
    """
    하나의 시총 그룹(예: 5조 이상)에 대해
    외국인/기관/외+기 × 5일/10일 → 총 6개 표를 하나의 문자열로 합치기
    """
    blocks = []

    blocks.append(fmt_table(f"{cap_title_prefix} - 외국인 5일 순매수 Top 10", "🌍", df_f_5, "순매수거래대금"))
    blocks.append(fmt_table(f"{cap_title_prefix} - 외국인 10일 순매수 Top 10", "🌍", df_f_10, "순매수거래대금"))

    blocks.append(fmt_table(f"{cap_title_prefix} - 기관 5일 순매수 Top 10", "🏦", df_i_5, "순매수거래대금"))
    blocks.append(fmt_table(f"{cap_title_prefix} - 기관 10일 순매수 Top 10", "🏦", df_i_10, "순매수거래대금"))

    blocks.append(fmt_table(f"{cap_title_prefix} - 외국인+기관 5일 순매수 Top 10", "🤝", df_fi_5, "합산순매수"))
    blocks.append(fmt_table(f"{cap_title_prefix} - 외국인+기관 10일 순매수 Top 10", "🤝", df_fi_10, "합산순매수"))

    return "\n\n".join(blocks)


# -----------------------------
# 8. 메인 로직
# -----------------------------
def main():
    # 1) 외국인 / 기관 / 외+기, 5일·10일 전체 데이터
    df_f_5 = get_netbuy_df(5, "외국인")
    df_f_10 = get_netbuy_df(10, "외국인")
    df_i_5 = get_netbuy_df(5, "기관합계")
    df_i_10 = get_netbuy_df(10, "기관합계")
    df_fi_5 = get_netbuy_df_combined(5)
    df_fi_10 = get_netbuy_df_combined(10)

    # 2) 시총별 3그룹 분리 (5조↑ / 1~5조 / 1조↓)
    f5_big,  f5_mid,  f5_small  = split_by_cap3(df_f_5,  "순매수거래대금", top_n=10)
    f10_big, f10_mid, f10_small = split_by_cap3(df_f_10, "순매수거래대금", top_n=10)

    i5_big,  i5_mid,  i5_small  = split_by_cap3(df_i_5,  "순매수거래대금", top_n=10)
    i10_big, i10_mid, i10_small = split_by_cap3(df_i_10, "순매수거래대금", top_n=10)

    fi5_big,  fi5_mid,  fi5_small  = split_by_cap3(df_fi_5,  "합산순매수", top_n=10)
    fi10_big, fi10_mid, fi10_small = split_by_cap3(df_fi_10, "합산순매수", top_n=10)

    # 3) 시총 그룹별 메시지(3개) 생성
    msg_5jo = build_group_message(
        "시총 5조 이상",
        f5_big, f10_big,
        i5_big, i10_big,
        fi5_big, fi10_big,
    )

    msg_1to5 = build_group_message(
        "시총 1~5조",
        f5_mid, f10_mid,
        i5_mid, i10_mid,
        fi5_mid, fi10_mid,
    )

    msg_under1 = build_group_message(
        "시총 1조 이하",
        f5_small, f10_small,
        i5_small, i10_small,
        fi5_small, fi10_small,
    )

    # 4) 각 시총 그룹별로 따로 텔레그램 전송 (총 3개 메시지)
    for idx, msg in enumerate([msg_5jo, msg_1to5, msg_under1], start=1):
        print(f"[DEBUG] Sending group message {idx}/3")
        send_message(msg)


if __name__ == "__main__":
    main()
