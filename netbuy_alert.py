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
        data={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
        },
    )


def collect_netbuy_data(top_n: int = 10):
    """
    최근 5/10거래일 기준으로
    - 외국인 순매수 상위
    - 기관 순매수 상위
    - 외국인+기관 순매수 상위
    를 계산해서 DataFrame 6개를 반환합니다.
    """

    today = datetime.now().date()
    start_date = today - timedelta(days=30)  # 여유 있게 30일치
    start = start_date.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    today_str = today.strftime("%Y%m%d")

    # 시가총액: 오늘 기준 KOSPI + KOSDAQ
    mc_kospi = stock.get_market_cap_by_ticker(today_str, market="KOSPI")
    mc_kosdaq = stock.get_market_cap_by_ticker(today_str, market="KOSDAQ")
    mc_all = pd.concat([mc_kospi, mc_kosdaq])
    # mc_all.columns 예: ['시가총액', '상장주식수', ...]
    mc_all = mc_all[["시가총액"]]

    # 종목 리스트는 시총 데이터 기준으로
    tickers = mc_all.index.tolist()

    # 결과 저장용 리스트
    f_5_list = []
    f_10_list = []
    i_5_list = []
    i_10_list = []
    fi_5_list = []
    fi_10_list = []

    for code in tickers:
        try:
            # 최근 30일 동안 해당 종목의 투자자별 거래대금(순매수) 조회
            # detail=True → 투자자 구분 컬럼(개인, 외국인, 기관합계 등)
            tv_df = stock.get_market_trading_value_by_date(start, end, code, detail=True)

            if tv_df is None or tv_df.empty:
                continue

            tv_df = tv_df.fillna(0)

            # pykrx 기준 컬럼 이름 가정: '외국인', '기관합계'
            if ("외국인" not in tv_df.columns) or ("기관합계" not in tv_df.columns):
                continue

            # 최소 5거래일 이상만 사용
            if tv_df.shape[0] < 5:
                continue

            last_5 = tv_df.tail(5)
            # 10일은 여유 있으면 계산
            last_10 = tv_df.tail(10) if tv_df.shape[0] >= 10 else None

            # 5일 순매수
            f_5 = last_5["외국인"].sum()
            i_5 = last_5["기관합계"].sum()
            fi_5 = f_5 + i_5

            # 10일 순매수
            f_10 = last_10["외국인"].sum() if last_10 is not None else 0.0
            i_10 = last_10["기관합계"].sum() if last_10 is not None else 0.0
            fi_10 = f_10 + i_10

            name = stock.get_market_ticker_name(code)
            mktcap = float(mc_all.loc[code, "시가총액"]) if code in mc_all.index else 0.0

            # 순매수 > 0 인 종목만
            if f_5 > 0:
                f_5_list.append(
                    {"code": code, "name": name, "net": float(f_5), "mktcap": mktcap}
                )
            if f_10 > 0:
                f_10_list.append(
                    {"code": code, "name": name, "net": float(f_10), "mktcap": mktcap}
                )
            if i_5 > 0:
                i_5_list.append(
                    {"code": code, "name": name, "net": float(i_5), "mktcap": mktcap}
                )
            if i_10 > 0:
                i_10_list.append(
                    {"code": code, "name": name, "net": float(i_10), "mktcap": mktcap}
                )
            if fi_5 > 0:
                fi_5_list.append(
                    {"code": code, "name": name, "net": float(fi_5), "mktcap": mktcap}
                )
            if fi_10 > 0:
                fi_10_list.append(
                    {"code": code, "name": name, "net": float(fi_10), "mktcap": mktcap}
                )

        except Exception:
            # 개별 종목 에러(정지/상폐 등)는 무시
            continue

    def to_sorted_df(lst):
        if not lst:
            return pd.DataFrame()
        df = pd.DataFrame(lst)
        df = df.sort_values(by="net", ascending=False).reset_index(drop=True)
        return df.head(top_n)

    df_f_5 = to_sorted_df(f_5_list)
    df_f_10 = to_sorted_df(f_10_list)
    df_i_5 = to_sorted_df(i_5_list)
    df_i_10 = to_sorted_df(i_10_list)
    df_fi_5 = to_sorted_df(fi_5_list)
    df_fi_10 = to_sorted_df(fi_10_list)

    return df_f_5, df_f_10, df_i_5, df_i_10, df_fi_5, df_fi_10


def make_table_block(title: str, emoji: str, df: pd.DataFrame) -> str:
    """
    df: columns = ['code', 'name', 'net', 'mktcap']
    net: 원 단위 순매수금액 → 억 단위로 변환
    mktcap: 원 단위 시총 → 조 단위로 변환
    """
    if df is None or df.empty:
        return f"{emoji} *{title}*\n(해당 조건을 만족하는 종목이 없습니다.)"

    header = "번호\t종목명\t순매수금액(억)\t시가총액(조)"
    lines = [f"{emoji} *{title}*", "```text", header]

    for i, row in df.iterrows():
        rank = i + 1
        name = row["name"]

        net_eok = row["net"] / 1e8          # 억 단위
        mc_jo = row["mktcap"] / 1e12 if row["mktcap"] > 0 else 0.0  # 조 단위

        line = (
            f"{rank:02d}\t"
            f"{name}\t"
            f"{net_eok:,.1f}\t"
            f"{mc_jo:,.2f}"
        )
        lines.append(line)

    lines.append("```")
    return "\n".join(lines)


def main():
    (
        df_f_5,
        df_f_10,
        df_i_5,
        df_i_10,
        df_fi_5,
        df_fi_10,
    ) = collect_netbuy_data(top_n=10)  # 필요하면 20으로 늘려도 됨

    blocks = []

    blocks.append(
        make_table_block("외국인 5일 순매수 Top 10", "🌍", df_f_5)
    )
    blocks.append(
        make_table_block("외국인 10일 순매수 Top 10", "🌍", df_f_10)
    )
    blocks.append(
        make_table_block("기관 5일 순매수 Top 10", "🏦", df_i_5)
    )
    blocks.append(
        make_table_block("기관 10일 순매수 Top 10", "🏦", df_i_10)
    )
    blocks.append(
        make_table_block("외국인+기관 5일 순매수 Top 10", "🤝", df_fi_5)
    )
    blocks.append(
        make_table_block("외국인+기관 10일 순매수 Top 10", "🤝", df_fi_10)
    )

    message = "\n\n".join(blocks)
    send_message(message)


if __name__ == "__main__":
    main()
