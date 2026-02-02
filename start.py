import FinanceDataReader as fdr
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import sys
from pykrx import stock   # ✅ 추가

# ==========================================
# 0. 사용자 설정
# ==========================================
IGYEOK_WEBHOOK_URL = "https://discord.com/api/webhooks/1461902939139604684/ZdCdITanTb3sotd8LlCYlJzSYkVLduAsjC6CD2h26X56wXoQRw7NY72kTNzxTI6UE4Pi"

# [한국 시간 설정]
KST_TIMEZONE = timezone(timedelta(hours=9))
CURRENT_KST = datetime.now(KST_TIMEZONE)
TARGET_DATE = CURRENT_KST.strftime("%Y-%m-%d")     # FDR용
TARGET_DATE_KRX = CURRENT_KST.strftime("%Y%m%d")   # pykrx용

# ==========================================
# 1. 공통 함수
# ==========================================
def send_discord_message(content):
    """디스코드 메시지 전송 함수"""
    try:
        data = {'content': content}
        requests.post(IGYEOK_WEBHOOK_URL, json=data)
    except Exception as e:
        print(f"디스코드 전송 실패: {e}")

def get_credit_ratio(code, date):
    """종목 신용잔고 비율 조회"""
    try:
        df = stock.get_market_credit_balance(date, date, code)
        if df.empty:
            return None
        return round(df['신용잔고비율'].iloc[-1], 2)
    except:
        return None

# ==========================================
# 2. 메인 로직
# ==========================================
def main():
    print(f"[{TARGET_DATE}] 프로그램 시작 (한국 시간 기준)")

    # ---------------------------------------------------------
    # 휴장일 체크
    # ---------------------------------------------------------
    weekday = CURRENT_KST.weekday()
    if weekday >= 5:
        day_name = "토요일" if weekday == 5 else "일요일"
        msg = f"⏹️ 오늘은 주말({day_name})이라 주식장이 열리지 않습니다."
        print(msg)
        send_discord_message(msg)
        sys.exit()

    try:
        check_market = fdr.DataReader('KS11', TARGET_DATE, TARGET_DATE)
        if check_market.empty:
            msg = f"⏹️ 오늘은 공휴일(장 휴무)이라 주식장이 열리지 않습니다."
            print(msg)
            send_discord_message(msg)
            sys.exit()
    except Exception as e:
        msg = f"⚠️ 장 운영 여부 확인 실패 ({e}). 프로그램 종료"
        print(msg)
        send_discord_message(msg)
        sys.exit()

    print("✅ 정상 개장일입니다. 분석 시작")

    # ---------------------------------------------------------
    # 이격도 분석
    # ---------------------------------------------------------
    print("🚀 [1단계] 계단식 이격도 분석 시작")

    try:
        df_kospi = fdr.StockListing('KOSPI').head(500)
        df_kosdaq = fdr.StockListing('KOSDAQ').head(1000)
        df_total = pd.concat([df_kospi, df_kosdaq])

        all_analyzed = []
        print(f"📡 총 {len(df_total)}개 종목 분석 중...")

        for _, row in df_total.iterrows():
            code = row['Code']
            name = row['Name']

            try:
                df = fdr.DataReader(code).tail(30)
                if len(df) < 20:
                    continue

                current_price = df['Close'].iloc[-1]
                ma20 = df['Close'].rolling(20).mean().iloc[-1]
                if ma20 == 0 or pd.isna(ma20):
                    continue

                disparity = round((current_price / ma20) * 100, 1)

                # ✅ 신용잔고 비율
                credit_ratio = get_credit_ratio(code, TARGET_DATE_KRX)

                all_analyzed.append({
                    'name': name,
                    'code': code,
                    'disparity': disparity,
                    'credit': credit_ratio
                })

            except:
                continue

        # ---------------------------------------------------------
        # 계단식 필터링
        # ---------------------------------------------------------
        results = [r for r in all_analyzed if r['disparity'] <= 93.0]
        filter_level = "이격도 93% 이하 (초과대낙폭)"

        if not results:
            results = [r for r in all_analyzed if r['disparity'] <= 95.0]
            filter_level = "이격도 95% 이하 (일반낙폭)"

        # ---------------------------------------------------------
        # 결과 출력
        # ---------------------------------------------------------
        if results:
            results = sorted(results, key=lambda x: x['disparity'])

            report = f"### 📊 종목 분석 결과 ({filter_level})\n"
            for r in results[:50]:
                credit_txt = f"{r['credit']}%" if r['credit'] is not None else "N/A"
                report += (
                    f"· **{r['name']}({r['code']})** "
                    f": 이격도 {r['disparity']}% | 신용잔고 {credit_txt}\n"
                )

            report += "\n" + "=" * 30 + "\n"
            report += "📝 **[Check List]**\n"
            report += "1. 영업이익 적자기업 제외하고 테마별 분류\n"
            report += "2. 기관/외국인/연기금 수급 분석\n"
            report += "3. 최근 뉴스 및 목표주가 확인\n"
            report += "4. 종합 판단 후 최종 종목 선정\n"

            send_discord_message(report)

            with open("targets.txt", "w", encoding="utf-8") as f:
                f.write("\n".join([f"{r['code']},{r['name']}" for r in results]))

            print(f"✅ {len(results)}개 종목 추출 완료")

        else:
            msg = "🔍 조건에 맞는 종목이 없습니다."
            print(msg)
            send_discord_message(msg)

    except Exception as e:
        err_msg = f"❌ 에러 발생: {e}"
        print(err_msg)
        send_discord_message(err_msg)

if __name__ == "__main__":
    main()
