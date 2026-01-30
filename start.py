import time
import requests
import json
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta

# ==========================================
# 1. 설정값 (사용자 설정)
# ==========================================
# 사용자님이 제공하신 웹후크 URL
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

TARGET_DATE = datetime.now().strftime("%Y%m%d") # 오늘 날짜
# TARGET_DATE = "20260130" # 테스트 시 날짜 고정 가능

# [눌림목 기술적 조건]
MA_WINDOW = 20           # 20일 이동평균선 기준
MIN_DISPARITY = 100.0    # 20일선 지지 (최소 100% 이상)
MAX_DISPARITY = 105.0    # 20일선 살짝 위 (최대 105% 이하)
VOL_DROP_RATE = 0.7      # 거래량 급감 기준 (전일 거래량의 70% 이하)

# [수급 조건]
SUPPLY_CHECK_DAYS = 5    # 최근 5일 수급 합계

print(f"[{TARGET_DATE}] 기준, '거래량 급감 + 20일선 눌림목' 분석 및 디스코드 전송을 시작합니다...")
print("-" * 60)

# ==========================================
# 2. 함수 정의
# ==========================================
def send_discord_message(webhook_url, content):
    """디스코드로 메시지를 전송하는 함수"""
    data = {"content": content}
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(webhook_url, data=json.dumps(data), headers=headers)
        if response.status_code == 204:
            print("✅ 디스코드 메시지 전송 성공!")
        else:
            print(f"❌ 전송 실패: {response.status_code}")
    except Exception as e:
        print(f"❌ 전송 중 에러 발생: {e}")

def get_profitable_tickers(date):
    """PER > 0 인 종목만 가져오기 (적자 기업 1차 필터링)"""
    df = stock.get_market_fundamental_by_ticker(date, market="ALL")
    filtered_df = df[df['PER'] > 0] 
    return filtered_df.index.tolist()

# ==========================================
# 3. 메인 로직 실행
# ==========================================
print("1. 흑자 기업(PER > 0) 필터링 중...")
tickers = get_profitable_tickers(TARGET_DATE)
print(f"   -> 대상 종목: {len(tickers)}개")

results = []
print("2. 차트(거래량 급감) 및 수급 분석 시작...")

count = 0
for ticker in tickers:
    count += 1
    if count % 100 == 0:
        print(f"   ... {count}개 분석 중")

    try:
        name = stock.get_market_ticker_name(ticker)

        # A. 차트 데이터 (최근 60일)
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv_by_date(start_date, TARGET_DATE, ticker)
        
        if len(ohlcv) < MA_WINDOW + 1:
            continue

        curr_close = ohlcv['종가'].iloc[-1]
        prev_close = ohlcv['종가'].iloc[-2]
        curr_vol = ohlcv['거래량'].iloc[-1]
        prev_vol = ohlcv['거래량'].iloc[-2]

        # B. 핵심 조건 체크
        # [조건 1] 주가 하락/보합 (상승 제외)
        if curr_close > prev_close:
            continue

        # [조건 2] 거래량 급감 (어제 거래량의 70% 이하)
        if curr_vol > (prev_vol * VOL_DROP_RATE):
            continue 

        # [조건 3] 20일선 눌림목 (이격도 100~105%)
        ma20 = ohlcv['종가'].rolling(window=MA_WINDOW).mean().iloc[-1]
        disparity = (curr_close / ma20) * 100

        if not (MIN_DISPARITY <= disparity <= MAX_DISPARITY):
            continue

        # C. 수급 체크 (기관/외국인 5일 누적)
        supply_start = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d") 
        supply_df = stock.get_market_net_purchases_of_equities_by_date(supply_start, TARGET_DATE, ticker)
        recent_supply = supply_df.tail(SUPPLY_CHECK_DAYS)
        
        inst_sum = int(recent_supply['기관합계'].sum())
        for_sum = int(recent_supply['외국인'].sum())

        # 기관이나 외국인 중 하나라도 순매수면 OK
        if inst_sum <= 0 and for_sum <= 0:
            continue

        # D. 결과 저장
        vol_change_rate = round((curr_vol - prev_vol) / prev_vol * 100, 1) # 예: -50.5
        
        results.append({
            '종목명': name,
            '현재가': curr_close,
            '이격도': round(disparity, 1),
            '거래량변동': f"{vol_change_rate}%",
            '기관수급': inst_sum,
            '외인수급': for_sum
        })

    except:
        continue

# ==========================================
# 4. 결과 정리 및 디스코드 전송
# ==========================================
print("\n" + "="*70)
print(f"📊 분석 완료. 디스코드로 결과를 전송합니다.")
print("="*70)

if len(results) > 0:
    res_df = pd.DataFrame(results)
    # 이격도 낮은 순 정렬 (지지선에 가까운 순)
    res_df = res_df.sort_values(by='이격도', ascending=True)

    # --- 디스코드 메시지 작성 ---
    discord_msg = f"## 🚀 {TARGET_DATE} 눌림목(20일선) 발굴 종목\n"
    discord_msg += f"**조건:** 흑자기업 | 20일선 지지 | 거래량급감({int(VOL_DROP_RATE*100)}%이하) | 수급유입\n\n"
    
    # 상위 10개만 전송 (너무 길면 잘릴 수 있음)
    for idx, row in res_df.head(10).iterrows():
        # 이모지: 기관수급이 좋으면 🔴, 외인수급이 좋으면 🔵
        icon = "🛡️"
        if row['기관수급'] > 0 and row['외인수급'] > 0: icon = "🔥(양매수)"
        elif row['기관수급'] > 0: icon = "🔴(기관)"
        elif row['외인수급'] > 0: icon = "🔵(외인)"

        discord_msg += (
            f"**{idx+1}. {row['종목명']}** {icon}\n"
            f"> 가격: {row['현재가']:,}원 (이격도 {row['이격도']}%)\n"
            f"> 거래량: {row['거래량변동']} 📉\n"
            f"> 수급(5일): 기 {row['기관수급']:,} / 외 {row['외인수급']:,}\n\n"
        )
    
    if len(res_df) > 10:
        discord_msg += f"\n*외 {len(res_df)-10}개 종목이 더 검색되었습니다.*"

    # 메시지 전송
    send_discord_message(DISCORD_WEBHOOK_URL, discord_msg)

else:
    # 검색된 종목이 없을 때도 알림
    msg = f"## 📉 {TARGET_DATE} 분석 결과\n조건에 맞는 '눌림목' 종목이 없습니다.\n(시장이 너무 강해서 조정이 없거나, 거래량이 안 줄었습니다.)"
    send_discord_message(DISCORD_WEBHOOK_URL, msg)

print("모든 작업이 완료되었습니다.")
