import requests
import json
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time

# ==========================================
# 1. 사용자 설정 (건들지 마세요!)
# ==========================================
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

TARGET_DATE = datetime.now().strftime("%Y%m%d") # 오늘 날짜

# [눌림목 조건]
MA_WINDOW = 20           # 20일 이동평균선
MIN_DISPARITY = 90.0    # 이격도 최소 (20일선 지지)
MAX_DISPARITY = 105.0    # 이격도 최대 (20일선 살짝 위)
VOL_DROP_RATE = 0.9      # 거래량 급감 (전일 대비 70% 이하)

# [수급 조건]
SUPPLY_CHECK_DAYS = 5    # 최근 5일 수급 합계

print(f"[{TARGET_DATE}] 시가총액 상위 1000개(코스피500+코스닥500) 눌림목 분석 시작!")
print("-" * 60)

# ==========================================
# 2. 함수 정의
# ==========================================
def send_discord_message(webhook_url, content):
    data = {"content": content}
    headers = {"Content-Type": "application/json"}
    try:
        requests.post(webhook_url, data=json.dumps(data), headers=headers)
    except:
        pass

def get_top_market_cap_tickers(date):
    """코스피/코스닥 시총 상위 500개씩 가져오기 (ETF 제외)"""
    print("1. 시가총액 상위 종목 리스트를 가져오는 중...")
    
    # 1) 코스피 상위 500개
    df_kospi = stock.get_market_cap(date, market="KOSPI")
    top_kospi = df_kospi.sort_values(by='시가총액', ascending=False).head(500).index.tolist()
    
    # 2) 코스닥 상위 500개
    df_kosdaq = stock.get_market_cap(date, market="KOSDAQ")
    top_kosdaq = df_kosdaq.sort_values(by='시가총액', ascending=False).head(500).index.tolist()
    
    # 3) 합치기
    total_tickers = top_kospi + top_kosdaq
    
    # 4) ETF, ETN 제외하기 (중요!)
    etfs = stock.get_etf_ticker_list(date)
    etns = stock.get_etn_ticker_list(date)
    exclude_list = set(etfs + etns)
    
    final_tickers = [t for t in total_tickers if t not in exclude_list]
    
    return final_tickers

# ==========================================
# 3. 메인 로직 실행
# ==========================================
tickers = get_top_market_cap_tickers(TARGET_DATE)
print(f"   -> 분석 대상: 총 {len(tickers)}개 우량주 (ETF 제외됨)")

results = []
print("2. 차트 및 수급 분석 시작 (진행률 표시)...")

count = 0
total_len = len(tickers)

for ticker in tickers:
    count += 1
    if count % 50 == 0: # 50개마다 진행상황 알려줌
        print(f"   ... {count}/{total_len} 완료 ({round(count/total_len*100)}%)")

    try:
        # A. 차트 데이터 (최근 60일)
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv_by_date(start_date, TARGET_DATE, ticker)
        
        if len(ohlcv) < MA_WINDOW + 1:
            continue

        curr_close = ohlcv['종가'].iloc[-1]
        prev_close = ohlcv['종가'].iloc[-2]
        curr_vol = ohlcv['거래량'].iloc[-1]
        prev_vol = ohlcv['거래량'].iloc[-2]

        # B. 조건 체크
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

        if inst_sum <= 0 and for_sum <= 0:
            continue

        # D. 저장
        name = stock.get_market_ticker_name(ticker)
        vol_change_rate = round((curr_vol - prev_vol) / prev_vol * 100, 1)
        
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
# 4. 디스코드 전송
# ==========================================
print("\n" + "="*70)
print(f"📊 분석 완료. 디스코드로 전송합니다.")

if len(results) > 0:
    res_df = pd.DataFrame(results)
    res_df = res_df.sort_values(by='이격도', ascending=True)

    discord_msg = f"## 🚀 {TARGET_DATE} 시총상위 눌림목 발굴\n"
    discord_msg += f"**대상:** 코스피/닥 상위 1000개 | **조건:** 20일선 지지 + 거래량급감\n\n"
    
    # 상위 15개 전송
    for idx, row in res_df.head(15).iterrows():
        icon = "🛡️"
        if row['기관수급'] > 0 and row['외인수급'] > 0: icon = "🔥"
        elif row['기관수급'] > 0: icon = "🔴"
        elif row['외인수급'] > 0: icon = "🔵"

        discord_msg += (
            f"**{row['종목명']}** {icon}\n"
            f"> {row['현재가']:,}원 (이격도 {row['이격도']}%)\n"
            f"> 거래량 {row['거래량변동']} / 기 {row['기관수급']:,}\n\n"
        )
    
    send_discord_message(DISCORD_WEBHOOK_URL, discord_msg)
    print("✅ 전송 완료!")

else:
    msg = f"## 📉 {TARGET_DATE} 분석 결과\n조건에 맞는 종목이 없습니다."
    send_discord_message(DISCORD_WEBHOOK_URL, msg)
    print("검색된 종목 없음.")
