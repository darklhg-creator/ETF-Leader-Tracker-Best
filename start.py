import requests
import json
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time

# ==========================================
# 1. 사용자 설정 (놓침 방지 완화 버전)
# ==========================================
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

TARGET_DATE = datetime.now().strftime("%Y%m%d")

# [A. 기준봉(폭발) 조건] - 조금 더 현실적으로 수정
CHECK_DAYS = 30           # 최근 30일 이내
FLAG_PRICE_RATE = 10.0    # 10% 이상 주가 급등 (그대로 유지)
FLAG_VOL_RATE = 3.0       # 전일 대비 300%(3배) 이상 (5배->3배로 완화하여 포착률 높임)

# [B. 눌림목(침묵) 조건] - 숨 쉴 구멍 주기
QUIET_VOL_RATIO = 0.35    # 기준봉 대비 35% 이하 (25%->35%로 여유 줌)

print(f"[{TARGET_DATE}] '폭발 후 침묵' 정밀 분석 시작")
print(f"조건: 30일내 {int(FLAG_PRICE_RATE)}%↑/3배 거래량 → 이후 거래량 {int(QUIET_VOL_RATIO*100)}% 이하 유지")
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

def get_target_tickers(date):
    """코스피 500 + 코스닥 1000 (총 1500개)"""
    print("1. 검색 대상 리스트 확보 중...")
    try:
        df_kospi = stock.get_market_cap(date, market="KOSPI")
        top_kospi = df_kospi.sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        
        df_kosdaq = stock.get_market_cap(date, market="KOSDAQ")
        top_kosdaq = df_kosdaq.sort_values(by='시가총액', ascending=False).head(1000).index.tolist()
        
        total_tickers = top_kospi + top_kosdaq
        etfs = stock.get_etf_ticker_list(date)
        etns = stock.get_etn_ticker_list(date)
        exclude_list = set(etfs + etns)
        
        return [t for t in total_tickers if t not in exclude_list]
    except:
        return []

# ==========================================
# 3. 메인 분석 로직
# ==========================================
tickers = get_target_tickers(TARGET_DATE)
print(f"   -> 분석 대상: {len(tickers)}개 종목")

results = []
print("2. 패턴 매칭 시작...")

count = 0
for ticker in tickers:
    count += 1
    if count % 100 == 0: print(f"   ... {count}개 완료")

    try:
        # 데이터 넉넉히 가져오기
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv_by_date(start_date, TARGET_DATE, ticker)
        
        if len(ohlcv) < 40: continue

        # 최근 30일 데이터 (오늘 포함)
        recent_data = ohlcv.iloc[-(CHECK_DAYS+1):]
        
        found_trigger = False
        trigger_date = ""
        trigger_vol = 0
        trigger_price_change = 0.0
        
        # ---------------------------------------------------------
        # Step 1. 기준봉(Trigger) 찾기
        # ---------------------------------------------------------
        # 최근 날짜부터 거꾸로 찾아서 '가장 최근의 폭발'을 기준으로 삼음
        # (과거에 여러 번 폭발했어도, 지금 눌림목을 만든 '그 녀석'이 중요하므로)
        for i in range(len(recent_data) - 2, 0, -1): # 오늘(마지막) 제외하고 역순 탐색
            curr_idx = i
            prev_idx = i - 1
            
            curr_vol = recent_data['거래량'].iloc[curr_idx]
            prev_vol = recent_data['거래량'].iloc[prev_idx]
            curr_close = recent_data['종가'].iloc[curr_idx]
            prev_close = recent_data['종가'].iloc[prev_idx]
            
            if prev_close == 0 or prev_vol == 0: continue
            
            price_rate = (curr_close - prev_close) / prev_close * 100
            vol_rate = curr_vol / prev_vol
            
            # [조건] 10% 이상 상승 AND 3배 이상 거래량
            if price_rate >= FLAG_PRICE_RATE and vol_rate >= FLAG_VOL_RATE:
                found_trigger = True
                trigger_date = recent_data.index[curr_idx].strftime("%Y-%m-%d")
                trigger_vol = curr_vol
                trigger_price_change = price_rate
                
                # 기준봉 이후 데이터 슬라이싱
                post_trigger_data = recent_data.iloc[curr_idx+1:]
                break # 가장 최근 기준봉 발견하면 스톱

        if not found_trigger: continue
        
        # 기준봉이 오늘 터진 거라면 눌림목 확인 불가하므로 패스
        if len(post_trigger_data) == 0: continue

        # ---------------------------------------------------------
        # Step 2. 눌림목(Quiet) 검증
        # ---------------------------------------------------------
        is_quiet = True
        current_vol_ratio = 0.0
        
        for i in range(len(post_trigger_data)):
            daily_vol = post_trigger_data['거래량'].iloc[i]
            
            # 하루라도 기준봉의 35%를 넘으면 탈락
            # (단, 오늘이 양봉이면서 거래량이 살짝 붙는 건 '반등 시작'일 수 있어서 봐줄 수도 있지만
            #  여기서는 일단 엄격하게 '거래량 죽어있는지'만 봅니다)
            if daily_vol > (trigger_vol * QUIET_VOL_RATIO):
                is_quiet = False
                break
            
            if i == len(post_trigger_data) - 1: # 마지막 날
                current_vol_ratio = (daily_vol / trigger_vol) * 100

        if not is_quiet: continue
            
        # ---------------------------------------------------------
        # Step 3. 수급 및 저장
        # ---------------------------------------------------------
        supply_start = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        supply_df = stock.get_market_net_purchases_of_equities_by_date(supply_start, TARGET_DATE, ticker)
        recent_supply = supply_df.tail(5)
        
        inst_sum = int(recent_supply['기관합계'].sum())
        for_sum = int(recent_supply['외국인'].sum())
        name = stock.get_market_ticker_name(ticker)
        
        results.append({
            '종목명': name,
            '현재가': ohlcv['종가'].iloc[-1],
            '기준일': trigger_date,
            '기준상승': f"{round(trigger_price_change,1)}%",
            '현재거래비율': f"{round(current_vol_ratio,1)}%",
            '기관수급': inst_sum,
            '외인수급': for_sum
        })

    except:
        continue

# ==========================================
# 4. 결과 전송
# ==========================================
print("\n" + "="*70)
print(f"📊 분석 완료 ({len(results)}개 발견). 디스코드 전송...")

if len(results) > 0:
    res_df = pd.DataFrame(results)
    res_df = res_df.sort_values(by='기준일', ascending=False)

    discord_msg = f"## 🌋 {TARGET_DATE} 폭발 후 침묵(눌림목) 발견\n"
    discord_msg += f"**조건:** 10%↑/3배폭발 → 35%이하 침묵 (안전모드)\n\n"
    
    for idx, row in res_df.head(20).iterrows():
        icon = "🤫"
        if row['기관수급'] > 0 and row['외인수급'] > 0: icon = "🔥"
        elif row['기관수급'] > 0: icon = "🔴"
        elif row['외인수급'] > 0: icon = "🔵"

        discord_msg += (
            f"**{idx+1}. {row['종목명']}** {icon}\n"
            f"> 가격: {row['현재가']:,}원 ({row['기준일']} 폭발)\n"
            f"> 침묵: 기준봉 대비 거래량 **{row['현재거래비율']}**\n"
            f"> 수급: 기 {row['기관수급']:,} / 외 {row['외인수급']:,}\n\n"
        )
    
    send_discord_message(DISCORD_WEBHOOK_URL, discord_msg)
    print("✅ 전송 완료!")

else:
    msg = f"## 📉 {TARGET_DATE} 분석 결과\n조건에 맞는 종목이 없습니다.\n(시장 거래량이 전체적으로 말라있거나, 급등주가 없습니다.)"
    send_discord_message(DISCORD_WEBHOOK_URL, msg)
    print("검색된 종목 없음.")
