import requests
import json
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time

# ==========================================
# 0. 사용자 설정 (2단계 필터 동시 가동)
# ==========================================
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"
TARGET_DATE = datetime.now().strftime("%Y%m%d")

# [공통 조건]
CHECK_DAYS = 30           # 30일 이내 탐색
DISPARITY_LIMIT = 95.0    # 이격도 95% 이하 (과대낙폭)
QUIET_VOL_RATIO = 0.5     # 거래량 침묵 (기준봉 대비 50% 이하 유지)

# [조건 A: 일반형 (Standard)]
COND_A_PRICE = 10.0       # 10% 이상 상승
COND_A_VOL = 2.0          # 200%(2배) 이상 폭발

# [조건 B: 강력형 (High-Power)] - 우선순위 높음
COND_B_PRICE = 15.0       # 15% 이상 급등
COND_B_VOL = 3.0          # 300%(3배) 이상 폭발

print(f"[{TARGET_DATE}] '더블 필터(Standard & High)' 정밀 분석 시작")
print("-" * 60)

# ==========================================
# 함수 정의
# ==========================================
def send_discord_message(webhook_url, content):
    """디스코드 메시지 전송 (길면 나눠서 보냄)"""
    if len(content) > 1900:
        chunks = [content[i:i+1900] for i in range(0, len(content), 1900)]
        for chunk in chunks:
            data = {"content": chunk}
            headers = {"Content-Type": "application/json"}
            try:
                requests.post(webhook_url, data=json.dumps(data), headers=headers)
                time.sleep(0.5)
            except: pass
    else:
        data = {"content": content}
        headers = {"Content-Type": "application/json"}
        try:
            requests.post(webhook_url, data=json.dumps(data), headers=headers)
        except: pass

def get_top_tickers(date):
    """코스피 500 + 코스닥 500"""
    print("1. 종목 리스트 확보 중...")
    try:
        kospi = stock.get_market_cap(date, market="KOSPI").sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        kosdaq = stock.get_market_cap(date, market="KOSDAQ").sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        tickers = kospi + kosdaq
        
        etfs = stock.get_etf_ticker_list(date)
        etns = stock.get_etn_ticker_list(date)
        exclude = set(etfs + etns)
        
        return [t for t in tickers if t not in exclude]
    except:
        return []

# ==========================================
# 메인 로직
# ==========================================
# 흑자 기업 필터링용 데이터
print("2. 재무 데이터 스캔 중... (적자 기업 자동 제외)")
fundamental_df = stock.get_market_fundamental_by_ticker(TARGET_DATE, market="ALL")

tickers = get_top_tickers(TARGET_DATE)
print(f"3. 분석 시작 (대상: {len(tickers)}개)")

# 결과 저장소
tier1_results = [] # 강력형 (15%/300%)
tier2_results = [] # 일반형 (10%/200%)

count = 0
for ticker in tickers:
    count += 1
    if count % 100 == 0: print(f"   ... {count}개 완료")

    try:
        # [0] 흑자 기업 필터 (PER > 0)
        try:
            per = fundamental_df.loc[ticker, 'PER']
            if per <= 0: continue # 적자는 바로 탈락
        except: continue

        # [1] 데이터 가져오기
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv_by_date(start_date, TARGET_DATE, ticker)
        if len(ohlcv) < 40: continue

        curr_close = ohlcv['종가'].iloc[-1]
        ma20 = ohlcv['종가'].rolling(window=20).mean().iloc[-1]
        
        # [2] 이격도 체크 (공통 조건)
        if ma20 == 0: continue
        disparity = (curr_close / ma20) * 100
        if disparity > DISPARITY_LIMIT: continue # 95% 초과면 탈락 (안 쌈)

        recent_data = ohlcv.iloc[-(CHECK_DAYS+1):]

        # ---------------------------------------------------------
        # [3] 티어 분류 로직 (강한 조건 B부터 체크)
        # ---------------------------------------------------------
        # B조건(15%/300%) 만족 여부 확인
        is_tier1 = False
        trigger_date_b = ""
        
        # 역순 탐색 (최근 기준봉 우선)
        for i in range(len(recent_data)-1, 0, -1):
            curr_row = recent_data.iloc[i]
            prev_row = recent_data.iloc[i-1]
            if prev_row['종가'] == 0 or prev_row['거래량'] == 0: continue

            rise = (curr_row['고가'] - prev_row['종가']) / prev_row['종가'] * 100
            vol_rate = curr_row['거래량'] / prev_row['거래량']

            # B 조건 체크
            if rise >= COND_B_PRICE and vol_rate >= COND_B_VOL:
                # 눌림목(침묵) 확인
                check_range = recent_data.iloc[i+1:]
                if len(check_range) == 0: continue
                
                trigger_vol = curr_row['거래량']
                is_quiet = True
                for vol in check_range['거래량']:
                    if vol > (trigger_vol * QUIET_VOL_RATIO):
                        is_quiet = False; break
                
                if is_quiet:
                    is_tier1 = True
                    trigger_date_b = recent_data.index[i].strftime("%Y-%m-%d")
                    
                    # 수급 확인
                    s_start = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
                    supply = stock.get_market_net_purchases_of_equities_by_date(s_start, TARGET_DATE, ticker)
                    inst = int(supply.tail(5)['기관합계'].sum())
                    fore = int(supply.tail(5)['외국인'].sum())
                    
                    name = stock.get_market_ticker_name(ticker)
                    tier1_results.append({
                        '종목명': name, '현재가': curr_close, '이격도': round(disparity,1),
                        '기준일': trigger_date_b, '기관': inst, '외인': fore, 'PER': per
                    })
                    break # B 조건 만족 시 A는 검사 안 하고 다음 종목으로

        if is_tier1: continue # 1티어에 넣었으면 다음 종목으로 (중복 방지)

        # ---------------------------------------------------------
        # B 조건 만족 안 했으면 -> A조건(10%/200%) 체크
        # ---------------------------------------------------------
        for i in range(len(recent_data)-1, 0, -1):
            curr_row = recent_data.iloc[i]
            prev_row = recent_data.iloc[i-1]
            if prev_row['종가'] == 0 or prev_row['거래량'] == 0: continue

            rise = (curr_row['고가'] - prev_row['종가']) / prev_row['종가'] * 100
            vol_rate = curr_row['거래량'] / prev_row['거래량']

            # A 조건 체크
            if rise >= COND_A_PRICE and vol_rate >= COND_A_VOL:
                check_range = recent_data.iloc[i+1:]
                if len(check_range) == 0: continue
                
                trigger_vol = curr_row['거래량']
                is_quiet = True
                for vol in check_range['거래량']:
                    if vol > (trigger_vol * QUIET_VOL_RATIO):
                        is_quiet = False; break
                
                if is_quiet:
                    # 수급 확인
                    s_start = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
                    supply = stock.get_market_net_purchases_of_equities_by_date(s_start, TARGET_DATE, ticker)
                    inst = int(supply.tail(5)['기관합계'].sum())
                    fore = int(supply.tail(5)['외국인'].sum())
                    
                    name = stock.get_market_ticker_name(ticker)
                    tier2_results.append({
                        '종목명': name, '현재가': curr_close, '이격도': round(disparity,1),
                        '기준일': recent_data.index[i].strftime("%Y-%m-%d"), '기관': inst, '외인': fore, 'PER': per
                    })
                    break 

    except: continue

# ==========================================
# 결과 전송
# ==========================================
print("\n" + "="*70)
print(f"📊 분석 완료. 1티어({len(tier1_results)}개), 2티어({len(tier2_results)}개) 발견.")

msg = f"## ⚔️ {TARGET_DATE} 흑자기업 더블 검색\n"
msg += f"**공통:** 흑자(PER>0) | 이격도95↓ | 침묵(50%↓)\n\n"

# [1티어 결과]
if len(tier1_results) > 0:
    df1 = pd.DataFrame(tier1_results).sort_values(by='이격도', ascending=True)
    msg += f"### 🔥 [1티어] 강력 세력주 (15%↑ / 300%↑)\n"
    for _, row in df1.iterrows():
        icon = "✅"
        if row['기관'] > 0 and row['외인'] > 0: icon = "👑(쌍끌이)"
        elif row['기관'] > 0: icon = "🔴(기관)"
        elif row['외인'] > 0: icon = "🔵(외인)"
        
        msg += (f"**{row['종목명']}** {icon}\n"
                f"> {row['현재가']:,}원 (이격도 {row['이격도']}%)\n"
                f"> {row['기준일']} 폭발 (PER {row['PER']})\n\n")
else:
    msg += f"### 🔥 [1티어] 강력 세력주\n검색된 종목 없음 (조건 만족하는 흑자기업 없음)\n\n"

msg += "-"*20 + "\n\n"

# [2티어 결과]
if len(tier2_results) > 0:
    df2 = pd.DataFrame(tier2_results).sort_values(by='이격도', ascending=True)
    msg += f"### 🛡️ [2티어] 일반 눌림목 (10%↑ / 200%↑)\n"
    # 너무 많으면 상위 15개만
    for _, row in df2.head(15).iterrows():
        icon = ""
        if row['기관'] > 0: icon = "🔴"
        
        msg += (f"**{row['종목명']}** {icon}\n"
                f"> {row['현재가']:,}원 (이격도 {row['이격도']}%)\n"
                f"> {row['기준일']} 기준봉\n\n")
    if len(df2) > 15:
        msg += f"*외 {len(df2)-15}개 종목 추가 검색됨*"
else:
    msg += f"### 🛡️ [2티어] 일반 눌림목\n검색된 종목 없음\n"

send_discord_message(DISCORD_WEBHOOK_URL, msg)
print("✅ 디스코드 전송 완료!")
