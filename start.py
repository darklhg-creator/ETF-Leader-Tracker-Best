import requests
import json
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time

# ==========================================
# 0. 사용자 설정 (실적 조건 제거 버전)
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

print(f"[{TARGET_DATE}] '차트 올인(No-Fundamental)' 검색 시작")
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
    """코스피 500 + 코스닥 500 (유동성 확보용)"""
    print("1. 종목 리스트 확보 중...")
    try:
        # 시가총액 데이터는 안전하게 하루 전 기준으로 가져옴
        safe_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        kospi = stock.get_market_cap(safe_date, market="KOSPI").sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        kosdaq = stock.get_market_cap(safe_date, market="KOSDAQ").sort_values(by='시가총액', ascending=False).head(500).index.tolist()
        tickers = kospi + kosdaq
        
        etfs = stock.get_etf_ticker_list(safe_date)
        etns = stock.get_etn_ticker_list(safe_date)
        exclude = set(etfs + etns)
        
        return [t for t in tickers if t not in exclude]
    except:
        return []

# ==========================================
# 메인 로직
# ==========================================
# 실적(PER) 데이터 로딩 부분 삭제함

tickers = get_top_tickers(TARGET_DATE)
print(f"2. 분석 시작 (대상: {len(tickers)}개)")

# 결과 저장소
tier1_results = [] # 강력형 (15%/300%)
tier2_results = [] # 일반형 (10%/200%)

count = 0
for ticker in tickers:
    count += 1
    if count % 100 == 0: print(f"   ... {count}개 완료")

    try:
        # [1] 데이터 가져오기
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv_by_date(start_date, TARGET_DATE, ticker)
        
        if len(ohlcv) < 40: continue

        curr_close = ohlcv['종가'].iloc[-1]
        ma20 = ohlcv['종가'].rolling(window=20).mean().iloc[-1]
        
        # [2] 이격
