import time
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta

# ==========================================
# 1. 진단 설정 (조건을 아주 널널하게 둠)
# ==========================================
TARGET_DATE = datetime.now().strftime("%Y%m%d") # 오늘
# TARGET_DATE = "20260130" # 날짜 강제 고정 (필요시 주석 해제)

# [진단용 느슨한 조건]
MIN_DISPARITY = 90.0     # 90%까지 봐줌
MAX_DISPARITY = 110.0    # 110%까지 봐줌
VOL_DROP_RATE = 1.0      # 거래량 같거나 줄면 통과 (1.0)

print(f"[{TARGET_DATE}] 필터 단계별 생존율 테스트를 시작합니다...")
print("-" * 60)

# ==========================================
# 2. 메인 로직 (단계별 카운팅)
# ==========================================

# 1) 시총 상위 가져오기
print("Step 1. 종목 리스트 가져오는 중...")
try:
    df_kospi = stock.get_market_cap(TARGET_DATE, market="KOSPI")
    top_kospi = df_kospi.sort_values(by='시가총액', ascending=False).head(300).index.tolist()
    
    df_kosdaq = stock.get_market_cap(TARGET_DATE, market="KOSDAQ")
    top_kosdaq = df_kosdaq.sort_values(by='시가총액', ascending=False).head(300).index.tolist()
    
    tickers = top_kospi + top_kosdaq # 총 600개만 테스트
    print(f"✅ 총 검사 대상: {len(tickers)}개 종목 로딩 성공")
except Exception as e:
    print(f"❌ 종목 가져오기 실패: {e}")
    tickers = []

# 카운터 변수
pass_data = 0      # 데이터 있음
pass_price = 0     # 주가 하락/보합
pass_vol = 0       # 거래량 감소
pass_disparity = 0 # 이격도 조건
pass_supply = 0    # 수급 조건

print("Step 2. 600개 종목 전수 검사 시작 (진행률 표시)...")

count = 0
for ticker in tickers:
    count += 1
    if count % 100 == 0: print(f"   ... {count}개 확인 중")

    try:
        # A. 데이터 가져오기
        start_date = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv_by_date(start_date, TARGET_DATE, ticker)
        
        if len(ohlcv) < 20: continue # 데이터 없으면 패스
        pass_data += 1

        curr_close = ohlcv['종가'].iloc[-1]
        prev_close = ohlcv['종가'].iloc[-2]
        curr_vol = ohlcv['거래량'].iloc[-1]
        prev_vol = ohlcv['거래량'].iloc[-2]

        # B. 필터링 시작 (탈락 원인 파악)
        
        # [검사 1] 주가가 떨어졌나?
        if curr_close > prev_close: continue
        pass_price += 1

        # [검사 2] 거래량이 줄었나? (100% 이하)
        if curr_vol > (prev_vol * VOL_DROP_RATE): continue
        pass_vol += 1

        # [검사 3] 이격도 (90~110%)
        ma20 = ohlcv['종가'].rolling(window=20).mean().iloc[-1]
        disparity = (curr_close / ma20) * 100
        if not (MIN_DISPARITY <= disparity <= MAX_DISPARITY): continue
        pass_disparity += 1

        # [검사 4] 수급 (기관 or 외인 순매수)
        supply_start = (datetime.strptime(TARGET_DATE, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        supply = stock.get_market_net_purchases_of_equities_by_date(supply_start, TARGET_DATE, ticker)
        recent = supply.tail(5)
        if recent['기관합계'].sum() <= 0 and recent['외국인'].sum() <= 0: continue
        pass_supply += 1

    except:
        continue

# ==========================================
# 3. 진단 결과 리포트
# ==========================================
print("\n" + "="*60)
print("🩺 [진단 리포트] 종목들이 어디서 사라졌을까?")
print("="*60)
print(f"1. 대상 종목 수 : {len(tickers)}개")
print(f"2. 데이터 정상  : {pass_data}개")
print(f"3. 주가 하락중  : {pass_price}개 (여기서 줄었으면 상승장)")
print(f"4. 거래량 감소  : {pass_vol}개 (여기서 줄었으면 투매장)")
print(f"5. 이격도 범위  : {pass_disparity}개 (범위: 90~110%)")
print(f"6. 수급(기관/외): {pass_supply}개 (최종 생존)")
print("-" * 60)

if pass_supply == 0:
    if pass_disparity == 0:
        print("결론: 💥 '이격도'가 문제였습니다. 종목들이 90% 밑으로 추락했거나 110% 위로 날아갔습니다.")
    elif pass_supply == 0:
        print("결론: 💸 '수급'이 문제였습니다. 기관/외국인이 다 팔고 도망갔습니다.")
else:
    print(f"결론: {pass_supply}개의 종목이 발견되었습니다. 코드는 정상입니다!")
