import FinanceDataReader as fdr
import pandas as pd
import requests
import time

# --- 설정 ---
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1461902939139604684/ZdCdITanTb3sotd8LlCYlJzSYkVLduAsjC6CD2h26X56wXoQRw7NY72kTNzxTI6UE4Pi"

def get_top_500_stocks():
    """코스피, 코스닥 시총 상위 500개씩 리스트업"""
    print("종목 리스트를 불러오는 중...")
    df_kospi = fdr.StockListing('KOSPI').sort_values('Marcap', ascending=False).head(500)
    df_kosdaq = fdr.StockListing('KOSDAQ').sort_values('Marcap', ascending=False).head(500)
    
    # 종목코드(Symbol)와 이름(Name)만 추출
    stocks = pd.concat([df_kospi[['Symbol', 'Name']], df_kosdaq[['Symbol', 'Name']]])
    return stocks.to_dict('records')

def analyze_retracement(symbol, name):
    """눌림목 지지 로직 적용"""
    try:
        df = fdr.DataReader(symbol).tail(60)
        if len(df) < 40: return None

        # 지표 계산
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['Vol_MA5'] = df['Volume'].rolling(window=5).mean()

        # 최근 20일 중 최고가 및 최고 거래량 (돌파 시점 기준)
        recent_df = df.iloc[-20:-2]
        max_high = recent_df['High'].max()
        max_vol = recent_df['Volume'].max()

        curr_close = df['Close'].iloc[-1]
        curr_vol = df['Volume'].iloc[-1]
        curr_ma20 = df['MA20'].iloc[-1]

        # --- 조건 검증 ---
        # 1. 저항의 지지 전환: 현재가가 전고점 대비 -3% ~ +3% 범위 내
        is_near_prev_high = abs(curr_close - max_high) / max_high < 0.03
        
        # 2. 이평선 지지: 현재가가 20일선 근처 (-2% ~ +2%)
        is_near_ma20 = abs(curr_close - curr_ma20) / curr_ma20 < 0.02
        
        # 3. 거래량 급감: 현재 거래량이 돌파 시점 최고 거래량의 25% 이하
        is_vol_dry = curr_vol < (max_vol * 0.25)

        # 4. 정배열 유지: 주가가 20일선 위에 있음
        is_above_ma20 = curr_close > curr_ma20

        if (is_near_prev_high or is_near_ma20) and is_vol_dry and is_above_ma20:
            return f"✅ **{name} ({symbol})**\n- 현재가: {curr_close:,}원\n- 거래량 비율: {round((curr_vol/max_vol)*100, 1)}% (급감)\n- 상태: 눌림목 지지 구간 확인"
    except:
        return None
    return None

def send_to_discord(message):
    data = {"content": message}
    requests.post(DISCORD_WEBHOOK_URL, json=data)

if __name__ == "__main__":
    stocks = get_top_500_stocks()
    results = []
    
    print(f"총 {len(stocks)}개 종목 분석 시작...")
    for stock in stocks:
        res = analyze_retracement(stock['Symbol'], stock['Name'])
        if res:
            results.append(res)
        time.sleep(0.05) # API 부하 방지

    if results:
        header = "📊 **오늘의 눌림목 지지 종목 스캔 결과**\n"
        full_msg = header + "\n".join(results)
        # 디스코드 메시지 길이 제한(2000자) 대응
        for i in range(0, len(full_msg), 1900):
            send_to_discord(full_msg[i:i+1900])
    else:
        send_to_discord("🧐 오늘 조건에 맞는 눌림목 종목이 없습니다.")
