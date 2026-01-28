import FinanceDataReader as fdr
import pandas as pd
import requests
import time

# --- 설정 ---
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1461902939139604684/ZdCdITanTb3sotd8LlCYlJzSYkVLduAsjC6CD2h26X56wXoQRw7NY72kTNzxTI6UE4Pi"

def get_top_500_stocks():
    """시총 상위 종목 수집 및 컬럼명 에러 방지"""
    print("종목 리스트 수집 중...")
    # KRX 전체 리스트를 가져와서 시총순 정렬
    df = fdr.StockListing('KRX')
    
    # 컬럼명 대응: 'Symbol'이 없으면 'Code' 사용
    col_name = 'Symbol' if 'Symbol' in df.columns else 'Code'
    
    # 코스피/코스닥 각각 상위 500개 추출
    kospi = df[df['Market'] == 'KOSPI'].sort_values('Marcap', ascending=False).head(500)
    kosdaq = df[df['Market'] == 'KOSDAQ'].sort_values('Marcap', ascending=False).head(500)
    
    combined = pd.concat([kospi, kosdaq])
    
    # 리스트화
    return [{'Symbol': row[col_name], 'Name': row['Name']} for _, row in combined.iterrows()]

def analyze_retracement(symbol, name):
    """눌림목 지지 로직 (조건 1, 2, 3번 적용)"""
    try:
        # 최근 60일 데이터 수집
        df = fdr.DataReader(symbol).tail(60)
        if len(df) < 40: return None

        # 지표 계산: 20일 이동평균선
        df['MA20'] = df['Close'].rolling(window=20).mean()
        
        # 최근 20일 중 최고가 및 최고 거래량 (돌파 시점 에너지 확인)
        recent_df = df.iloc[-20:-2]
        max_high = recent_df['High'].max()
        max_vol = recent_df['Volume'].max()

        curr_close = df['Close'].iloc[-1]
        curr_vol = df['Volume'].iloc[-1]
        curr_ma20 = df['MA20'].iloc[-1]

        # --- 눌림목 지지 조건 ---
        # 1. 저항의 지지 전환 (전고점 근처) 또는 2. 이평선 지지 (20일선 근처)
        is_near_support = (abs(curr_close - max_high) / max_high < 0.03) or \
                          (abs(curr_close - curr_ma20) / curr_ma20 < 0.02)
        
        # 3. 거래량 급감 (최고 거래량 대비 30% 이하로 에너지가 응축된 상태)
        is_vol_dry = curr_vol < (max_vol * 0.3)
        
        # 추가: 정배열 유지 (주가가 20일선 위에 위치)
        is_above_ma20 = curr_close > curr_ma20

        if is_near_support and is_vol_dry and is_above_ma20:
            return f"✅ **{name} ({symbol})**\n- 현재가: {curr_close:,}원\n- 거래량비율: {round((curr_vol/max_vol)*100, 1)}% (급감)\n- 상태: 눌림목 지지 확인"
    except:
        return None
    return None

def send_to_discord(message):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        print(f"디스코드 전송 실패: {e}")

if __name__ == "__main__":
    stocks = get_top_500_stocks()
    results = []
    
    print(f"총 {len(stocks)}개 종목 분석 시작...")
    for stock in stocks:
        res = analyze_retracement(stock['Symbol'], stock['Name'])
        if res:
            results.append(res)
        # 깃허브 액션 IP 차단 방지를 위해 아주 짧은 대기 시간 추가
        time.sleep(0.01)

    # 결과 전송
    if results:
        header = f"📊 **[{pd.Timestamp.now().strftime('%Y-%m-%d')}] 눌림목 지지 스캔 결과**\n"
        full_msg = header + "\n".join(results)
        # 디스코드 2,000자 제한에 맞춰 끊어서 전송
        for i in range(0, len(full_msg), 1900):
            send_to_discord(full_msg[i:i+1900])
    else:
        send_to_discord("🧐 오늘 조건에 맞는 눌림목 지지 종목이 없습니다.")
