import pandas as pd
import numpy as np
from pykrx import stock
import time
from datetime import datetime, timedelta
import requests

def get_local_minima(series, order=5):
    minima_indices = []
    for i in range(order, len(series) - order):
        if all(series[i] <= series[i-j] for j in range(1, order + 1)) and \
           all(series[i] <= series[i+j] for j in range(1, order + 1)):
            minima_indices.append(i)
    return minima_indices

def check_turnaround_trend(ticker, name, start_date, end_date):
    try:
        df = stock.get_market_ohlcv_by_date(fromdate=start_date, todate=end_date, ticker=ticker)
        if len(df) < 50: return None

        # 1. 20일선 이격도 계산
        ma20 = df['종가'].rolling(window=20).mean()
        curr_disparity_20 = round((df['종가'].iloc[-1] / ma20.iloc[-1]) * 100, 1)

        # 2. 저점(저가) 추출
        low_values = df['저가'].values
        low_idx = get_local_minima(low_values, order=5)
        
        # 오늘이 저점으로 인식되면 제외
        if len(low_idx) > 0 and low_idx[-1] == len(df) - 1: low_idx = low_idx[:-1]

        # 저점이 최소 4개는 있어야 함 (1, 2, 3, 4)
        if len(low_idx) >= 4:
            recent_idx = low_idx[-4:] # 마지막 4개 저점 인덱스
            recent_lows = low_values[recent_idx] # 마지막 4개 저점 가격
            
            # 조건: 1번 > 2번 (하락/바닥 형성) AND 2번 < 3번 < 4번 (상승 전환)
            if (recent_lows[0] > recent_lows[1]) and (recent_lows[1] < recent_lows[2] < recent_lows[3]):
                
                # 추세선과 R2는 상승 구간인 2, 3, 4번(인덱스상 뒤의 3개)으로 계산
                trend_x = np.array(recent_idx[1:])
                trend_y = recent_lows[1:]
                
                coeffs = np.polyfit(trend_x, trend_y, 1)
                p = np.poly1d(coeffs)
                y_hat = p(trend_x); y_bar = np.mean(trend_y)
                ss_res = np.sum((trend_y - y_hat)**2); ss_tot = np.sum((trend_y - y_bar)**2)
                r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
                
                # 신뢰도 필터 (0.85 이상)
                if r_squared < 0.85: return None

                # 오늘 종가가 추세선(2-3-4 연결선) 지지 중인지 확인
                today_idx = len(df) - 1
                expected_price = p(today_idx)
                current_close = df['종가'].iloc[-1]
                
                if expected_price * 0.99 <= current_close <= expected_price * 1.05:
                    low_dates = [df.index[i].strftime("%m/%d") for i in recent_idx]
                    return {
                        "종목명": name,
                        "1차(고)": low_dates[0],
                        "2차(저)": low_dates[1],
                        "3차(상)": low_dates[2],
                        "4차(상)": low_dates[3],
                        "이격도": curr_disparity_20
                    }
    except: pass
    return None

# (is_market_open, get_top_tickers, send_discord_message 함수는 이전과 동일)

if __name__ == "__main__":
    # 시장 개장 확인
    if not is_market_open():
        print("시장이 열리지 않는 날입니다.")
        exit()

    now = datetime.now()
    # 분석 기간을 150일로 조금 더 넉넉히 (저점 4개를 찾기 위함)
    start_date = (now - timedelta(days=150)).strftime("%Y%m%d")
    end_date = now.strftime("%Y%m%d")
    
    # 시총 상위 리스트 확보 (KOSPI 500 + KOSDAQ 1000)
    kospi = list(stock.get_market_cap_by_ticker(end_date, market="KOSPI").sort_values(by='시가총액', ascending=False).head(500).index)
    kosdaq = list(stock.get_market_cap_by_ticker(end_date, market="KOSDAQ").sort_values(by='시가총액', ascending=False).head(1000).index)
    all_targets = kospi + kosdaq
    
    results = []
    for i, ticker in enumerate(all_targets):
        name = stock.get_market_ticker_name(ticker)
        res = check_turnaround_trend(ticker, name, start_date, end_date)
        if res:
            results.append(res)
            print(f"✨ 턴어라운드 포착: {name}")
        if (i+1) % 200 == 0: print(f"⏳ 진행 중... ({i+1}/{len(all_targets)})")
        time.sleep(0.02)

    if results:
        final_df = pd.DataFrame(results).sort_values(by='이격도', ascending=False)
        msg = f"📅 {now.strftime('%Y-%m-%d')} 하락 후 상승전환 종목\n```\n{final_df.to_string(index=False)}\n```"
    else:
        msg = f"📅 {now.strftime('%Y-%m-%d')} 포착된 종목이 없습니다."
    
    send_discord_message(msg)
