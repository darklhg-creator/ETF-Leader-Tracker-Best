import pandas as pd
import numpy as np
from pykrx import stock
import time
from datetime import datetime, timedelta
import requests

# 1. 주변 캔들 대비 저점을 찾는 함수
def get_local_minima(series, order=5):
    minima_indices = []
    for i in range(order, len(series) - order):
        if all(series[i] <= series[i-j] for j in range(1, order + 1)) and \
           all(series[i] <= series[i+j] for j in range(1, order + 1)):
            minima_indices.append(i)
    return minima_indices

# 2. 하락 후 상승전환(1>2<3<4) 패턴 및 추세선 확인 함수
def check_turnaround_trend(ticker, name, start_date, end_date):
    try:
        df = stock.get_market_ohlcv_by_date(fromdate=start_date, todate=end_date, ticker=ticker)
        if len(df) < 50: return None

        # 20일선 이격도 계산
        ma20 = df['종가'].rolling(window=20).mean()
        curr_disparity_20 = round((df['종가'].iloc[-1] / ma20.iloc[-1]) * 100, 1)

        low_values = df['저가'].values
        low_idx = get_local_minima(low_values, order=5)
        
        if len(low_idx) > 0 and low_idx[-1] == len(df) - 1: low_idx = low_idx[:-1]

        if len(low_idx) >= 4:
            recent_idx = low_idx[-4:] 
            recent_lows = low_values[recent_idx] 
            
            # 패턴 확인: 1번 > 2번 (하락) AND 2번 < 3번 < 4번 (상승)
            if (recent_lows[0] > recent_lows[1]) and (recent_lows[1] < recent_lows[2] < recent_lows[3]):
                
                # 상승 구간(2, 3, 4번)으로 추세선 및 R2 계산
                trend_x = np.array(recent_idx[1:])
                trend_y = recent_lows[1:]
                
                coeffs = np.polyfit(trend_x, trend_y, 1)
                p = np.poly1d(coeffs)
                y_hat = p(trend_x); y_bar = np.mean(trend_y)
                ss_res = np.sum((trend_y - y_hat)**2); ss_tot = np.sum((trend_y - y_bar)**2)
                r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
                
                if r_squared < 0.85: return None

                # 오늘 종가가 2-3-4 추세선 지지 중인지 확인
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

# 3. 시장 개장 여부 확인 함수 (이게 빠져서 에러가 났었습니다!)
def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5: return False
    target_date = now.strftime("%Y%m%d")
    try:
        df = stock.get_market_ohlcv_by_date(target_date, target_date, "005930")
        return not df.empty
    except: return False

def get_top_tickers(market_name, count):
    now = datetime.now()
    target_date = now.strftime("%Y%m%d")
    df = stock.get_market_cap_by_ticker(target_date, market=market_name)
    while df.empty:
        now -= timedelta(days=1)
        target_date = now.strftime("%Y%m%d")
        df = stock.get_market_cap_by_ticker(target_date, market=market_name)
    return df.sort_values(by='시가총액', ascending=False).head(count).index

def send_discord_message(content):
    webhook_url = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"
    requests.post(webhook_url, json={"content": content})

if __name__ == "__main__":
    if not is_market_open():
        print("오늘은 장이 열리지 않습니다.")
        exit()

    now = datetime.now()
    start_date = (now - timedelta(days=150)).strftime("%Y%m%d")
    end_date = now.strftime("%Y%m%d")
    
    kospi = list(get_top_tickers("KOSPI", 500))
    kosdaq = list(get_top_tickers("KOSDAQ", 1000))
    all_targets = kospi + kosdaq
    
    results = []
    for i, ticker in enumerate(all_targets):
        name = stock.get_market_ticker_name(ticker)
        res = check_turnaround_trend(ticker, name, start_date, end_date)
        if res:
            results.append(res)
            print(f"✅ 포착: {name}")
        if (i+1) % 200 == 0: print(f"⏳ 분석 중... ({i+1}/{len(all_targets)})")
        time.sleep(0.02)

    if results:
        final_df = pd.DataFrame(results).sort_values(by='이격도', ascending=False)
        msg = f"📅 {now.strftime('%Y-%m-%d')} 하락 후 상승전환 종목\n```\n{final_df.to_string(index=False)}\n```"
    else:
        msg = f"📅 {now.strftime('%Y-%m-%d')} 조건에 맞는 종목이 없습니다."
    
    send_discord_message(msg)
