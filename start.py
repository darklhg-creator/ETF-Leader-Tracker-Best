import requests
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta, timezone

WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

def send_discord_message(msg_content):
    payload = {"content": msg_content}
    try:
        requests.post(WEBHOOK_URL, json=payload)
    except Exception as e:
        print(f"❌ 전송 에러: {e}")

def main():
    KST = timezone(timedelta(hours=9))
    today_dt = datetime.now(KST)
    target_date = today_dt.strftime("%Y%m%d")
    
    # 전일 종가를 가져오기 위해 7일 전부터의 데이터를 조회
    start_date = (today_dt - timedelta(days=7)).strftime("%Y%m%d")
    
    print(f"📅 조회 기준일: {target_date}")

    try:
        # 1. 오늘 ETF 시세 가져오기
        df_today = stock.get_etf_ohlcv_by_ticker(target_date)
        
        if df_today.empty:
            print("❌ 오늘 데이터가 없습니다.")
            return

        exclude_filters = [
            '미국', '차이나', '중국', '일본', '나스닥', 'S&P', '글로벌', 'MSCI', '인도', '베트남', 
            '필라델피아', '레버리지', '인버스', '블룸버그', '항셍', '니케이', '빅테크', 'TSMC', 
            '대만', '유로', '스톡스', '선물', '채권', '국고채', '머니마켓', 'KOFR', 'CD금리'
        ]
        
        results = []

        for ticker, row in df_today.iterrows():
            name = stock.get_etf_ticker_name(ticker)
            if any(word in name for word in exclude_filters): continue
            
            try:
                # [핵심] 등락률이 없으므로 과거 데이터를 가져와서 직접 계산
                # ticker별로 최근 2일치 데이터를 가져옴
                df_hist = stock.get_etf_ohlcv_by_date(start_date, target_date, ticker)
                
                if len(df_hist) < 2: continue # 데이터가 부족하면 패스
                
                prev_close = df_hist['종가'].iloc[-2] # 전일 종가
                curr_close = df_hist['종가'].iloc[-1] # 오늘 종가
                
                # 등락률 계산식: ((현재가 - 전일가) / 전일가) * 100
                change_rate = ((curr_close - prev_close) / prev_close) * 100
                trading_amt = float(row['거래대금'])

                if change_rate > 0:
                    results.append({
                        '종목명': name,
                        '상승률': round(change_rate, 2),
                        '거래대금(억)': round(trading_amt / 100_000_000, 1)
                    })
            except:
                continue

        # 2. 결과 정렬 및 전송
        if results:
            final_df = pd.DataFrame(results).sort_values(by='상승률', ascending=False).head(10)
            
            # 상승률 표시 포맷 변경
            final_df['상승률'] = final_df['상승률'].map(lambda x: f"{x:.2f}%")

            discord_msg = f"🚀 **[오늘의 국내 ETF 상승률 TOP 10]** ({today_dt.strftime('%Y-%m-%d')})\n"
            discord_msg += "```text\n"
            discord_msg += final_df.to_string(index=False) + "\n"
            discord_msg += "```\n"
            discord_msg += "💡 등락률 데이터를 직접 계산하여 정확도를 높였습니다."
            
            send_discord_message(discord_msg)
            print(final_df)
        else:
            print("⚠️ 상승한 종목이 없습니다.")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
