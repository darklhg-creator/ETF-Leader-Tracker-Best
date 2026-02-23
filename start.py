import requests
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta, timezone

# ==========================================
# ⚙️ 1. 환경 설정 (Configuration)
# ==========================================
WEBHOOK_URL = "https://discord.com/api/webhooks/1466732864392397037/roekkL5WS9fh8uQnm6Bjcul4C8MDo1gsr1ZmzGh8GfuomzlJ5vpZdVbCaY--_MZOykQ4"

# 순수 국내 섹터만 남기기 위한 강력한 필터링 키워드
EXCLUDE_KEYWORDS = [
    '미국', '차이나', '중국', '일본', '나스닥', 'S&P', '글로벌', 'MSCI', '인도', '베트남', 
    '필라델피아', '레버리지', '인버스', '블룸버그', '항셍', '니케이', '빅테크', 'TSMC', 
    '대만', '유로', '스톡스', '선물', '채권', '국고채', '머니마켓', 'KOFR', 'CD금리', '달러', '엔화'
]

# ==========================================
# 🛠️ 2. 핵심 기능 클래스 (ETF Data Pipeline)
# ==========================================
class ETFTracker:
    def __init__(self, target_date):
        self.target_date = target_date
        self.df = pd.DataFrame()

    def fetch_data(self):
        """거래소(KRX)에서 하루치 ETF 변동 데이터를 한 번에 가져옵니다."""
        print(f"📡 [{self.target_date}] 데이터 수집 시작...")
        # get_market_price_change는 거래소가 공인한 시/고/저/종/등락률/거래대금을 완벽히 제공합니다.
        self.df = stock.get_market_price_change(self.target_date, self.target_date, "ETF")
        
        if self.df.empty:
            raise ValueError("데이터가 없습니다. 휴장일이거나 데이터 집계 전입니다.")
        
        print(f"✅ 수집 완료 (총 {len(self.df)}개 종목)")

    def process_data(self):
        """데이터 정제 및 필터링 (속도와 안정성을 위한 Pandas 벡터 연산)"""
        df = self.df.copy()
        
        # 1. 컬럼명 유연성 확보 (오류 원인 완벽 차단)
        cols = df.columns.tolist()
        rate_col = next((c for c in cols if '등락' in c), '등락률')
        amt_col = next((c for c in cols if '대금' in c), '거래대금')
        name_col = next((c for c in cols if '종목명' in c), '종목명')

        if name_col not in df.columns:
            df[name_col] = [stock.get_etf_ticker_name(ticker) for ticker in df.index]

        # 2. 제외 키워드 필터링 (for문 대신 정규표현식 사용으로 속도 최적화)
        pattern = '|'.join(EXCLUDE_KEYWORDS)
        df = df[~df[name_col].str.contains(pattern, na=False)]

        # 3. 데이터 형변환 및 오류값(NaN) 제거
        df[rate_col] = pd.to_numeric(df[rate_col], errors='coerce').fillna(0)
        df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)

        # 4. 등락률 0% 초과 종목만 추출 후 정렬
        top10_df = df[df[rate_col] > 0].sort_values(by=rate_col, ascending=False).head(10)

        # 5. 깔끔한 출력을 위한 리스트 조립
        results = []
        for _, row in top10_df.iterrows():
            results.append({
                '종목명': row[name_col],
                '상승률(%)': float(row[rate_col]),
                '거래대금(억)': round(float(row[amt_col]) / 100_000_000, 1)
            })

        return pd.DataFrame(results)

# ==========================================
# 🚀 3. 디스코드 전송 및 메인 실행
# ==========================================
def send_discord(df_result, target_date):
    if df_result.empty:
        msg = f"⚠️ **[{target_date}]** 조건에 맞는 상승 종목이 없습니다."
    else:
        df_display = df_result.copy()
        df_display['상승률(%)'] = df_display['상승률(%)'].apply(lambda x: f"{x:.2f}%")
        
        msg = f"🚀 **[국내 주도주 ETF 상승률 TOP 10]** ({target_date})\n"
        msg += "```text\n"
        msg += df_display.to_string(index=False) + "\n"
        msg += "```\n"

    try:
        requests.post(WEBHOOK_URL, json={"content": msg})
        print("✉️ 디스코드 메시지 전송 성공!")
    except Exception as e:
        print(f"❌ 전송 실패: {e}")

def main():
    KST = timezone(timedelta(hours=9))
    today = datetime.now(KST)
    
    # 주말 작동 방지 로직
    if today.weekday() >= 5:
        print("💤 주말입니다. 분석을 쉬어갑니다.")
        return

    target_date = today.strftime("%Y%m%d")
    display_date = today.strftime("%Y-%m-%d")

    try:
        tracker = ETFTracker(target_date)
        tracker.fetch_data()
        final_df = tracker.process_data()
        
        print("\n📊 [분석 결과]")
        print(final_df)
        
        send_discord(final_df, display_date)

    except Exception as e:
        error_msg = f"❌ 시스템 에러: {e}"
        print(error_msg)
        # 치명적 에러 발생 시 디스코드로 즉시 알림 전송
        requests.post(WEBHOOK_URL, json={"content": error_msg}) 

if __name__ == "__main__":
    main()
