import FinanceDataReader as fdr
from pykrx import stock
import requests
import pandas as pd
from datetime import datetime
import time

# 요청하신 새로운 디스코드 웹후크 설정
IGYEOK_WEBHOOK_URL = "https://discord.com/api/webhooks/1463876197027942514/N9wyH6wL3HKmMSFxNjL1nSbjuoc6q0cZ_nNi9iPILmDecmiIzjU9gDAgGKpUV0A_fSzl"

def send_discord_message(content):
    """디스코드 메시지 전송 (2000자 제한 대응 및 안전 전송)"""
    if not content or len(content.strip()) < 10: return
    try:
        if len(content) > 1900:
            chunks = [content[i:i+1900] for i in range(0, len(content), 1900)]
            for chunk in chunks:
                requests.post(IGYEOK_WEBHOOK_URL, json={'content': chunk})
                time.sleep(1) # 전송 안정성을 위해 1초 대기
        else:
            requests.post(IGYEOK_WEBHOOK_URL, json={'content': content})
    except Exception as e:
        print(f"메시지 전송 실패: {e}")

def main():
    print("🚀 [분석 시작] 4단계 리포트 생성 및 전송 중...")
    try:
        # 1. 대상 종목 리스트 확보 (KRX 전체 종목)
        df_krx = fdr.StockListing('KRX')
        
        # 업종 정보 컬럼명 자동 매칭 (데이터 소스에 따라 다를 수 있음)
        sector_col = 'Sector' if 'Sector' in df_krx.columns else 'Industry'
        
        # 코스피/코스닥 상위 500개 추출
        df_kospi = df_krx[df_krx['Market']=='KOSPI'].head(50)
        df_kosdaq = df_krx[df_krx['Market']=='KOSDAQ'].head(50)
        target_codes = pd.concat([df_kospi, df_kosdaq])

        all_analyzed = []
        today_str = datetime.now().strftime("%Y%m%d")
        
        # 당일 수급 데이터 일괄 로드 (성능 최적화)
        purchase_df = stock.get_market_net_purchases_of_equities_by_ticker(today_str, today_str, "ALL")

        print(f"📡 {len(target_codes)}개 종목 이격도 분석 중...")

        for idx, row in target_codes.iterrows():
            code, name = row['Code'], row['Name']
            sector = row.get(sector_col, '기타 업종')
            
            # 영업이익 정보 (최신 공시 기준 흑자 여부 확인)
            op_profit = row.get('OperatingProfit', 0)
            try:
                op_profit = float(op_profit) if pd.notna(op_profit) else 0
            except:
                op_profit = 0

            try:
                # 최근 30일치 주가 데이터 분석
                df = fdr.DataReader(code).tail(30)
                if len(df) < 20: continue
                
                curr = df['Close'].iloc[-1]
                ma20 = df['Close'].rolling(window=20).mean().iloc[-1]
                
                if ma20 == 0 or pd.isna(ma20): continue
                disparity = round((curr / ma20) * 100, 1)

                # 1차 필터링: 이격도 95% 이하인 종목만 수집
                if disparity <= 95.0:
                    # 수급 데이터 매칭
                    inst = purchase_df.loc[code, '기관합계'] if code in purchase_df.index else 0
                    fore = purchase_df.loc[code, '외국인합계'] if code in purchase_df.index else 0
                    pen = purchase_df.loc[code, '연기금등'] if code in purchase_df.index else 0
                    
                    all_analyzed.append({
                        'name': name, 'code': code, 'disparity': disparity, 
                        'sector': sector, 'is_profit': op_profit > 0,
                        'inst': inst, 'fore': fore, 'pen': pen
                    })
            except:
                continue

        if not all_analyzed:
            send_discord_message("🔍 현재 조건(이격도 95% 이하)에 맞는 종목이 없습니다.")
            return

        # --- 리포트 1: 이격도 분석 결과 (기존 방식 유지) ---
        results_95 = sorted(all_analyzed, key=lambda x: x['disparity'])
        report1 = "### 📊 1. 이격도 분석 결과 (95% 이하)\n"
        for r in results_95[:50]:
            report1 += f"· **{r['name']}({r['code']})**: {r['disparity']}%\n"
        send_discord_message(report1)

        # --- 리포트 2: 테마분류표 ---
        report2 = "### 📋 2. 1번 기업들 테마분류표\n"
        report2 += "| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        results_sector = sorted(all_analyzed, key=lambda x: x['sector'])
        for r in results_sector[:40]:
            report2 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(report2)

        # --- 리포트 3: 흑자기업 필터링 (적자 제외) ---
        profit_only = [r for r in all_analyzed if r['is_profit']]
        report3 = "### 📉 3. 흑자기업 필터링 리스트 (적자 제외)\n"
        report3 += "| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        if not profit_only:
            report3 += "| - | 해당되는 흑자 기업 없음 | - |\n"
        else:
            for r in sorted(profit_only, key=lambda x: x['sector'])[:40]:
                report3 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(report3)

        # --- 리포트 4: 당일 수급 정리표 ---
        report4 = "### 💰 4. 3번 기업들 당일 수급 현황 (기관/외인/연기금)\n"
        report4 += "| 종목명 | 기관 | 외국인 | 연기금 |\n| --- | --- | --- | --- |\n"
        # 3번 리스트(흑자기업)를 기준으로 수급 출력
        source_list = profit_only if profit_only else all_analyzed
        for r in source_list[:40]:
            report4 += f"| {r['name']} | {r['inst']:,} | {r['fore']:,} | {r['pen']:,} |\n"
        send_discord_message(report4)

        print("✅ 모든 분석 리포트 전송이 완료되었습니다.")

    except Exception as e:
        print(f"❌ 분석 실행 중 에러 발생: {e}")

if __name__ == "__main__":
    main()
