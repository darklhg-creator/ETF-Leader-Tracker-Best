import FinanceDataReader as fdr
from pykrx import stock
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import time

IGYEOK_WEBHOOK_URL = "https://discord.com/api/webhooks/1463876197027942514/N9wyH6wL3HKmMSFxNjL1nSbjuoc6q0cZ_nNi9iPILmDecmiIzjU9gDAgGKpUV0A_fSzl"

def send_discord_message(content):
    if not content or len(content.strip()) < 10: return
    try:
        if len(content) > 1900:
            chunks = [content[i:i+1900] for i in range(0, len(content), 1900)]
            for chunk in chunks:
                requests.post(IGYEOK_WEBHOOK_URL, json={'content': chunk})
                time.sleep(1)
        else:
            requests.post(IGYEOK_WEBHOOK_URL, json={'content': content})
    except Exception as e:
        print(f"전송 에러: {e}")

def get_detailed_info(code):
    """네이버 금융에서 업종 및 영업이익 직접 확인"""
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    try:
        res = requests.get(url, timeout=5)
        soup = BeautifulSoup(res.text, 'lxml')
        
        # 업종 정보 추출
        h4_tags = soup.find_all('h4')
        sector = "기타"
        for h4 in h4_tags:
            if '업종명' in h4.text:
                sector = h4.find_next('em').text.strip()
                break
        
        # 재무 정보 (영업이익) 추출 - 최근 결산 기준
        is_profit = False
        table = soup.find('table', {'class': 'tb_type1 tb_num'})
        if table:
            profit_row = table.find('th', string='영업이익')
            if profit_row:
                # 최근 결산 년도 데이터 확인 (보통 첫 번째 데이터 열)
                target_td = profit_row.find_next('td')
                if target_td:
                    val = target_td.text.replace(',', '').strip()
                    if val and val != '-' and int(val) > 0:
                        is_profit = True
        return sector, is_profit
    except:
        return "기타", False

def main():
    print("🚀 [정밀 분석 시작] 이격도 + 네이버 재무 + 수급 데이터 매칭")
    try:
        df_krx = fdr.StockListing('KRX')
        df_kospi = df_krx[df_krx['Market']=='KOSPI'].head(400)
        df_kosdaq = df_krx[df_krx['Market']=='KOSDAQ'].head(400)
        target_codes = pd.concat([df_kospi, df_kosdaq])

        all_analyzed = []
        today = datetime.now().strftime("%Y%m%d")
        purchase_df = stock.get_market_net_purchases_of_equities_by_ticker(today, today, "ALL")

        for idx, row in target_codes.iterrows():
            code, name = row['Code'], row['Name']
            try:
                # 1단계: 이격도 분석 (가장 빠름)
                df = fdr.DataReader(code).tail(25)
                if len(df) < 20: continue
                curr = df['Close'].iloc[-1]
                ma20 = df['Close'].rolling(window=20).mean().iloc[-1]
                disparity = round((curr / ma20) * 100, 1)

                if disparity <= 95.0:
                    # 2단계: 상세 정보(업종, 재무) 확보 - 필터링된 종목에 대해서만 수행
                    sector, is_profit = get_detailed_info(code)
                    
                    # 3단계: 수급 데이터 매칭
                    inst = purchase_df.loc[code, '기관합계'] if code in purchase_df.index else 0
                    fore = purchase_df.loc[code, '외국인합계'] if code in purchase_df.index else 0
                    pen = purchase_df.loc[code, '연기금등'] if code in purchase_df.index else 0
                    
                    all_analyzed.append({
                        'name': name, 'code': code, 'disparity': disparity, 
                        'sector': sector, 'is_profit': is_profit,
                        'inst': inst, 'fore': fore, 'pen': pen
                    })
            except: continue

        if not all_analyzed:
            send_discord_message("🔍 현재 조건에 맞는 종목이 없습니다.")
            return

        # --- 1. 이격도 결과 ---
        rep1 = "### 📊 1. 이격도 분석 결과 (95% 이하)\n"
        for r in sorted(all_analyzed, key=lambda x: x['disparity'])[:50]:
            rep1 += f"· **{r['name']}({r['code']})**: {r['disparity']}%\n"
        send_discord_message(rep1)

        # --- 2. 테마분류표 ---
        rep2 = "### 📋 2. 1번 기업들 테마분류표\n| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        for r in sorted(all_analyzed, key=lambda x: x['sector'])[:40]:
            rep2 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(rep2)

        # --- 3. 흑자기업 필터링 ---
        profit_only = [r for r in all_analyzed if r['is_profit']]
        rep3 = "### 📉 3. 흑자기업 필터링 (적자 제외)\n| 테마(업종) | 종목명 | 이격도 |\n| --- | --- | --- |\n"
        if not profit_only:
            rep3 += "| - | 흑자 기업 데이터 수집 중... | - |\n"
        else:
            for r in sorted(profit_only, key=lambda x: x['sector'])[:40]:
                rep3 += f"| {r['sector']} | {r['name']} | {r['disparity']}% |\n"
        send_discord_message(rep3)

        # --- 4. 당일 수급 현황 ---
        rep4 = "### 💰 4. 3번 기업들 당일 수급 현황 (기관/외인/연기금)\n| 종목명 | 기관 | 외국인 | 연기금 |\n| --- | --- | --- | --- |\n"
        source = profit_only if profit_only else all_analyzed
        for r in source[:40]:
            rep4 += f"| {r['name']} | {r['inst']:,} | {r['fore']:,} | {r['pen']:,} |\n"
        send_discord_message(rep4)

    except Exception as e:
        print(f"❌ 에러: {e}")

if __name__ == "__main__":
    main()
