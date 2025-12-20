"""
[전략 요약: 업비트 솔라나(SOL) 밀착 추적 그리드 봇]

1. 기본 환경
   - 대상: KRW-SOL (솔라나)
   - 방식: 촘촘한 스캘핑 (Grid Scalping)
   - 시드: 그리드당 10,000원 (설정에 따라 변경 가능)

2. 매수 및 추적 전략 (Crawling Grid)
   - 진입: 현재가 기준 아래로 0.3% 간격, 총 5단계 매수 그물망 상시 유지
   - 하락 시: 매수 체결 시 즉시 익절 주문을 걸고, 맨 아래에 새로운 그물 보충
   - 상승 시: 가격이 올라 그물이 멀어지면, 최하단 주문을 취소하여 현재가 밑으로 '전진 배치'
   - 특징: 가격을 자석처럼 밀착 추적하여 횡보 및 완만한 상승장에서 수익 극대화

3. 청산 및 관리 전략
   - 익절(TP): 각 매수 건별 +0.5% 지정가 매도 (수수료 제외 약 0.4% 순수익)
   - 꼬임 방지: 매수-매도 주문을 1:1로 매칭(Order Pairing)하여 잔고 섞임 원천 차단
   - 부하 관리: 3~5초 단위 상태 체크로 업비트 API 호출 제한(Rate Limit) 준수
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import json
import threading
import traceback
from decimal import Decimal, getcontext
from datetime import datetime
import pyupbit
from pyupbit import WebSocketManager

getcontext().prec = 28

# ==========================================
# [사용자 설정 영역]
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL           = "KRW-SOL"
BUY_AMOUNT_KRW   = Decimal("10000")  # 1만원씩
GRID_GAP_PCT     = Decimal("0.003")  # 0.3% 간격
PROFIT_PCT       = Decimal("0.005")  # 0.5% 익절
MAX_LAYERS       = 5                 # 항상 유지할 그물 개수
# ==========================================

def logj(ev, **kwargs):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(json.dumps({"ts": now, "ev": ev, **kwargs}, ensure_ascii=False), flush=True)

def adjust_price(price):
    p = Decimal(str(price))
    if p >= 2000000:   tick = Decimal("1000")
    elif p >= 500000:  tick = Decimal("500")
    elif p >= 100000:  tick = Decimal("100")
    elif p >= 10000:   tick = Decimal("10")
    else: tick = Decimal("1")
    return (p / tick).to_integral_value(rounding='ROUND_FLOOR') * tick

class CrawlingGridBot:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {} # {buy_uuid: sell_price}
        self.current_price = Decimal("0")

    def maintain_grid(self):
        try:
            with self.lock:
                # 1. 현재 대기 중인 매수 주문들 가져오기 (가격순 정렬)
                orders = self.upbit.get_order(SYMBOL, state='wait') or []
                buy_orders = sorted([o for o in orders if o['side'] == 'bid'], 
                                    key=lambda x: Decimal(str(x['price'])), reverse=True)

                # 2. 상승 시: 가장 위 주문이 현재가와 너무 멀어지면, 가장 아래 주문 취소 후 위에 새로 깔기
                if buy_orders:
                    highest_buy = Decimal(str(buy_orders[0]['price']))
                    # 현재가와 1번 그리드의 간격이 0.3%보다 커졌다면 (상승 중)
                    if (self.current_price - highest_buy) / self.current_price > GRID_GAP_PCT:
                        # 가장 낮은 가격의 주문(마지막 주문) 취소
                        lowest_order = buy_orders[-1]
                        self.upbit.cancel_order(lowest_order['uuid'])
                        if lowest_order['uuid'] in self.grid_map:
                            del self.grid_map[lowest_order['uuid']]
                        logj("shift_up", msg="Price rose. Moving bottom grid to top.")
                        # 리스트에서 제거해서 아래 '보충' 로직이 작동하게 함
                        buy_orders.pop()

                # 3. 부족한 그물 보충 (하락해서 체결됐거나, 상승해서 취소했을 때 실행됨)
                needed = MAX_LAYERS - len(buy_orders)
                if needed <= 0: return

                for i in range(1, MAX_LAYERS + 1):
                    # 현재가 바로 밑(1단계)부터 순차적으로 빈 자리 채우기
                    target_p = adjust_price(self.current_price * (Decimal("1") - GRID_GAP_PCT * i))
                    
                    # 이미 해당 가격대에 주문이 있으면 패스
                    if any(abs(Decimal(str(o['price'])) - target_p) < 10 for o in buy_orders):
                        continue

                    # 매수 주문 실행
                    vol = BUY_AMOUNT_KRW / target_p
                    res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                    if res and 'uuid' in res:
                        sell_p = adjust_price(target_p * (Decimal("1") + PROFIT_PCT))
                        self.grid_map[res['uuid']] = {"sell_price": sell_p}
                        logj("place_buy", price=str(target_p), sell_at=str(sell_p))
                        break # 한 번에 하나씩만 보충 (부하 방지)
                    time.sleep(0.1)

        except Exception:
            logj("err_maintain", trace=traceback.format_exc())

    def check_fill(self):
        """체결된 매수 주문이 있으면 즉시 익절 매도 주문"""
        try:
            dones = self.upbit.get_order(SYMBOL, state='done') or []
            with self.lock:
                for o in dones:
                    uid = o['uuid']
                    if uid in self.grid_map:
                        sell_p = self.grid_map[uid]['sell_price']
                        vol = o['executed_volume']
                        s_res = self.upbit.sell_limit_order(SYMBOL, float(sell_p), float(vol))
                        if s_res:
                            logj("place_sell", price=str(sell_p))
                            del self.grid_map[uid]
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        logj("bot_start", msg="Crawling Grid System Active (5 Layers)")
        wm = WebSocketManager("ticker", [SYMBOL])
        last_t = 0
        while True:
            data = wm.get()
            if not data: continue
            self.current_price = Decimal(str(data['trade_price']))
            
            # 3초마다 그물 유지보수 (상승 추적 및 하락 보충)
            if time.time() - last_t > 3:
                self.maintain_grid()
                self.check_fill()
                last_t = time.time()

if __name__ == "__main__":
    CrawlingGridBot().run()