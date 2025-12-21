#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
BUY_AMOUNT_KRW   = Decimal("10000")
GRID_GAP_PCT     = Decimal("0.003") 
PROFIT_PCT       = Decimal("0.005") 
MAX_LAYERS       = 5                # 현재가 밑에 유지할 실시간 그물 수
MAX_INVENTORY    = 40               # 총 운용 그리드 수 (40만원 예산)
# ==========================================

def logj(ev, **kwargs):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(json.dumps({"ts": now, "ev": ev, **kwargs}, ensure_ascii=False), flush=True)

def adjust_price(price):
    p = Decimal(str(price))
    if p >= 100000: tick = Decimal("100")
    elif p >= 10000: tick = Decimal("10")
    else: tick = Decimal("1")
    return (p / tick).to_integral_value(rounding='ROUND_FLOOR') * tick

class FinalStableGridBot:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        # grid_map: { 'UUID': {'buy_price': 186000, 'sell_price': 187000, 'side': 'bid'|'ask'} }
        self.grid_map = {} 
        self.current_price = Decimal("0")

    def init_clear_and_seed(self):
        try:
            logj("init_clear", msg="Cleaning old orders...")
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    self.upbit.cancel_order(o['uuid'])
                    time.sleep(0.1)

            curr = Decimal(str(pyupbit.get_current_price(SYMBOL)))
            seed_p = adjust_price(curr)
            vol = BUY_AMOUNT_KRW / seed_p
            
            res = self.upbit.buy_limit_order(SYMBOL, float(seed_p), float(vol))
            if res and 'uuid' in res:
                sell_p = adjust_price(seed_p * (Decimal("1") + PROFIT_PCT))
                self.grid_map[res['uuid']] = {'buy_price': seed_p, 'sell_price': sell_p, 'side': 'bid'}
                logj("seed_buy_placed", price=str(seed_p), target_sell=str(sell_p))
        except Exception:
            logj("err_init", trace=traceback.format_exc())

    def maintain_grid(self):
        try:
            with self.lock:
                # 1. 인벤토리 체크 (현재 장부에 적힌 모든 주문 수)
                if len(self.grid_map) >= MAX_INVENTORY:
                    # 너무 자주 찍히지 않게 현재가 로그만 가끔 노출
                    return

                # 2. 대기 매수 주문 추출
                orders = self.upbit.get_order(SYMBOL, state='wait') or []
                buy_orders = sorted([o for o in orders if o['side'] == 'bid'], 
                                    key=lambda x: Decimal(str(x['price'])), reverse=True)

                # 3. 가격 상승 추적 (Shift Up)
                if buy_orders:
                    highest_buy = Decimal(str(buy_orders[0]['price']))
                    if (self.current_price - highest_buy) / self.current_price > (GRID_GAP_PCT + Decimal("0.001")):
                        lowest_order = buy_orders[-1]
                        self.upbit.cancel_order(lowest_order['uuid'])
                        
                        # [버그 수정] 취소된 주문은 장부에서도 즉시 제거 (유령 주문 방지)
                        if lowest_order['uuid'] in self.grid_map:
                            del self.grid_map[lowest_order['uuid']]
                        
                        logj("shift_up", cancelled=str(lowest_order['price']))
                        buy_orders.pop()

                # 4. 중복 매수 방지용 가격 리스트 (이미 예약했거나 들고 있는 가격)
                existing_prices = [info['buy_price'] for info in self.grid_map.values()]

                # 5. 그물망 유지
                for i in range(1, MAX_LAYERS + 1):
                    target_p = adjust_price(self.current_price * (Decimal("1") - GRID_GAP_PCT * i))
                    min_dist = target_p * Decimal("0.0015")
                    
                    # 장부상 가격과 겹치면 패스
                    if any(abs(p - target_p) < min_dist for p in existing_prices):
                        continue

                    if len(buy_orders) < MAX_LAYERS and len(self.grid_map) < MAX_INVENTORY:
                        if self.upbit.get_balance("KRW") < float(BUY_AMOUNT_KRW):
                            return

                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            sell_p = adjust_price(target_p * (Decimal("1") + PROFIT_PCT))
                            # 장부에 매수 주문 등록
                            self.grid_map[res['uuid']] = {'buy_price': target_p, 'sell_price': sell_p, 'side': 'bid'}
                            logj("place_buy", price=str(target_p), sell_at=str(sell_p))
                            buy_orders.append({'price': target_p})
                            existing_prices.append(target_p)
                        time.sleep(0.1)
        except Exception:
            logj("err_maintain", trace=traceback.format_exc())

    def check_fill(self):
        try:
            # 최근 완료된 주문 20개 확인
            dones = self.upbit.get_order(SYMBOL, state='done') or []
            with self.lock:
                for o in dones:
                    uid = o['uuid']
                    if uid in self.grid_map:
                        info = self.grid_map[uid]
                        
                        # CASE A: 매수 완료 -> 매도 주문 실행
                        if info['side'] == 'bid':
                            sell_p = info['sell_price']
                            vol = o['executed_volume']
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(sell_p), float(vol))
                            if s_res and 'uuid' in s_res:
                                # [중요] 기존 매수 UUID 지우고 새 매도 UUID로 명찰 교체
                                del self.grid_map[uid]
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 
                                    'sell_price': sell_p, 
                                    'side': 'ask'
                                }
                                logj("place_sell", price=str(sell_p), buy_p=str(info['buy_price']))
                        
                        # CASE B: 매도 완료 -> 장부에서 완전히 제거 (슬롯 해제)
                        elif info['side'] == 'ask':
                            del self.grid_map[uid]
                            logj("trade_success", sell_price=str(info['sell_price']))

        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        self.init_clear_and_seed()
        logj("bot_start", msg=f"v2.4 Active. Max Inventory: {MAX_INVENTORY}")
        wm = WebSocketManager("ticker", [SYMBOL])
        last_t = 0
        while True:
            data = wm.get()
            if not data: continue
            self.current_price = Decimal(str(data['trade_price']))
            
            if time.time() - last_t > 4:
                self.maintain_grid()
                self.check_fill()
                last_t = time.time()

if __name__ == "__main__":
    FinalStableGridBot().run()