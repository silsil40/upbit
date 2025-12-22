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
BUY_AMOUNT_KRW   = Decimal("10000") # 그물망 기준금액
GRID_GAP_PCT     = Decimal("0.003") 
PROFIT_PCT       = Decimal("0.005") 
MAX_LAYERS       = 5                
MAX_INVENTORY    = 40               
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

class FinalStableGridBotV26:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {} 
        self.current_price = Decimal("0")

    def init_clear_and_seed(self):
        try:
            logj("init_clear", msg="Cleaning old orders...")
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    self.upbit.cancel_order(o['uuid'])
                    time.sleep(0.2) # 초기화 시 API 안정성 확보

            curr = pyupbit.get_current_price(SYMBOL)
            if not curr: return
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
                if len(self.grid_map) >= MAX_INVENTORY:
                    return

                orders = self.upbit.get_order(SYMBOL, state='wait') or []
                buy_orders = sorted([o for o in orders if o['side'] == 'bid'], 
                                    key=lambda x: Decimal(str(x['price'])), reverse=True)

                # 1. 가격 상승 추적 (Shift Up) 및 부분 체결 처리
                if buy_orders:
                    highest_buy = Decimal(str(buy_orders[0]['price']))
                    if (self.current_price - highest_buy) / self.current_price > (GRID_GAP_PCT + Decimal("0.001")):
                        lowest_order = buy_orders[-1]
                        uuid_to_cancel = lowest_order['uuid']
                        
                        # [조치] 취소 전 실제 체결량 확인
                        order_detail = self.upbit.get_order(uuid_to_cancel)
                        part_vol = Decimal(str(order_detail.get('executed_volume', '0')))
                        
                        # 주문 취소 실행
                        self.upbit.cancel_order(uuid_to_cancel)
                        time.sleep(0.1) # API Rate Limit 지연
                        
                        # [조치] 부분 체결분이 있다면 즉시 매도 주문으로 전환
                        if part_vol > 0 and uuid_to_cancel in self.grid_map:
                            info = self.grid_map[uuid_to_cancel]
                            sell_p = info['sell_price']
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(sell_p), float(part_vol))
                            if s_res and 'uuid' in s_res:
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 
                                    'sell_price': sell_p, 
                                    'side': 'ask'
                                }
                                logj("partial_to_sell", price=str(sell_p), vol=str(part_vol))
                        
                        # 장부에서 기존 매수 명찰 삭제
                        if uuid_to_cancel in self.grid_map:
                            del self.grid_map[uuid_to_cancel]
                        
                        logj("shift_up", cancelled=str(lowest_order['price']))
                        buy_orders.pop()

                # 2. 그물망 유지
                existing_prices = [info['buy_price'] for info in self.grid_map.values()]
                for i in range(1, MAX_LAYERS + 1):
                    target_p = adjust_price(self.current_price * (Decimal("1") - GRID_GAP_PCT * i))
                    min_dist = target_p * Decimal("0.0015")
                    
                    if any(abs(p - target_p) < min_dist for p in existing_prices):
                        continue

                    if len(buy_orders) < MAX_LAYERS and len(self.grid_map) < MAX_INVENTORY:
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            sell_p = adjust_price(target_p * (Decimal("1") + PROFIT_PCT))
                            self.grid_map[res['uuid']] = {'buy_price': target_p, 'sell_price': sell_p, 'side': 'bid'}
                            logj("place_buy", price=str(target_p), sell_at=str(sell_p))
                            buy_orders.append({'price': target_p})
                            existing_prices.append(target_p)
                        # [조치] 루프 내 API 호출 간 지연시간
                        time.sleep(0.1)
        except Exception:
            logj("err_maintain", trace=traceback.format_exc())

    def check_fill(self):
        try:
            dones = self.upbit.get_order(SYMBOL, state='done') or []
            with self.lock:
                for o in dones:
                    uid = o['uuid']
                    if uid in self.grid_map:
                        info = self.grid_map[uid]
                        if info['side'] == 'bid':
                            sell_p, vol = info['sell_price'], o['executed_volume']
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(sell_p), float(vol))
                            if s_res and 'uuid' in s_res:
                                del self.grid_map[uid]
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 
                                    'sell_price': sell_p, 
                                    'side': 'ask'
                                }
                                logj("place_sell", price=str(sell_p), buy_p=str(info['buy_price']))
                        elif info['side'] == 'ask':
                            del self.grid_map[uid]
                            logj("trade_success", sell_price=str(info['sell_price']))
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        self.init_clear_and_seed()
        logj("bot_start", msg=f"v2.6 Enterprise. Seed: {BUY_AMOUNT_KRW}")
        wm = WebSocketManager("ticker", [SYMBOL])
        last_t = 0
        while True:
            try:
                data = wm.get()
                if not data: continue
                self.current_price = Decimal(str(data['trade_price']))
                
                if time.time() - last_t > 4:
                    self.maintain_grid()
                    self.check_fill()
                    last_t = time.time()
            except Exception:
                # [조치] 루프 중단 방지용 예외 처리 및 재연결 대기
                logj("err_loop", trace=traceback.format_exc())
                time.sleep(2)

if __name__ == "__main__":
    FinalStableGridBotV26().run()