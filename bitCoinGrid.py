#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[전략 요약: 업비트 솔라나(SOL) 엔터프라이즈급 밀착 추적 그리드 봇]

1. 기본 환경 및 자산 관리
    - 대상: KRW-SOL (솔라나)
    - 방식: 촘촘한 스캘핑 (Grid Scalping) 기반의 자석식 추적
    - 예산: 그리드당 200,000원 x 1000슬롯 = 무제한 확장 대응
    - [신규] 동적 잔고 확인: 가동 시점의 총 자산(현금+코인)을 자동 계산하여 수익 기준점 수립

2. 크롤링 그리드(Crawling Grid) 추적 전략
    - 진입: 현재가 기준 하단 0.3% 간격으로 5단계 그물망 상시 유지
    - [복구] 보수적 간격 유지: 매수/매도 주문 구분 없이 모든 주문과의 거리를 체크하여 촘촘한 중복 매수 방지 (v2.8 로직)
    - 전진 배치(Shift Up): 가격 상승 시 최하단 주문을 취소 후 현재가 밑으로 밀착 전진
    - 하락 대응: 매수 체결 시 즉시 +0.5% 지정가 익절 매도(TP) 실행

3. 엔터프라이즈급 안정성 및 실시간 추적 (v2.9.3)
    - [신규] 순수익 로그: 매도 성공 시 양방향 수수료(0.1%)를 제외한 실제 현금 증가분(Net Profit) 기록
    - 부분 체결 구제: Shift Up 시 이미 체결된 수량은 즉각 매도로 전환하여 자산 미아 방지
    - 상태 보존: grid_state.json 파일을 통해 장부 실시간 저장 및 복구
    - API 보호: 호출 간 지연 및 루프 예외 처리를 통해 24시간 무중단 가동
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
BUY_AMOUNT_KRW   = Decimal("200000")   # 매수 기준 금액
GRID_GAP_PCT     = Decimal("0.003") 
PROFIT_PCT       = Decimal("0.005") 
MAX_LAYERS       = 5                
MAX_INVENTORY    = 1000                 # 맥스 매수 슬롯 1000개
STATE_FILE       = "grid_state.json"  
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

class EnterpriseFinalBotV292:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {} 
        self.current_price = Decimal("0")
        
        # [실시간 수익 추적용 변수]
        self.start_total_asset = Decimal("0") 
        self.cumulative_net_profit = Decimal("0")
        
        self.load_state()

    def get_total_asset(self):
        """현재 계좌의 총 가치(현금 + 주문중인 현금 + 보유 코인 평가액) 자동 계산"""
        try:
            balances = self.upbit.get_balances()
            total = Decimal("0")
            curr_p = pyupbit.get_current_price(SYMBOL)
            if not curr_p: return Decimal("0")
            curr_p_dec = Decimal(str(curr_p))

            for b in balances:
                coin = b['currency']
                if coin == "KRW":
                    total += (Decimal(b['balance']) + Decimal(b['locked']))
                elif coin == SYMBOL.split('-')[1]: # SOL 등 대상 코인
                    total += (Decimal(b['balance']) + Decimal(b['locked'])) * curr_p_dec
            
            return total
        except Exception:
            logj("err_get_asset", trace=traceback.format_exc())
            return Decimal("0")

    def save_state(self):
        try:
            serializable_map = {}
            for uid, info in self.grid_map.items():
                serializable_map[uid] = {
                    'buy_price': str(info['buy_price']),
                    'sell_price': str(info['sell_price']),
                    'side': info['side']
                }
            tmp_file = STATE_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(serializable_map, f, indent=4)
            os.replace(tmp_file, STATE_FILE)
        except Exception:
            logj("err_save", trace=traceback.format_exc())

    def load_state(self):
        if not os.path.exists(STATE_FILE): return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw_map = json.load(f)
                for uid, info in raw_map.items():
                    self.grid_map[uid] = {
                        'buy_price': Decimal(info['buy_price']),
                        'sell_price': Decimal(info['sell_price']),
                        'side': info['side']
                    }
            logj("state_loaded", count=len(self.grid_map))
        except Exception:
            logj("err_load", trace=traceback.format_exc())

    def init_clear_and_seed(self):
        try:
            self.start_total_asset = self.get_total_asset()
            krw_balance = Decimal(str(self.upbit.get_balance("KRW")))
            
            logj("bot_session_init", 
                 start_total_asset=format(int(self.start_total_asset), ','),
                 available_krw=format(int(krw_balance), ','),
                 msg="Session baseline established.")

            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid' and o['uuid'] not in self.grid_map:
                    self.upbit.cancel_order(o['uuid'])
                    time.sleep(0.2)

            if not self.grid_map:
                curr = pyupbit.get_current_price(SYMBOL)
                if not curr: return
                seed_p = adjust_price(curr)
                if krw_balance >= BUY_AMOUNT_KRW:
                    vol = BUY_AMOUNT_KRW / seed_p
                    res = self.upbit.buy_limit_order(SYMBOL, float(seed_p), float(vol))
                    if res and 'uuid' in res:
                        sell_p = adjust_price(seed_p * (Decimal("1") + PROFIT_PCT))
                        self.grid_map[res['uuid']] = {'buy_price': seed_p, 'sell_price': sell_p, 'side': 'bid'}
                        logj("seed_buy_placed", price=str(seed_p))
                        self.save_state()
        except Exception:
            logj("err_init", trace=traceback.format_exc())

    def maintain_grid(self):
        try:
            with self.lock:
                if len(self.grid_map) >= MAX_INVENTORY: return
                balance = Decimal(str(self.upbit.get_balance("KRW")))

                orders = self.upbit.get_order(SYMBOL, state='wait') or []
                buy_orders = sorted([o for o in orders if o['side'] == 'bid'], 
                                    key=lambda x: Decimal(str(x['price'])), reverse=True)

                if buy_orders:
                    highest_buy = Decimal(str(buy_orders[0]['price']))
                    if (self.current_price - highest_buy) / self.current_price > (GRID_GAP_PCT + Decimal("0.001")):
                        lowest_order = buy_orders[-1]
                        uuid_to_cancel = lowest_order['uuid']
                        detail = self.upbit.get_order(uuid_to_cancel)
                        part_vol = Decimal(str(detail.get('executed_volume', '0'))) if detail else Decimal("0")
                        
                        self.upbit.cancel_order(uuid_to_cancel)
                        time.sleep(0.1)
                        
                        if part_vol > 0 and uuid_to_cancel in self.grid_map:
                            info = self.grid_map[uuid_to_cancel]
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(part_vol))
                            if s_res:
                                self.grid_map[s_res['uuid']] = {'buy_price': info['buy_price'], 'sell_price': info['sell_price'], 'side': 'ask'}
                        
                        if uuid_to_cancel in self.grid_map: del self.grid_map[uuid_to_cancel]
                        logj("shift_up", cancelled=str(lowest_order['price']))
                        self.save_state()
                        buy_orders.pop()

                # [복구] v2.8 방식으로 수정: side 구분 없이 모든 장부상의 주문을 체크하여 안전 간격을 유지합니다.
                existing_prices = [info['buy_price'] for info in self.grid_map.values()]
                
                for i in range(1, MAX_LAYERS + 1):
                    if balance < (BUY_AMOUNT_KRW * Decimal("1.0005")): break
                    target_p = adjust_price(self.current_price * (Decimal("1") - GRID_GAP_PCT * i))
                    
                    # 매수/매도 주문 근처 0.15% 이내에는 주문을 내지 않음
                    if any(abs(p - target_p) < (target_p * Decimal("0.0015")) for p in existing_prices): continue

                    if len(buy_orders) < MAX_LAYERS:
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            balance -= (BUY_AMOUNT_KRW * Decimal("1.0005"))
                            sell_p = adjust_price(target_p * (Decimal("1") + PROFIT_PCT))
                            self.grid_map[res['uuid']] = {'buy_price': target_p, 'sell_price': sell_p, 'side': 'bid'}
                            logj("place_buy", price=str(target_p))
                            buy_orders.append({'price': target_p})
                            self.save_state()
                        time.sleep(0.1)
        except Exception:
            logj("err_maintain", trace=traceback.format_exc())

    def check_fill(self):
        try:
            dones = self.upbit.get_order(SYMBOL, state='done') or []
            changed = False
            with self.lock:
                for o in dones:
                    uid = o['uuid']
                    if uid in self.grid_map:
                        info = self.grid_map[uid]
                        vol = Decimal(str(o['executed_volume']))
                        price = Decimal(str(o['price']))

                        if info['side'] == 'bid':
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(vol))
                            if s_res:
                                del self.grid_map[uid]
                                self.grid_map[s_res['uuid']] = {'buy_price': info['buy_price'], 'sell_price': info['sell_price'], 'side': 'ask'}
                                logj("place_sell", price=str(info['sell_price']))
                                changed = True
                        elif info['side'] == 'ask':
                            del self.grid_map[uid]
                            
                            # --- [순수익 추적: Net Profit] ---
                            buy_cost = info['buy_price'] * vol
                            sell_rev = price * vol
                            net_profit = (sell_rev * Decimal("0.9995")) - (buy_cost * Decimal("1.0005"))
                            self.cumulative_net_profit += net_profit
                            
                            logj("trade_success", 
                                 buy=format(int(info['buy_price']), ','), 
                                 sell=format(int(price), ','),
                                 profit=str(round(net_profit, 2)), 
                                 session_total=str(round(self.cumulative_net_profit, 2)))
                            changed = True
                if changed: self.save_state()
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        self.init_clear_and_seed()
        logj("bot_start", msg=f"v2.9.3 Active. Baseline Asset: {format(int(self.start_total_asset), ',')}")
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
                logj("err_loop", trace=traceback.format_exc())
                time.sleep(2)

if __name__ == "__main__":
    EnterpriseFinalBotV292().run()