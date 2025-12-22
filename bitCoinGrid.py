#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[전략 요약: 업비트 솔라나(SOL) 엔터프라이즈급 밀착 추적 그리드 봇]

1. 기본 환경 및 자산 관리
   - 대상: KRW-SOL (솔라나)
   - 방식: 촘촘한 스캘핑 (Grid Scalping) 기반의 자석식 추적
   - 예산: 그리드당 250,000원 x 40슬롯 = 총 1,000만 원 운용 (설정 가변)
   - 사전 점검: 가동 시 보유 현금(KRW) 및 주문 가능 금액 실시간 모니터링

2. 크롤링 그리드(Crawling Grid) 추적 전략
   - 진입: 현재가 기준 하단 0.3% 간격으로 5단계 그물망 상시 유지
   - 전진 배치(Shift Up): 가격 상승으로 그물이 멀어지면 최하단 주문을 취소 후 현재가 밑으로 밀착 전진
   - 하락 대응: 매수 체결 시 즉시 +0.5% 지정가 익절 매도(TP) 실행 및 하단 그물 자동 보충

3. 엔터프라이즈급 안정성 및 예외 처리 (v2.5~v2.8 핵심)
   - 부분 체결 구제: Shift Up 시 미체결 취소 전, 이미 체결된 수량(Partial Fill)은 즉각 매도로 전환하여 자산 미아 방지
   - 상태 보존(Persistence): grid_state.json 파일을 통해 매수/매도 장부를 실시간 저장 및 재시작 시 100% 자동 복구
   - API 보호: 호출 간 0.1~0.2s 지연(Sleep) 및 루프 예외 처리를 통해 업비트 Rate Limit 차단 및 24시간 무중단 가동 보장
   - 데이터 무결성: Decimal 연산 및 Atomic Write(임시 파일 교체 방식) 적용으로 장부 깨짐 방지
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
BUY_AMOUNT_KRW   = Decimal("10000")   # 테스트 완료 후 250000으로 수정하세요
GRID_GAP_PCT     = Decimal("0.003") 
PROFIT_PCT       = Decimal("0.005") 
MAX_LAYERS       = 5                
MAX_INVENTORY    = 40               
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

class EnterpriseFinalBotV28:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {} 
        self.current_price = Decimal("0")
        self.load_state()

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
        if not os.path.exists(STATE_FILE):
            return
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
            # [신규] 시작 시 지갑 잔고 확인 로그
            krw_balance = self.upbit.get_balance("KRW")
            logj("wallet_status", 
                 available_krw=format(int(krw_balance), ','), 
                 buy_unit=format(int(BUY_AMOUNT_KRW), ','),
                 msg="Checking funds before start...")

            logj("init_clear", msg="Cleaning old bid orders...")
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            
            for o in orders:
                if o['side'] == 'bid' and o['uuid'] not in self.grid_map:
                    self.upbit.cancel_order(o['uuid'])
                    time.sleep(0.2)

            if not self.grid_map:
                curr = pyupbit.get_current_price(SYMBOL)
                if not curr: return
                seed_p = adjust_price(curr)
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
                        
                        if uuid_to_cancel in self.grid_map:
                            del self.grid_map[uuid_to_cancel]
                        
                        logj("shift_up", cancelled=str(lowest_order['price']))
                        self.save_state()
                        buy_orders.pop()

                existing_prices = [info['buy_price'] for info in self.grid_map.values()]
                for i in range(1, MAX_LAYERS + 1):
                    target_p = adjust_price(self.current_price * (Decimal("1") - GRID_GAP_PCT * i))
                    min_dist = target_p * Decimal("0.0015")
                    if any(abs(p - target_p) < min_dist for p in existing_prices): continue

                    if len(buy_orders) < MAX_LAYERS and len(self.grid_map) < MAX_INVENTORY:
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
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
                        if info['side'] == 'bid':
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(o['executed_volume']))
                            if s_res:
                                del self.grid_map[uid]
                                self.grid_map[s_res['uuid']] = {'buy_price': info['buy_price'], 'sell_price': info['sell_price'], 'side': 'ask'}
                                logj("place_sell", price=str(info['sell_price']))
                                changed = True
                        elif info['side'] == 'ask':
                            del self.grid_map[uid]
                            logj("trade_success", price=str(info['sell_price']))
                            changed = True
                if changed: self.save_state()
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        self.init_clear_and_seed()
        logj("bot_start", msg=f"v2.8 Enterprise Ready. Max: {MAX_INVENTORY}")
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
    EnterpriseFinalBotV28().run()