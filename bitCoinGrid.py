#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[전략 요약: KRW-SOL 액티브 미트(Active Meat) v3.2]

1. 순수 데이터 기반 태세 전환 (Time-Stagnation Removed)
   - 시간 정체 조건을 제거하여 시장이 조용해도 NORMAL 모드에서 지속적으로 수익을 추구합니다.
   - 오직 MDD(낙폭)와 VP(체결강도) 데이터만으로 리스크를 판단합니다.

2. 30분 단위 예리한 체결강도 분석 (30-min VP Window)
   - 분석 윈도우를 60분에서 30분으로 단축하여 시장의 급변에 기민하게 반응합니다.

3. 500만 원 거래량 노이즈 필터 (Noise Filter)
   - 30분간 총 거래대금이 500만 원 미만일 경우, 발생하는 체결강도 수치를 무시하고
     NORMAL 상태를 유지하여 소액 노이즈에 의한 오작동을 방지합니다.

4. 밸런스형 수익 구간 확보
   - 솔라나 특성을 반영하여 MDD -5%까지는 NORMAL 모드를 유지, 수익의 '살코기'를 극대화합니다.
"""

import os
import time
import json
import threading
import traceback
from decimal import Decimal, getcontext
from datetime import datetime
from collections import deque
import pyupbit
from pyupbit import WebSocketManager

getcontext().prec = 28

# ==========================================
# [사용자 설정 영역] - Config
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL           = "KRW-SOL"
BUY_AMOUNT_KRW   = Decimal("250000") # 슬롯당 25만 원
BASE_GRID_GAP    = Decimal("0.003") # 0.3%
PROFIT_PCT       = Decimal("0.005") # 0.5%

# --- [리스크 판단 엔진 설정] ---
VP_WINDOW_SEC      = 1800      # 체결강도 분석 범위: 30분
MIN_TOTAL_VOL_KRW  = 5000000   # 최소 신뢰 거래대금: 500만 원 (노이즈 필터)

# --- [모드 전환 임계값 (Active Meat)] ---
# 1. CAUTION (주의): -5% 하락 OR 체결강도 40% 미만
CAUTION_MDD      = Decimal("-0.05")
CAUTION_VP       = Decimal("40.0")

# 2. DEFENSIVE (방어): -10% 하락 OR 체결강도 25% 미만
DEFENSIVE_MDD    = Decimal("-0.10")
DEFENSIVE_VP     = Decimal("25.0")

# 3. FREEZE (정지): -20% 하락 OR 체결강도 10% 미만
FREEZE_MDD       = Decimal("-0.20")
FREEZE_VP        = Decimal("10.0")

# --- [시스템 제한] ---
MAX_LAYERS       = 5
MAX_INVENTORY    = 1000
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

class EnterpriseShieldBotV3_2:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {}
        self.current_price = Decimal("0")
        
        self.current_mode = "NORMAL"
        self.last_mode = "NORMAL"
        self.rolling_24h_high = Decimal("0")
        self.last_high_update = 0
        self.trade_history = deque() # (timestamp, volume, side)
        self.cumulative_net_profit = Decimal("0")
        
        self.load_state()

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

    def get_total_asset(self):
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
                elif coin == SYMBOL.split('-')[1]:
                    total += (Decimal(b['balance']) + Decimal(b['locked'])) * curr_p_dec
            return total
        except Exception:
            logj("err_get_asset", trace=traceback.format_exc())
            return Decimal("0")

    def update_24h_high(self):
        if time.time() - self.last_high_update < 300: return
        try:
            df = pyupbit.get_ohlcv(SYMBOL, interval="minute60", count=24)
            if df is not None:
                self.rolling_24h_high = Decimal(str(df['high'].max()))
                self.last_high_update = time.time()
        except: pass

    def get_volume_power(self):
        """30분 윈도우 + 500만 원 노이즈 필터 적용 체결강도 계산"""
        now = time.time()
        while self.trade_history and now - self.trade_history[0][0] > VP_WINDOW_SEC:
            self.trade_history.popleft()
        
        buy_vol = sum(vol for ts, vol, side in self.trade_history if side == 'BID')
        sell_vol = sum(vol for ts, vol, side in self.trade_history if side == 'ASK')
        
        # 총 거래대금(KRW) 계산
        total_vol_krw = (Decimal(str(buy_vol)) + Decimal(str(sell_vol))) * self.current_price
        
        # [노이즈 필터링] 총 거래량이 임계값 미만이면 시장이 조용한 것이므로 100% 반환
        if total_vol_krw < MIN_TOTAL_VOL_KRW:
            return Decimal("100.0")
            
        if sell_vol == 0: return Decimal("100.0")
        return (Decimal(str(buy_vol)) / Decimal(str(sell_vol))) * 100

    def decide_mode(self):
        """시간 조건 완전 제거: MDD와 체결강도만으로 판단"""
        if self.rolling_24h_high == 0: return "NORMAL"
        
        mdd = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high
        v_power = self.get_volume_power()
        
        if mdd <= FREEZE_MDD or v_power < FREEZE_VP:
            return "FREEZE"
        if mdd <= DEFENSIVE_MDD or v_power < DEFENSIVE_VP:
            return "DEFENSIVE"
        if mdd <= CAUTION_MDD or v_power < CAUTION_VP:
            return "CAUTION"
        
        return "NORMAL"

    def reset_buy_grid(self):
        with self.lock:
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    self.upbit.cancel_order(o['uuid'])
                    time.sleep(0.1)
            self.grid_map = {uid: info for uid, info in self.grid_map.items() if info['side'] == 'ask'}
            self.save_state()
            logj("mode_reset_execution", mode=self.current_mode, msg="Grid re-aligned for Active Meat strategy.")

    def maintain_grid(self):
        try:
            mode_multipliers = {"NORMAL": 1.0, "CAUTION": 2.0, "DEFENSIVE": 3.3, "FREEZE": 999.0}
            current_gap = BASE_GRID_GAP * Decimal(str(mode_multipliers.get(self.current_mode, 1.0)))
            
            with self.lock:
                if self.current_mode == "FREEZE": return 
                if len(self.grid_map) >= MAX_INVENTORY: return
                
                balance = Decimal(str(self.upbit.get_balance("KRW")))
                orders = self.upbit.get_order(SYMBOL, state='wait') or []
                buy_orders = sorted([o for o in orders if o['side'] == 'bid'], 
                                    key=lambda x: Decimal(str(x['price'])), reverse=True)

                if buy_orders and self.current_mode == "NORMAL":
                    highest_buy = Decimal(str(buy_orders[0]['price']))
                    if (self.current_price - highest_buy) / self.current_price > (current_gap + Decimal("0.001")):
                        lowest_order = buy_orders[-1]
                        self.upbit.cancel_order(lowest_order['uuid'])
                        if lowest_order['uuid'] in self.grid_map: del self.grid_map[lowest_order['uuid']]
                        logj("shift_up", cancelled=str(lowest_order['price']))
                        buy_orders.pop()

                existing_prices = [info['buy_price'] for info in self.grid_map.values()]
                for i in range(1, MAX_LAYERS + 1):
                    if balance < (BUY_AMOUNT_KRW * Decimal("1.0005")): break
                    target_p = adjust_price(self.current_price * (Decimal("1") - current_gap * i))
                    if any(abs(p - target_p) < (target_p * (current_gap * Decimal("0.5"))) for p in existing_prices): continue

                    if len(buy_orders) < MAX_LAYERS:
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            sell_p = adjust_price(target_p * (Decimal("1") + PROFIT_PCT))
                            self.grid_map[res['uuid']] = {'buy_price': target_p, 'sell_price': sell_p, 'side': 'bid'}
                            logj("place_buy", price=str(target_p), mode=self.current_mode)
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
                            buy_cost = info['buy_price'] * vol
                            sell_rev = price * vol
                            net_profit = (sell_rev * Decimal("0.9995")) - (buy_cost * Decimal("1.0005"))
                            self.cumulative_net_profit += net_profit
                            
                            logj("trade_success", buy=format(int(info['buy_price']), ','), sell=format(int(price), ','),
                                 profit=str(round(net_profit, 2)), session_total=str(round(self.cumulative_net_profit, 2)))
                            
                            if self.current_mode != "NORMAL":
                                logj("recovery_to_normal", msg="Profit hit. Mode -> NORMAL.")
                                self.current_mode = "NORMAL"
                            changed = True
                if changed: self.save_state()
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        total_asset = self.get_total_asset()
        logj("bot_start", v="3.2 ActiveMeat", base_asset=format(int(total_asset), ','))

        wm = WebSocketManager("ticker", [SYMBOL])
        last_loop_time = 0
        
        while True:
            try:
                data = wm.get()
                if not data: continue
                self.current_price = Decimal(str(data['trade_price']))
                self.trade_history.append((time.time(), Decimal(str(data['trade_volume'])), data['ask_bid']))
                
                if time.time() - last_loop_time > 4:
                    self.update_24h_high()
                    self.current_mode = self.decide_mode()
                    
                    if self.current_mode != self.last_mode:
                        logj("mode_changed", old=self.last_mode, new=self.current_mode)
                        self.reset_buy_grid()
                        self.last_mode = self.current_mode
                    
                    self.maintain_grid()
                    self.check_fill()
                    
                    mdd_val = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high if self.rolling_24h_high > 0 else 0
                    logj("status", mode=self.current_mode, price=format(int(self.current_price), ','), 
                         high=format(int(self.rolling_24h_high), ','),
                         mdd=f"{round(mdd_val*100, 2)}%", v_power=f"{round(self.get_volume_power(), 1)}%")
                    last_loop_time = time.time()
            except Exception:
                logj("err_loop", trace=traceback.format_exc())
                time.sleep(2)

if __name__ == "__main__":
    EnterpriseShieldBotV3_2().run()