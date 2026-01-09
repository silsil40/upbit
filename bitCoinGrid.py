#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[KRW-SOL Active Meat v3.2.2 - 전략 명세서]

1. 로컬 메모리 우선(Local-First): 업비트 API보다 봇의 내부 수첩(grid_map)을 우선 신뢰하여 중복 주문 원천 차단
2. 주기적 정합성 체크(Reconciliation): 1분 주기로 실제 거래소 주문과 수첩을 대조하여 '좀비 주문' 및 '뒤틀린 그리드' 강제 교정
3. 150원 지터 필터(Hysteresis): 가격이 150원(1.5틱) 이상 유의미하게 변할 때만 주문을 재배치하여 API 난사 방지
4. 원자적 리셋(Atomic Reset): 취소와 생성 사이에 0.5초의 쿨타임을 두어 분산 시스템 간의 상태 정합성 보장
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
VP_WINDOW_SEC      = 1800           # 분석 범위: 30분
MIN_TOTAL_VOL_KRW  = 1000000000     # 데이터 신뢰 임계값: 10억 원

# --- [모드 전환 임계값] ---
CAUTION_MDD      = Decimal("-0.05")
CAUTION_VP       = Decimal("40.0")
DEFENSIVE_MDD    = Decimal("-0.10")
DEFENSIVE_VP     = Decimal("25.0")
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

class EnterpriseShieldBotV3_2_2:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {}
        self.current_price = Decimal("0")
        
        self.current_mode = "NORMAL"
        self.last_mode = "NORMAL"
        self.rolling_24h_high = Decimal("0")
        self.last_high_update = 0
        self.last_reconcile_time = 0 # 정합성 체크 주기 관리
        self.trade_history = deque() 
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
                        'side': info['side'],
                        'timestamp': info.get('timestamp', time.time()) # 생성 시각 추적
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
                    'side': info['side'],
                    'timestamp': info.get('timestamp', time.time())
                }
            tmp_file = STATE_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(serializable_map, f, indent=4)
            os.replace(tmp_file, STATE_FILE)
        except Exception:
            logj("err_save", trace=traceback.format_exc())

    def reconcile_orders(self):
        """[핵심] 1분마다 거래소와 수첩의 싱크를 맞춤 (Conflict Resolution)"""
        if time.time() - self.last_reconcile_time < 60: return
        try:
            actual_orders = self.upbit.get_order(SYMBOL, state='wait') or []
            actual_uuids = [o['uuid'] for o in actual_orders]
            now = time.time()

            with self.lock:
                # 1. 수첩(Memory) -> 거래소(API) 대조: 체결/취소된 좀비 제거
                to_delete = []
                for uid, info in self.grid_map.items():
                    # 방금 주문한 건(15초 이내)은 전송 지연 가능성이 크므로 대조 제외
                    if uid not in actual_uuids and (now - info.get('timestamp', 0) > 15):
                        if info['side'] == 'bid': # 매수는 리스트에 없으면 확실히 삭제
                            to_delete.append(uid)
                
                for uid in to_delete:
                    del self.grid_map[uid]
                    logj("sync_removed", uid=uid, msg="Local order pruned after API check.")

                # 2. 거래소(API) -> 수첩(Memory) 대조: 누락된 주문 복구
                for o in actual_orders:
                    uid = o['uuid']
                    if uid not in self.grid_map:
                        p = Decimal(str(o['price']))
                        side = 'bid' if o['side'] == 'bid' else 'ask'
                        self.grid_map[uid] = {
                            'buy_price': p,
                            'sell_price': adjust_price(p * (Decimal("1") + PROFIT_PCT)),
                            'side': side,
                            'timestamp': now
                        }
                        logj("sync_added", uid=uid, price=str(p), side=side)
            
            self.last_reconcile_time = now
            self.save_state()
        except:
            logj("err_reconcile", trace=traceback.format_exc())

    def update_24h_high(self):
        if time.time() - self.last_high_update < 300: return
        try:
            df = pyupbit.get_ohlcv(SYMBOL, interval="minute60", count=24)
            if df is not None:
                self.rolling_24h_high = Decimal(str(df['high'].max()))
                self.last_high_update = time.time()
        except: pass

    def get_volume_power(self):
        now = time.time()
        while self.trade_history and now - self.trade_history[0][0] > VP_WINDOW_SEC:
            self.trade_history.popleft()
        buy_vol = sum(vol for ts, vol, side in self.trade_history if side == 'BID')
        sell_vol = sum(vol for ts, vol, side in self.trade_history if side == 'ASK')
        total_vol_krw = (Decimal(str(buy_vol)) + Decimal(str(sell_vol))) * self.current_price
        if total_vol_krw < MIN_TOTAL_VOL_KRW: return Decimal("100.0")
        if sell_vol == 0: return Decimal("100.0")
        return (Decimal(str(buy_vol)) / Decimal(str(sell_vol))) * 100

    def decide_mode(self):
        if self.rolling_24h_high == 0: return "NORMAL"
        mdd = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high
        v_power = self.get_volume_power()
        if mdd <= FREEZE_MDD or v_power < FREEZE_VP: return "FREEZE"
        if mdd <= DEFENSIVE_MDD or v_power < DEFENSIVE_VP: return "DEFENSIVE"
        if mdd <= CAUTION_MDD or v_power < CAUTION_VP: return "CAUTION"
        return "NORMAL"

    def reset_buy_grid(self):
        """모드 전환 시 원자적 리셋: 전체 취소 후 수첩 초기화"""
        with self.lock:
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    self.upbit.cancel_order(o['uuid'])
                    if o['uuid'] in self.grid_map: del self.grid_map[o['uuid']]
            time.sleep(0.5) # API 동기화 쿨타임
            self.save_state()
            logj("mode_reset_execution", mode=self.current_mode)

    def maintain_grid(self):
        try:
            mode_m = {"NORMAL": 1.0, "CAUTION": 2.0, "DEFENSIVE": 3.3, "FREEZE": 999.0}
            current_gap = BASE_GRID_GAP * Decimal(str(mode_m.get(self.current_mode, 1.0)))
            
            with self.lock:
                if self.current_mode == "FREEZE": return 
                
                # 1. 현재 수첩상의 매수 주문 분석
                buy_infos = sorted([i for i in self.grid_map.values() if i['side'] == 'bid'], 
                                   key=lambda x: x['buy_price'], reverse=True)
                
                # 2. 150원 유격(Hysteresis) 기반 Shift-Up 판단
                if buy_infos:
                    highest_buy = buy_infos[0]['buy_price']
                    target_1st = adjust_price(self.current_price * (Decimal("1") - current_gap))
                    
                    # 목표가와 실제주문가의 오차가 150원을 넘을 때만 재배치 (API 난사 방지)
                    if abs(highest_buy - target_1st) > 150:
                        logj("grid_realign", current=str(highest_buy), target=str(target_1st), diff=str(highest_buy - target_1st))
                        self.reset_buy_grid()
                        return

                # 3. 설계도 기반 주문 생성 (Price-Level Lock)
                existing_prices = [i['buy_price'] for i in self.grid_map.values() if i['side'] == 'bid']
                
                for i in range(1, MAX_LAYERS + 1):
                    target_p = adjust_price(self.current_price * (Decimal("1") - current_gap * i))
                    
                    # 중복 가격 체크: 수첩에 100원 이내로 붙은 주문이 있으면 절대 추가 안 함
                    if any(abs(p - target_p) <= 100 for p in existing_prices):
                        continue

                    # 최대 레이어 제한 확인
                    if len([i for i in self.grid_map.values() if i['side'] == 'bid']) < MAX_LAYERS:
                        balance = Decimal(str(self.upbit.get_balance("KRW")))
                        if balance < (BUY_AMOUNT_KRW * Decimal("1.0005")): break
                        
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            # 주문 즉시 수첩에 기록 (Optimistic Update)
                            self.grid_map[res['uuid']] = {
                                'buy_price': target_p, 
                                'sell_price': adjust_price(target_p * (Decimal("1") + PROFIT_PCT)),
                                'side': 'bid',
                                'timestamp': time.time()
                            }
                            logj("place_buy", price=str(target_p), mode=self.current_mode)
                            self.save_state()
                        time.sleep(0.3) # API 연속 호출 방지
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
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 
                                    'sell_price': info['sell_price'], 
                                    'side': 'ask',
                                    'timestamp': time.time()
                                }
                                logj("place_sell", price=str(info['sell_price']))
                                changed = True
                        elif info['side'] == 'ask':
                            del self.grid_map[uid]
                            buy_cost = info['buy_price'] * vol
                            sell_rev = price * vol
                            net_profit = (sell_rev * Decimal("0.9995")) - (buy_cost * Decimal("1.0005"))
                            self.cumulative_net_profit += net_profit
                            logj("trade_success", profit=str(round(net_profit, 2)), total=str(round(self.cumulative_net_profit, 2)))
                            if self.current_mode != "NORMAL": self.current_mode = "NORMAL"
                            changed = True
                if changed: self.save_state()
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        logj("bot_start", v="3.2.2 AtomicSync")
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
                    self.reconcile_orders() # [추가] 주기적 수첩-현장 대조
                    self.current_mode = self.decide_mode()
                    
                    if self.current_mode != self.last_mode:
                        self.reset_buy_grid()
                        self.last_mode = self.current_mode
                    
                    self.maintain_grid()
                    self.check_fill()
                    
                    mdd_val = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high if self.rolling_24h_high > 0 else 0
                    logj("status", mode=self.current_mode, price=format(int(self.current_price), ','), 
                         mdd=f"{round(mdd_val*100, 2)}%", v_power=f"{round(self.get_volume_power(), 1)}%")
                    last_loop_time = time.time()
            except Exception:
                logj("err_loop", trace=traceback.format_exc())
                time.sleep(2)

if __name__ == "__main__":
    EnterpriseShieldBotV3_2_2().run()