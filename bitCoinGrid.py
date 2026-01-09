#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[KRW-SOL Active Meat v3.2.5 - Enterprise Shield]

1. Duplicate Slot Protection: 매수/매도 구분 없이 점유된 가격대(Slot)의 중복 매수를 원천 차단
2. Hybrid Tracking: 상승 시 최하단 주문을 재활용(Tail-Recycle)하여 그리드 밀착 추적
3. Atomic Partial Fill Defense: 주문 취소/이동 시 체결 수량을 즉시 확인하여 누락 없는 매도 전환
4. Orphan Coin Recovery: 1분마다 실제 잔고 대조를 통해 수첩 밖의 고아 코인을 자동 익절 매도
5. Atomic Sync (RLock): 재진입 가능 락을 통한 멀티 쓰레드 환경 및 중첩 로직 안정성 보장
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

# 고정밀 연산을 위한 컨텍스트 설정
getcontext().prec = 28

# ==========================================
# [사용자 설정 영역] - Config
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL           = "KRW-SOL"
BUY_AMOUNT_KRW   = Decimal("300000") # 슬롯당 30만 원
BASE_GRID_GAP    = Decimal("0.003")  # 0.3%
PROFIT_PCT       = Decimal("0.005")  # 0.5%

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

class EnterpriseShieldBotV3_2_5:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.RLock()
        self.grid_map = {}
        self.current_price = Decimal("0")
        
        self.current_mode = "NORMAL"
        self.last_mode = "NORMAL"
        self.rolling_24h_high = Decimal("0")
        self.last_high_update = 0
        self.last_reconcile_time = 0 
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
                        'volume': Decimal(info.get('volume', '0')),
                        'timestamp': info.get('timestamp', time.time())
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
                    'volume': str(info.get('volume', '0')),
                    'timestamp': info.get('timestamp', time.time())
                }
            tmp_file = STATE_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(serializable_map, f, indent=4)
            os.replace(tmp_file, STATE_FILE)
        except Exception:
            logj("err_save", trace=traceback.format_exc())

    def reconcile_orders(self):
        """[v3.2.5] 정기 정합성 체크: 고아 코인 구제 + 중복 가격 주문 정리"""
        if time.time() - self.last_reconcile_time < 60: return
        try:
            actual_orders = self.upbit.get_order(SYMBOL, state='wait') or []
            actual_uuids = [o['uuid'] for o in actual_orders]
            now = time.time()

            with self.lock:
                # 1. 수첩-API 대조 및 동기화
                to_delete = []
                for uid, info in self.grid_map.items():
                    if uid not in actual_uuids and (now - info.get('timestamp', 0) > 15):
                        if info['side'] == 'bid': to_delete.append(uid)
                for uid in to_delete: del self.grid_map[uid]
                
                for o in actual_orders:
                    if o['uuid'] not in self.grid_map:
                        p = Decimal(str(o['price']))
                        v = Decimal(str(o['remaining_volume']))
                        self.grid_map[o['uuid']] = {
                            'buy_price': p,
                            'sell_price': adjust_price(p * (Decimal("1") + PROFIT_PCT)),
                            'side': 'bid' if o['side'] == 'bid' else 'ask',
                            'volume': v,
                            'timestamp': now
                        }

                # 2. 중복 가격 주문 정리 가드
                price_tracker = {}
                uids_to_cancel = []
                for uid, info in list(self.grid_map.items()):
                    p = info['buy_price']
                    if p in price_tracker:
                        uids_to_cancel.append(uid)
                    else:
                        price_tracker[p] = uid
                
                for uid in uids_to_cancel:
                    if uid in actual_uuids:
                        self.upbit.cancel_order(uid)
                        logj("cleanup_duplicate_price", price=str(self.grid_map[uid]['buy_price']))
                    if uid in self.grid_map: del self.grid_map[uid]

                # 3. 고아 코인 구제 (잔고 대조)
                actual_balance = Decimal(str(self.upbit.get_balance(SYMBOL)))
                tracked_sell_vol = sum(info['volume'] for info in self.grid_map.values() if info['side'] == 'ask')
                orphan_vol = actual_balance - tracked_sell_vol
                if orphan_vol > Decimal("0.05"):
                    logj("recovery_orphan_found", vol=str(orphan_vol))
                    target_sell_p = adjust_price(self.current_price * (Decimal("1") + PROFIT_PCT))
                    res = self.upbit.sell_limit_order(SYMBOL, float(target_sell_p), float(orphan_vol))
                    if res:
                        self.grid_map[res['uuid']] = {
                            'buy_price': self.current_price,
                            'sell_price': target_sell_p,
                            'side': 'ask', 'volume': orphan_vol, 'timestamp': time.time()
                        }

                # 4. 정기 Full Re-align
                buy_infos = sorted([i for i in self.grid_map.values() if i['side'] == 'bid'], 
                                   key=lambda x: x['buy_price'], reverse=True)
                if buy_infos:
                    mode_m = {"NORMAL": 1.0, "CAUTION": 2.0, "DEFENSIVE": 3.3, "FREEZE": 999.0}
                    curr_gap = BASE_GRID_GAP * Decimal(str(mode_m.get(self.current_mode, 1.0)))
                    target_1st = adjust_price(self.current_price * (Decimal("1") - curr_gap))
                    if abs(buy_infos[0]['buy_price'] - target_1st) > 150:
                        logj("periodic_realign")
                        self.reset_buy_grid()
            
            self.last_reconcile_time = now
            self.save_state()
        except: pass

    def reset_buy_grid(self):
        """[Partial Fill Defense] 리셋 시 부분 체결 확인 및 즉시 매도 전환"""
        with self.lock:
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    uuid = o['uuid']
                    self.upbit.cancel_order(uuid)
                    time.sleep(0.2)
                    detail = self.upbit.get_order(uuid)
                    exec_vol = Decimal(str(detail.get('executed_volume', '0')))
                    if exec_vol > 0 and uuid in self.grid_map:
                        info = self.grid_map[uuid]
                        s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(exec_vol))
                        if s_res:
                            self.grid_map[s_res['uuid']] = {
                                'buy_price': info['buy_price'], 'sell_price': info['sell_price'],
                                'side': 'ask', 'volume': exec_vol, 'timestamp': time.time()
                            }
                            logj("partial_on_reset", vol=str(exec_vol))
                    if uuid in self.grid_map: del self.grid_map[uuid]
            time.sleep(0.3) 
            self.save_state()
            logj("mode_reset_execution", mode=self.current_mode)

    def maintain_grid(self):
        """[v3.2.5] 상시 추격: 꼬리 재활용 및 중복 가격대 방어"""
        try:
            mode_m = {"NORMAL": 1.0, "CAUTION": 2.0, "DEFENSIVE": 3.3, "FREEZE": 999.0}
            current_gap = BASE_GRID_GAP * Decimal(str(mode_m.get(self.current_mode, 1.0)))
            
            with self.lock:
                if self.current_mode == "FREEZE": return 
                
                # 1. Shift-Up (상승 시 하단 주문 재활용)
                buy_infos = sorted([i for i in self.grid_map.items() if i[1]['side'] == 'bid'], 
                                   key=lambda x: x[1]['buy_price'])
                if len(buy_infos) >= MAX_LAYERS:
                    highest_buy = buy_infos[-1][1]['buy_price']
                    if (self.current_price - highest_buy) / self.current_price > (current_gap + Decimal("0.002")):
                        lowest_uid = buy_infos[0][0]
                        detail = self.upbit.get_order(lowest_uid)
                        exec_vol = Decimal(str(detail.get('executed_volume', '0')))
                        self.upbit.cancel_order(lowest_uid)
                        time.sleep(0.2)
                        if exec_vol > 0 and lowest_uid in self.grid_map:
                            info = self.grid_map[lowest_uid]
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(exec_vol))
                            if s_res:
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 'sell_price': info['sell_price'],
                                    'side': 'ask', 'volume': exec_vol, 'timestamp': time.time()
                                }
                                logj("partial_on_shift", vol=str(exec_vol))
                        if lowest_uid in self.grid_map: del self.grid_map[lowest_uid]
                        logj("shift_up_single")

                # 2. [v3.2.5] 중복 방지 매수 신규 주문
                # 이미 체결되어 매도 대기(ask) 중인 가격까지 점유 상태로 간주
                occupied_prices = [i['buy_price'] for i in self.grid_map.values()]
                current_bid_count = len([i for i in self.grid_map.values() if i['side'] == 'bid'])

                for i in range(1, MAX_LAYERS + 1):
                    target_p = adjust_price(self.current_price * (Decimal("1") - current_gap * i))
                    
                    # 수첩에 어떤 상태로든 이 가격이 있으면 건너뜀
                    if any(abs(p - target_p) <= 150 for p in occupied_prices): continue

                    # 매수 대기 슬롯 가용성 확인
                    if current_bid_count < MAX_LAYERS:
                        balance = Decimal(str(self.upbit.get_balance("KRW")))
                        if balance < (BUY_AMOUNT_KRW * Decimal("1.0005")): break
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            self.grid_map[res['uuid']] = {
                                'buy_price': target_p, 
                                'sell_price': adjust_price(target_p * (Decimal("1") + PROFIT_PCT)),
                                'side': 'bid', 'volume': vol, 'timestamp': time.time()
                            }
                            logj("place_buy", price=str(target_p))
                            current_bid_count += 1
                            self.save_state()
                        time.sleep(0.3)
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
                        if info['side'] == 'bid':
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(vol))
                            if s_res:
                                del self.grid_map[uid]
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 'sell_price': info['sell_price'], 
                                    'side': 'ask', 'volume': vol, 'timestamp': time.time()
                                }
                                logj("place_sell", price=str(info['sell_price']))
                                changed = True
                        elif info['side'] == 'ask':
                            del self.grid_map[uid]
                            net = (Decimal(str(o['price'])) * vol * Decimal("0.9995")) - (info['buy_price'] * vol * Decimal("1.0005"))
                            self.cumulative_net_profit += net
                            logj("trade_success", profit=str(round(net, 2)), total=str(round(self.cumulative_net_profit, 2)))
                            if self.current_mode != "NORMAL": self.current_mode = "NORMAL"
                            changed = True
                if changed: self.save_state()
        except Exception:
            logj("err_check", trace=traceback.format_exc())

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
        vp = self.get_volume_power()
        if mdd <= FREEZE_MDD or vp < FREEZE_VP: return "FREEZE"
        if mdd <= DEFENSIVE_MDD or vp < DEFENSIVE_VP: return "DEFENSIVE"
        if mdd <= CAUTION_MDD or vp < CAUTION_VP: return "CAUTION"
        return "NORMAL"

    def run(self):
        logj("bot_start", v="3.2.5 EnterpriseShield")
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
                    self.reconcile_orders()
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
    EnterpriseShieldBotV3_2_5().run()