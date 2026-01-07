#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[전략 요약: KRW-SOL 지능형 방어 그리드 봇 v3.0]

1. 4단계 지능형 태세 전환 (State Machine)
   - Normal: 평시 수익 모드 (0.3% 간격 / 20만 원)
   - Caution: 흐름 불안 (0.6% 간격 / 20만 원) - 수익정체 1h OR 낙폭 -3% OR 강도 < 60%
   - Defensive: 하락장 대응 (1.0% 간격 / 20만 원) - 수익정체 2h OR 낙폭 -5% OR 강도 < 40%
   - Freeze: 투매 발생 (매수 중단) - 낙폭 -10% OR 강도 < 20%

2. 실시간 데이터 분석 (Decision Intelligence)
   - Rolling 24h High: 최근 24시간 내 최고가 대비 낙폭(MDD) 실시간 추적
   - Volume Power (1h): 최근 1시간 매수/매도 체결량 비율 실시간 집계
   - TP Stagnation: 마지막 익절 발생 후 경과 시간 추적

3. 리그리딩 시스템 (Clean & Rebuild)
   - 모드 상향/하향 시 기존 매수 주문(Bid) 전량 취소 후 새로운 간격으로 재배치
   - 매도 주문(Ask)은 수익 확보를 위해 어떤 경우에도 유지 (탈출구 전략)
   - 매도 체결 시 즉시 Normal 모드 복구 (회복 탄력성)
"""

import os
import time
import json
import threading
import traceback
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
from collections import deque
import pyupbit
from pyupbit import WebSocketManager

getcontext().prec = 28

# ==========================================
# [사용자 설정 영역]
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL           = "KRW-SOL"
BUY_AMOUNT_KRW   = Decimal("200000")
BASE_GRID_GAP    = Decimal("0.003") # 0.3%
PROFIT_PCT       = Decimal("0.005") # 0.5%
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

class EnterpriseShieldBotV3:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {}
        self.current_price = Decimal("0")
        
        # [모드 관리 변수]
        self.current_mode = "NORMAL"
        self.last_mode = "NORMAL"
        self.last_tp_time = time.time()
        self.rolling_24h_high = Decimal("0")
        self.last_high_update = 0
        
        # [체결 강도 계산용 (1시간 윈도우)]
        self.trade_history = deque() # (timestamp, volume, side)
        
        # [수익 추적]
        self.start_total_asset = Decimal("0")
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

    def update_24h_high(self):
        """실시간 롤링 윈도우 24시간 최고가 업데이트 (5분 주기)"""
        if time.time() - self.last_high_update < 300: return
        try:
            df = pyupbit.get_ohlcv(SYMBOL, interval="minute60", count=24)
            if df is not None:
                self.rolling_24h_high = Decimal(str(df['high'].max()))
                self.last_high_update = time.time()
        except:
            pass

    def get_volume_power(self):
        """최근 1시간 체결 강도 계산"""
        now = time.time()
        # 1시간 지난 데이터 삭제
        while self.trade_history and now - self.trade_history[0][0] > 3600:
            self.trade_history.popleft()
        
        buy_vol = sum(vol for ts, vol, side in self.trade_history if side == 'BID')
        sell_vol = sum(vol for ts, vol, side in self.trade_history if side == 'ASK')
        
        if sell_vol == 0: return Decimal("100")
        return (Decimal(str(buy_vol)) / Decimal(str(sell_vol))) * 100

    def decide_mode(self):
        """현재 시장 데이터 기반 모드 결정 엔진"""
        if self.rolling_24h_high == 0: return "NORMAL"
        
        mdd = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high
        stagnation_min = (time.time() - self.last_tp_time) / 60
        v_power = self.get_volume_power()
        
        # 1. FREEZE (정지)
        if mdd <= Decimal("-0.10") or v_power < 20:
            return "FREEZE"
        # 2. DEFENSIVE (방어)
        if mdd <= Decimal("-0.05") or stagnation_min >= 120 or v_power < 40:
            return "DEFENSIVE"
        # 3. CAUTION (주의)
        if mdd <= Decimal("-0.03") or stagnation_min >= 60 or v_power < 60:
            return "CAUTION"
        
        return "NORMAL"

    def reset_buy_grid(self):
        """모드 변경 시 매수 주문 리셋 (Clean & Build)"""
        with self.lock:
            # 1. 업비트에서 모든 매수 주문 취소
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    self.upbit.cancel_order(o['uuid'])
                    time.sleep(0.1)
            
            # 2. grid_map에서 'bid' 상태인 주문 정보 제거 (매도는 유지)
            new_map = {uid: info for uid, info in self.grid_map.items() if info['side'] == 'ask'}
            self.grid_map = new_map
            self.save_state()
            logj("mode_reset_execution", mode=self.current_mode, msg="Buy grid cleared for re-gridding.")

    def maintain_grid(self):
        try:
            # 모드 배수 설정
            mode_multipliers = {"NORMAL": 1.0, "CAUTION": 2.0, "DEFENSIVE": 3.3, "FREEZE": 999.0}
            current_gap = BASE_GRID_GAP * Decimal(str(mode_multipliers.get(self.current_mode, 1.0)))
            
            with self.lock:
                if self.current_mode == "FREEZE": return # 정지 모드 시 매수 금지
                if len(self.grid_map) >= MAX_INVENTORY: return
                
                balance = Decimal(str(self.upbit.get_balance("KRW")))
                orders = self.upbit.get_order(SYMBOL, state='wait') or []
                buy_orders = sorted([o for o in orders if o['side'] == 'bid'], 
                                    key=lambda x: Decimal(str(x['price'])), reverse=True)

                # Shift Up 로직 (Normal 모드에서 주로 작동)
                if buy_orders and self.current_mode == "NORMAL":
                    highest_buy = Decimal(str(buy_orders[0]['price']))
                    if (self.current_price - highest_buy) / self.current_price > (current_gap + Decimal("0.001")):
                        lowest_order = buy_orders[-1]
                        self.upbit.cancel_order(lowest_order['uuid'])
                        time.sleep(0.1)
                        if lowest_order['uuid'] in self.grid_map: del self.grid_map[lowest_order['uuid']]
                        logj("shift_up", cancelled=str(lowest_order['price']))
                        buy_orders.pop()

                existing_prices = [info['buy_price'] for info in self.grid_map.values()]
                
                for i in range(1, MAX_LAYERS + 1):
                    if balance < (BUY_AMOUNT_KRW * Decimal("1.0005")): break
                    target_p = adjust_price(self.current_price * (Decimal("1") - current_gap * i))
                    
                    # 중복 주문 방지 (0.5 * 현재 간격의 여유를 둠)
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
                            
                            # 수익 계산 및 익절 시간 갱신
                            buy_cost = info['buy_price'] * vol
                            sell_rev = price * vol
                            net_profit = (sell_rev * Decimal("0.9995")) - (buy_cost * Decimal("1.0005"))
                            self.cumulative_net_profit += net_profit
                            self.last_tp_time = time.time() # 익절 시 타이머 초기화
                            
                            logj("trade_success", 
                                 buy=format(int(info['buy_price']), ','), 
                                 sell=format(int(price), ','),
                                 profit=str(round(net_profit, 2)), 
                                 session_total=str(round(self.cumulative_net_profit, 2)))
                            
                            # 익절 발생 시 무조건 NORMAL 모드 복구
                            if self.current_mode != "NORMAL":
                                logj("recovery_to_normal", msg="Profit hit. Resetting mode to NORMAL.")
                                self.current_mode = "NORMAL"
                            
                            changed = True
                if changed: self.save_state()
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        # 자산 초기화
        balances = self.upbit.get_balances()
        krw_val = next((Decimal(b['balance']) for b in balances if b['currency'] == 'KRW'), Decimal("0"))
        self.start_total_asset = self.get_total_asset()
        logj("bot_start", v="3.0 Shield", base_asset=format(int(self.start_total_asset), ','))

        wm = WebSocketManager("ticker", [SYMBOL])
        last_loop_time = 0
        
        while True:
            try:
                data = wm.get()
                if not data: continue
                
                self.current_price = Decimal(str(data['trade_price']))
                
                # 체결 강도 데이터 수집 (BID: 매수체결(빨간색), ASK: 매도체결(파란색))
                # 업비트 웹소켓 ticker 데이터의 ask_bid는 'ASK'가 매도체결, 'BID'가 매수체결임
                self.trade_history.append((time.time(), Decimal(str(data['trade_volume'])), data['ask_bid']))
                
                if time.time() - last_loop_time > 4:
                    self.update_24h_high()
                    self.current_mode = self.decide_mode()
                    
                    # 모드 변경 감지 시 리셋 로직
                    if self.current_mode != self.last_mode:
                        logj("mode_changed", old=self.last_mode, new=self.current_mode)
                        self.reset_buy_grid()
                        self.last_mode = self.current_mode
                    
                    self.maintain_grid()
                    self.check_fill()
                    
                    # 모니터링 로그
                    mdd = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high if self.rolling_24h_high > 0 else 0
                    logj("status", mode=self.current_mode, price=format(int(self.current_price), ','), 
                         mdd=f"{round(mdd*100, 2)}%", v_power=f"{round(self.get_volume_power(), 1)}%",
                         stagnation=f"{round((time.time()-self.last_tp_time)/60, 1)}m")
                    
                    last_loop_time = time.time()
            except Exception:
                logj("err_loop", trace=traceback.format_exc())
                time.sleep(2)

if __name__ == "__main__":
    EnterpriseShieldBotV3().run()