#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-SOL Scalping Bot (Final Version: 2025-12-14)
Target: Solana (SOL) / Threshold: 1B KRW / Rebound: 33% Retracement

================================================================================
[전략 상세 명세서 (Strategy Specification)]
================================================================================

1. 전략 A: 급등주 돌파 매매 (Bullish Breakout)
   - 목적: 상승 추세에서 힘이 실리는 구간에 올라타서 짧게 먹기
   - 감시 조건 (3분봉 마감 시 체크):
     A) 캔들이 [양봉] 일 것
     B) 거래대금이 [10억 원 이상] 터질 것 (SOL 기준 최적화)
     C) 필터 1: RSI(9)가 [75 미만] (너무 고점이면 패스)
     D) 필터 2: 종가가 [20일 이동평균선] 위에 있을 것 (정배열/상승추세)
   - 진입 타점:
     -> 다음 봉에서 현재가가 [기준봉의 종가]를 상승 돌파하고 [1초 이상 유지] 시 시장가 매수

2. 전략 B: 낙폭과대 반등 매매 (Panic Sell Rebound) *Modified*
   - 목적: 공포에 질린 투매(Panic Sell) 물량을 세력이 받아먹은 직후 V자 반등 노리기
   - 감시 조건 (3분봉 마감 시 체크):
     A) 최근 3개 봉이 [3연속 음봉] 일 것 (하락 추세)
     B) 하락 구간의 RSI(9)가 [25 미만] 일 것 (과매도 상태)
     C) 마지막 3번째 음봉이 [거래대금 10억 원 이상] 터질 것 (투매 클라이막스)
   - 진입 타점 (수정됨):
     -> 기준봉 바로 [다음 봉(4번째)]에서,
     -> 현재가가 [기준봉 몸통의 33%(1/3) 지점]을 회복(돌파)하면 즉시 시장가 매수
   - 유효 기간 (Reset):
     -> 4번째 봉에서 매수가 안 되면 즉시 전략 폐기 (시간 끌면 반등 실패로 간주)

3. 공통 청산 규칙 (Exit Rule)
   - 익절 (Take Profit): 진입가 대비 [+0.7%] 도달 시 [지정가] 매도 주문
   - 손절 (Stop Loss): 진입가 대비 [-0.7%] 이탈 시 [시장가] 매도 (슬리피지 방어)
================================================================================
"""

import os
import time
import json
import threading
import uuid
import math
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import pyupbit
from pyupbit import WebSocketManager
import pandas as pd

# ==========================================
# [사용자 설정]
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

# [변경 1] 종목: 솔라나
SYMBOL             = "KRW-SOL" 
BUY_KRW_AMOUNT     = Decimal("400000")     # 1회 매수 금액

# [변경 2] 거래대금 기준: 10억 원 (1,000,000,000)
TURNOVER_THRESH    = Decimal("1000000000") 

# 지표 설정
RSI_PERIOD         = 9
RSI_LIMIT          = 75.0 # [전략 A] 돌파 전략 RSI 상한선
RSI_REBOUND_LIMIT  = 25.0 # [전략 B] 반등 전략 RSI 하한선
MA_PERIOD          = 20

# 진입 및 청산 설정
BREAKOUT_HOLD_SEC  = 1.0              # [전략 A] 돌파 시 1초 버티기
TP_PCT             = Decimal("0.007") # 익절 +0.7%
SL_PCT             = Decimal("0.007") # 손절 -0.7%

# API 제한 관리
REQS_PER_SEC       = 8
REQS_PER_MIN       = 200
# ==========================================

RUN_ID = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:6]}"

def now_kst_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

def logj(event: str, **fields):
    rec = {"ts": now_kst_str(), "run": RUN_ID, "ev": event}
    rec.update(fields)
    print(json.dumps(rec, ensure_ascii=False), flush=True)

# 호가 단위 계산 (솔라나 가격대에 맞춰 자동 조정됨)
def adjust_price_to_tick(price):
    if price >= 2000000: tick = 1000
    elif price >= 1000000: tick = 500
    elif price >= 500000:  tick = 100
    elif price >= 100000:  tick = 50
    elif price >= 10000:   tick = 10
    elif price >= 1000:    tick = 1
    elif price >= 100:     tick = 0.1
    elif price >= 10:      tick = 0.01
    else: tick = 0.0001
    return math.floor(price / tick) * tick

# API 과부하 방지 (TokenBucket)
class TokenBucket:
    def __init__(self, per_sec, per_min):
        self.per_sec = per_sec; self.per_min = per_min
        self.lock = threading.Lock()
        self.sec_tokens = per_sec; self.min_tokens = per_min
        self.last_sec = int(time.time()); self.last_min = int(time.time()//60)

    def _refill(self):
        t = time.time(); s = int(t); m = int(t//60)
        if s != self.last_sec: self.sec_tokens = self.per_sec; self.last_sec = s
        if m != self.last_min: self.min_tokens = self.per_min; self.last_min = m

    def acquire(self):
        while True:
            with self.lock:
                self._refill()
                if self.sec_tokens > 0 and self.min_tokens > 0:
                    self.sec_tokens -= 1; self.min_tokens -= 1
                    return
            time.sleep(0.02)

class UpbitWrap:
    def __init__(self):
        if not UPBIT_ACCESS_KEY: print("Warning: API Key Missing")
        self.u = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.tb = TokenBucket(REQS_PER_SEC, REQS_PER_MIN)

    def _safe(self, fn, *args, **kwargs):
        for _ in range(3): # 재시도 3회
            self.tb.acquire()
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logj("api_retry", err=str(e))
                time.sleep(0.2)
        return None

    def get_ohlcv(self):
        return self._safe(pyupbit.get_ohlcv, SYMBOL, interval="minute3", count=200)

    def get_balance_krw(self):
        r = self._safe(self.u.get_balance, "KRW")
        return Decimal(str(r)) if r is not None else Decimal("0")

    def get_coin_free(self):
        bals = self._safe(self.u.get_balances) or []
        coin = SYMBOL.split("-")[1]
        for b in bals:
            if b.get('currency') == coin: return Decimal(str(b.get('balance')))
        return Decimal("0")

    def buy_market(self, krw):
        return self._safe(self.u.buy_market_order, SYMBOL, float(krw))

    def sell_market(self, vol):
        return self._safe(self.u.sell_market_order, SYMBOL, float(vol))

    def sell_limit(self, price, vol):
        return self._safe(self.u.sell_limit_order, SYMBOL, float(price), float(vol))

    def cancel_order(self, uuid):
        return self._safe(self.u.cancel_order, uuid)

def calc_indicators(df):
    if df is None or len(df) < 30: return None, None, None, None
    close = df['close']
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=RSI_PERIOD, min_periods=1).mean()
    avg_loss = loss.rolling(window=RSI_PERIOD, min_periods=1).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    ma20 = close.rolling(window=MA_PERIOD).mean()
    
    return rsi, ma20.iloc[-1], df['close'].iloc[-2], df['volume'].iloc[-2]

class BreakoutBot:
    def __init__(self):
        self.api = UpbitWrap()
        self.lock = threading.RLock()
        
        # [공통 상태 변수]
        self.cur_candle = None
        self.in_pos = False
        self.entry_price = None
        self.sl_price = None
        self.tp_uuid = None
        self.tp_check_ts = 0
        
        # [전략 A: 돌파 매매] 상태 변수
        self.watch_target_b = None 
        self.hold_start_b = None
        
        # [전략 B: 반등 매매] 상태 변수
        self.watch_target_r = None 
        self.is_rebound_ready = False 

    def _finalize_candle(self, c):
        threading.Thread(target=self._analyze_and_manage_strategy, args=(c,), daemon=True).start()

    # [전략 분석 핵심 로직]
    def _analyze_and_manage_strategy(self, c):
        try:
            df = self.api.get_ohlcv()
            if df is None: return

            rsi_series, ma20, prev_close, prev_volume = calc_indicators(df)
            current_rsi = rsi_series.iloc[-1]
            prev_rsi = rsi_series.iloc[-2] if len(rsi_series) > 1 else 50.0
            
            # ----------------------------------------------------
            # 1. [전략 A] 돌파 매매 (Breakout)
            # ----------------------------------------------------
            # 조건: 거래대금 10억 이상 + 양봉
            if c['vol'] >= TURNOVER_THRESH and c['o'] < c['c']: 
                is_valid_breakout = (current_rsi < RSI_LIMIT) and (c['c'] >= ma20)
                
                with self.lock:
                    if is_valid_breakout:
                        self.watch_target_b = c['c']
                        self.hold_start_b = None
                        logj("watch_on_Breakout", target=c['c'], rsi=f"{current_rsi:.1f}")
                    else:
                        self.watch_target_b = None
                        logj("watch_skip_Breakout", reason="Filter Fail", rsi=f"{current_rsi:.1f}")

            # ----------------------------------------------------
            # 2. [전략 B] 낙폭 반등 (Rebound)
            # ----------------------------------------------------
            
            # (A) 유효기간 만료 체크 (1개 봉 지날 때까지 진입 못하면 폐기)
            if self.is_rebound_ready:
                with self.lock:
                    self.watch_target_r = None
                    self.is_rebound_ready = False
                    logj("rebound_expired", reason="Time Limit Exceeded")
            
            # (B) 새로운 기준봉 탐색
            if not self.is_rebound_ready and len(df) >= 4:
                prev_c_df = df.iloc[-4:-1] 
                
                # 조건 1: 3연속 음봉
                is_three_crows = all(prev_c_df['close'].values < prev_c_df['open'].values)

                # 조건 2: 과매도 (직전 봉 마감 RSI < 25)
                is_oversold = prev_rsi < RSI_REBOUND_LIMIT
                
                # 조건 3: 이번 봉(c)이 10억 이상 터진 음봉
                is_standard_rebound_candle = (c['vol'] >= TURNOVER_THRESH) and (c['o'] > c['c'])

                with self.lock:
                    if is_three_crows and is_oversold and is_standard_rebound_candle:
                        self.is_rebound_ready = True
                        
                        # [변경 3] 타점 수정: 몸통의 33%(1/3) 회복 지점
                        # 타겟 = 종가 + (몸통길이 * 0.33)
                        body_len = c['o'] - c['c']
                        target_raw = c['c'] + (body_len * Decimal("0.33"))
                        
                        # 호가 단위(Tick)에 맞춰 가격 보정
                        self.watch_target_r = adjust_price_to_tick(float(target_raw))
                        
                        logj("rebound_watch_on", close=str(c['c']), target=str(self.watch_target_r), rsi=f"{prev_rsi:.1f}")
            
        except Exception as e:
            logj("err_analyze", msg=str(e))

    def _enter(self, price, strategy_name):
        if self.in_pos: return
        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT: return

        logj("buy_try", price=price, strat=strategy_name)
        r = self.api.buy_market(BUY_KRW_AMOUNT)
        
        if r and r.get('uuid'):
            self.in_pos = True
            self.entry_price = Decimal(str(price))
            
            target_price = adjust_price_to_tick(float(self.entry_price) * (1 + float(TP_PCT)))
            self.sl_price = self.entry_price * (Decimal("1") - SL_PCT)

            vol = Decimal("0")
            for _ in range(10):
                time.sleep(0.1) 
                vol = self.api.get_coin_free()
                if vol > 0: break
            
            if vol > 0:
                ord_res = self.api.sell_limit(target_price, vol)
                if ord_res and ord_res.get('uuid'):
                    self.tp_uuid = ord_res['uuid']
                    logj("tp_placed", price=target_price, sl_trigger=str(self.sl_price))
            else:
                logj("err", msg="Buy success but No Balance?")
        
        with self.lock:
            self.watch_target_b = None
            self.hold_start_b = None
            self.watch_target_r = None
            self.is_rebound_ready = False

    def _sl(self, price):
        logj("sl_trigger", price=price)
        if self.tp_uuid: self.api.cancel_order(self.tp_uuid)
        time.sleep(0.2) 
        
        vol = self.api.get_coin_free()
        if vol > 0: self.api.sell_market(vol)
        self._reset("SL_Done")

    def _reset(self, reason):
        self.in_pos = False
        self.entry_price = None
        self.tp_uuid = None
        self.sl_price = None
        
        self.watch_target_b = None
        self.hold_start_b = None
        self.watch_target_r = None
        self.is_rebound_ready = False
        logj("reset", msg=reason)

    def _on_tick(self, d):
        if 'trade_price' not in d: return
        p = Decimal(str(d['trade_price']))
        v = Decimal(str(d['trade_volume']))
        ts = d['timestamp']
        
        t = datetime.fromtimestamp(ts/1000)
        start = t.replace(second=0, microsecond=0, minute=(t.minute//3)*3)
        
        if self.cur_candle is None or self.cur_candle['start'] != start:
            if self.cur_candle: self._finalize_candle(self.cur_candle)
            self.cur_candle = {'start':start, 'o':p, 'c':p, 'h':p, 'l':p, 'vol':Decimal(0)}
        
        self.cur_candle['c'] = p
        if p > self.cur_candle['h']: self.cur_candle['h'] = p
        if p < self.cur_candle['l']: self.cur_candle['l'] = p
        self.cur_candle['vol'] += (p*v)

        with self.lock:
            # 1. 포지션 관리
            if self.in_pos:
                if self.sl_price and p <= self.sl_price:
                    self._sl(p) 
                    return
                elif self.tp_uuid:
                    if time.time() - self.tp_check_ts > 2:
                        if self.api.get_coin_free() < Decimal("0.0001"):
                            self._reset("TP_Done")
                        self.tp_check_ts = time.time()
                return

            # 2. 진입 감시
            # [전략 A] 돌파
            if self.watch_target_b:
                if p > self.watch_target_b:
                    if self.hold_start_b is None: self.hold_start_b = time.time()
                    elif time.time() - self.hold_start_b >= BREAKOUT_HOLD_SEC:
                        logj("breakout_hit_A", price=str(p))
                        self._enter(p, "BREAKOUT_A")
                        return
                else:
                    self.hold_start_b = None
            
            # [전략 B] 반등 (기준봉 1/3 회복 확인)
            if self.is_rebound_ready and self.watch_target_r:
                # 조건: 현재가가 타겟(1/3지점) 돌파 AND 현재 봉이 양봉 상태
                if (p >= self.watch_target_r) and (p >= self.cur_candle['o']):
                    logj("rebound_hit_B", price=str(p))
                    self._enter(p, "REBOUND_B")
                    return

    def start(self):
        logj("start", 
             setting=f"Coin:{SYMBOL}, Vol:>10억, TP:{TP_PCT}, Rebound:33%", 
             msg="Bot Started with Optimized Logic")
             
        while True:
            try:
                wm = WebSocketManager("trade", [SYMBOL])
                while True: 
                    d = wm.get()
                    if d: self._on_tick(d)
            except Exception as e:
                logj("ws_err", msg=str(e))
                time.sleep(3)

if __name__ == "__main__":
    BreakoutBot().start()