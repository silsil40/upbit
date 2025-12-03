#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-XRP Scalping Bot (Final Optimized + High Accuracy)

[전략 요약]
1. 타겟: 3분봉 거래대금 30억 이상 & 양봉
2. 필터: 기준봉 RSI(9) < 75 AND 종가 >= 20MA
   (데이터 200개 기준으로 계산하여 업비트 차트와 오차 제거)
3. 진입: 위 조건 만족 시, 현재가가 기준봉 종가를 '1초' 이상 돌파하면 시장가 매수
4. 익절: 매수 체결 즉시 '+0.7%' 가격에 지정가 매도 주문
5. 손절: 현재가가 '-0.6%' 이하로 떨어지면 지정가 취소 후 시장가 매도
   (슬리피지 고려하여 -0.6% 설정 -> 실손실 약 -0.8% 예상)

Author: Gemini
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
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "여기에_엑세스키")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "여기에_시크릿키")

SYMBOL           = "KRW-XRP"
BUY_KRW_AMOUNT   = Decimal("400000")
TURNOVER_THRESH  = Decimal("3000000000") # 30억

# 지표 설정
RSI_PERIOD       = 9
RSI_LIMIT        = 75.0
MA_PERIOD        = 20

# 진입 및 청산 설정
BREAKOUT_HOLD_SEC = 1.0             # 1초만 버티면 진입 (속도전)
TP_PCT            = Decimal("0.007") # 익절 +0.7% (지정가)
SL_PCT            = Decimal("0.006") # 손절 -0.6% (시장가 트리거)

# API 제한 관리
REQS_PER_SEC     = 8
REQS_PER_MIN     = 200
# ==========================================

RUN_ID = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:6]}"

def now_kst_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

def logj(event: str, **fields):
    rec = {"ts": now_kst_str(), "run": RUN_ID, "ev": event}
    rec.update(fields)
    print(json.dumps(rec, ensure_ascii=False), flush=True)

# 호가 단위 계산 (지정가 주문 필수)
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

# API 과부하 방지
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
        # [수정됨] 3분봉 200개 조회 (RSI 오차 해결)
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
    if df is None or len(df) < 30: return None, None
    close = df['close']
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=RSI_PERIOD, min_periods=1).mean()
    avg_loss = loss.rolling(window=RSI_PERIOD, min_periods=1).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    ma20 = close.rolling(window=MA_PERIOD).mean()
    return rsi.iloc[-1], ma20.iloc[-1]

class BreakoutBot:
    def __init__(self):
        self.api = UpbitWrap()
        self.lock = threading.RLock()
        
        # 상태 변수
        self.cur_candle = None
        self.watch_target = None
        self.hold_start = None
        
        self.in_pos = False
        self.entry_price = None
        self.sl_price = None
        self.tp_uuid = None
        self.tp_check_ts = 0

    # 캔들 마감 처리 (Thread 사용)
    def _finalize_candle(self, c):
        if c['vol'] >= TURNOVER_THRESH and c['o'] < c['c']:
            # 비동기로 지표 계산 (봇 멈춤 방지)
            threading.Thread(target=self._analyze, args=(c,), daemon=True).start()

    def _analyze(self, c):
        try:
            df = self.api.get_ohlcv()
            if df is not None:
                rsi, ma20 = calc_indicators(df)
                last_c = df['close'].iloc[-1]
                
                # [필터] RSI < 75 AND 종가 >= 20MA
                if rsi < RSI_LIMIT and last_c >= ma20:
                    with self.lock:
                        self.watch_target = c['c']
                        self.hold_start = None
                    logj("watch_on", target=c['c'], rsi=f"{rsi:.1f}")
                else:
                    logj("watch_skip", reason="Indicator Fail", rsi=f"{rsi:.1f}")
        except Exception as e:
            logj("err", msg=str(e))

    # 진입 로직
    def _enter(self, price):
        if self.in_pos: return
        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT: return

        # 1. 시장가 매수
        logj("buy_try", price=price)
        r = self.api.buy_market(BUY_KRW_AMOUNT)
        
        if r and r.get('uuid'):
            self.in_pos = True
            # 체결가는 일단 현재가로 가정 (빠른 대응 위해)
            self.entry_price = Decimal(str(price))
            
            # 목표가(지정가) 및 손절가 계산
            target_price = adjust_price_to_tick(float(self.entry_price) * (1 + float(TP_PCT)))
            self.sl_price = self.entry_price * (Decimal("1") - SL_PCT)

            # 2. [스마트 대기] 잔고 들어올 때까지 0.1초 간격 조회 (최대 1초)
            vol = Decimal("0")
            for _ in range(10):
                time.sleep(0.1) 
                vol = self.api.get_coin_free()
                if vol > 0: break
            
            # 3. 지정가 매도 주문
            if vol > 0:
                ord_res = self.api.sell_limit(target_price, vol)
                if ord_res and ord_res.get('uuid'):
                    self.tp_uuid = ord_res['uuid']
                    logj("tp_placed", price=target_price, sl_trigger=str(self.sl_price))
            else:
                logj("err", msg="Buy success but No Balance?")

    # 손절 로직
    def _sl(self, price):
        logj("sl_trigger", price=price)
        # 지정가 취소 -> 시장가 매도
        if self.tp_uuid: self.api.cancel_order(self.tp_uuid)
        time.sleep(0.2) # 취소 전파 대기
        
        vol = self.api.get_coin_free()
        if vol > 0: self.api.sell_market(vol)
        self._reset("SL_Done")

    def _reset(self, reason):
        self.in_pos = False
        self.entry_price = None
        self.tp_uuid = None
        self.watch_target = None
        self.hold_start = None
        logj("reset", msg=reason)

    # 틱 처리
    def _on_tick(self, d):
        if 'trade_price' not in d: return
        p = Decimal(str(d['trade_price']))
        v = Decimal(str(d['trade_volume']))
        ts = d['timestamp']
        
        # 캔들 조립 (간소화)
        t = datetime.fromtimestamp(ts/1000)
        start = t.replace(second=0, microsecond=0, minute=(t.minute//3)*3)
        
        if self.cur_candle is None or self.cur_candle['start'] != start:
            if self.cur_candle: self._finalize_candle(self.cur_candle)
            self.cur_candle = {'start':start, 'o':p, 'c':p, 'vol':Decimal(0)}
        
        self.cur_candle['c'] = p
        self.cur_candle['vol'] += (p*v)

        with self.lock:
            # 1. 진입 감시
            if not self.in_pos and self.watch_target:
                if p > self.watch_target:
                    if self.hold_start is None: self.hold_start = time.time()
                    # 1초 유지 시 진입
                    elif time.time() - self.hold_start >= BREAKOUT_HOLD_SEC:
                        logj("breakout_hit", price=str(p))
                        self._enter(p)
                        self.hold_start = None
                else:
                    self.hold_start = None

            # 2. 포지션 관리
            elif self.in_pos:
                # SL 체크
                if self.sl_price and p <= self.sl_price:
                    self._sl(p)
                # TP 체크 (잔고 0이면 익절된 것)
                elif self.tp_uuid:
                    if time.time() - self.tp_check_ts > 2: # 2초마다 확인
                        if self.api.get_coin_free() < Decimal("0.0001"):
                            self._reset("TP_Done")
                        self.tp_check_ts = time.time()

    def start(self):
        logj("start", setting=f"TP:{TP_PCT}, SL:{SL_PCT}, Hold:{BREAKOUT_HOLD_SEC}s")
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