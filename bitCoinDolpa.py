#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[전략 요약: 업비트 솔라나(SOL) 초단타 스캘핑 봇]

1. 기본 환경
   - 대상: KRW-SOL (솔라나)
   - 기준: 3분봉 (WebSocket 실시간 감시)
   - 필수 조건: 3분봉 거래대금 10억 원 이상 폭발 시에만 전략 가동

2. 매수 전략 (Dual Strategy)
   A. 급등주 눌림/돌파 (Trend Follow)
      - 조건: 양봉 마감 + 20일 이평선 위 + RSI 75 미만 (고점 추격 방지)
      - 진입: 직전 종가를 뚫고 올라가서 1초 이상 가격 유지 시 매수

   B. 낙폭과대 반등 (Reversal)
      - 조건: 3연속 음봉(흑삼병) + RSI 25 미만 (과매도) + 투매 물량(10억 이상)
      - 진입: 마지막 음봉 몸통의 33% 가격까지 반등 시 매수

3. 청산 전략 (Risk Management)
   - 익절(TP): +0.7% (매수 즉시 지정가 매도 주문)
   - 손절(SL): -0.7% (가격 도달 시 즉시 시장가 매도)
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
# [사용자 설정 영역]
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

# [설정] 종목: 솔라나
SYMBOL             = "KRW-SOL" 
BUY_KRW_AMOUNT     = Decimal("400000")     # 1회 매수 금액 (40만 원)

# [설정] 거래대금 기준: 10억 원
TURNOVER_THRESH    = Decimal("1000000000") 

# 지표 설정
RSI_PERIOD         = 9
RSI_LIMIT          = 75.0 # [전략 A] 돌파 시 RSI가 이 값보다 낮아야 함
RSI_REBOUND_LIMIT  = 25.0 # [전략 B] 반등 시 RSI가 이 값보다 낮아야 함
MA_PERIOD          = 20

# 진입 및 청산 설정
BREAKOUT_HOLD_SEC  = 1.0              # [전략 A] 돌파 시 1초간 가격 유지해야 진입
TP_PCT             = Decimal("0.007") # 익절 목표: +0.7%
SL_PCT             = Decimal("0.007") # 손절 기준: -0.7%

# API 호출 제한 (업비트 정책 준수)
REQS_PER_SEC       = 8
REQS_PER_MIN       = 200
# ==========================================

RUN_ID = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:6]}"

def now_kst_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

# [수정 1] Decimal 타입을 JSON으로 변환하기 위한 인코더 클래스 추가
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)  # Decimal을 문자열로 변환
        return super(DecimalEncoder, self).default(obj)

# [수정 2] logj 함수에서 DecimalEncoder 사용하도록 변경
def logj(event: str, **fields):
    rec = {"ts": now_kst_str(), "run": RUN_ID, "ev": event}
    rec.update(fields)
    # cls=DecimalEncoder 옵션 추가
    print(json.dumps(rec, ensure_ascii=False, cls=DecimalEncoder), flush=True)

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
        for _ in range(3):
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

    def sell_limit(self, price, vol):
        return self._safe(self.u.sell_limit_order, SYMBOL, float(price), float(vol))

    def sell_market(self, vol):
        return self._safe(self.u.sell_market_order, SYMBOL, float(vol))

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
        
        # [상태 변수들]
        self.cur_candle = None
        self.in_pos = False         
        self.entry_price = None     
        self.sl_price = None        
        self.tp_uuid = None         
        self.tp_check_ts = 0
        
        # [전략 A 변수]
        self.watch_target_b = None  
        self.hold_start_b = None    
        
        # [전략 B 변수]
        self.watch_target_r = None   
        self.is_rebound_ready = False 

    def _finalize_candle(self, c):
        threading.Thread(target=self._analyze_and_manage_strategy, args=(c,), daemon=True).start()

    def _analyze_and_manage_strategy(self, c):
        try:
            df = self.api.get_ohlcv()
            if df is None: return

            last_api_time = df.index[-1]
            if last_api_time < c['start']:
                new_row = pd.DataFrame([{
                    'open': float(c['o']), 'high': float(c['h']), 'low': float(c['l']),
                    'close': float(c['c']), 'volume': float(c['vol']),
                    'value': float(c['vol'] * c['c'])
                }], index=[c['start']])
                df = pd.concat([df, new_row])

            rsi_series, ma20, prev_close, prev_volume = calc_indicators(df)
            if rsi_series is None: return # 데이터 부족 시 리턴
            current_rsi = rsi_series.iloc[-1] 

            # ----------------------------------------------------
            # 1. [전략 B] 낙폭 반등
            # ----------------------------------------------------
            with self.lock:
                if self.is_rebound_ready:
                      logj("rebound_expired", reason="Candle Closed (Missed)")
                self.is_rebound_ready = False
                self.watch_target_r = None

            if len(df) >= 3:
                recent_3 = df.iloc[-3:] 
                
                closes = recent_3['close'].values
                opens = recent_3['open'].values
                is_three_crows = all(closes < opens)

                is_oversold = current_rsi < RSI_REBOUND_LIMIT
                is_vol_spike = (c['vol'] >= TURNOVER_THRESH)

                if is_three_crows and is_oversold and is_vol_spike:
                    with self.lock:
                        self.is_rebound_ready = True
                        
                        body_len = c['o'] - c['c']
                        target_raw = c['c'] + (body_len * Decimal("0.33"))
                        self.watch_target_r = adjust_price_to_tick(float(target_raw))
                        
                        logj("rebound_watch_on", 
                             status="New Pattern Found",
                             close=c['c'], 
                             target=self.watch_target_r,
                             rsi=f"{current_rsi:.1f}",
                             vol_krw=f"{int(c['vol']):,}")

            # ----------------------------------------------------
            # 2. [전략 A] 돌파 매매
            # ----------------------------------------------------
            if c['vol'] >= TURNOVER_THRESH and c['o'] < c['c']: 
                is_valid_breakout = (current_rsi < RSI_LIMIT) and (c['c'] >= ma20)
                with self.lock:
                    if is_valid_breakout:
                        self.watch_target_b = c['c']
                        self.hold_start_b = None
                        logj("watch_on_Breakout", target=c['c'], rsi=f"{current_rsi:.1f}")
                    else:
                        self.watch_target_b = None

        except Exception as e:
            logj("err_analyze", msg=str(e))

    def _enter(self, price, strategy_name):
        if self.in_pos: return
        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT: return

        # [수정] logj가 Decimal을 처리해주므로 바로 넘겨도 됨
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
                    logj("tp_placed", price=target_price, sl_trigger=self.sl_price)
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
        # [수정 3] Tick 처리 중 에러가 나도 봇이 죽지 않도록 보호
        try:
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
                            logj("breakout_hit_A", price=p)
                            self._enter(p, "BREAKOUT_A")
                            return
                    else:
                        self.hold_start_b = None
                
                # [전략 B] 반등
                if self.is_rebound_ready and self.watch_target_r:
                    if (p >= self.watch_target_r) and (p >= self.cur_candle['o']):
                        logj("rebound_hit_B", price=p)
                        self._enter(p, "REBOUND_B")
                        return

        except Exception as e:
            # 에러 발생 시 로그만 남기고 봇은 계속 수행
            logj("err_tick", msg=str(e))

    def start(self):
        logj("start", 
             setting=f"Coin:{SYMBOL}, Vol:>10억, TP:{TP_PCT}, Rebound:33%", 
             msg="Bot Started with Fixed JSON & SafeGuard")
        
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