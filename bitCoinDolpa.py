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
import traceback  # [적용됨] 에러 위치 추적 모듈
from decimal import Decimal, getcontext
from datetime import datetime, timezone

import pyupbit
from pyupbit import WebSocketManager
import pandas as pd

# Decimal 정밀도 설정
getcontext().prec = 28

# ==========================================
# [사용자 설정 영역]
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL             = "KRW-SOL" 
BUY_KRW_AMOUNT     = Decimal("400000")
TURNOVER_THRESH    = Decimal("1000000000") 

# 지표 상수 (Decimal)
RSI_PERIOD         = 9
RSI_LIMIT          = Decimal("75.0") 
RSI_REBOUND_LIMIT  = Decimal("25.0") 
MA_PERIOD          = 20

BREAKOUT_HOLD_SEC  = 1.0              
TP_PCT             = Decimal("0.007") 
SL_PCT             = Decimal("0.007") 

REQS_PER_SEC       = 8
REQS_PER_MIN       = 200
# ==========================================

RUN_ID = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:6]}"

def now_kst_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, Decimal):
                return str(obj)
            return super(DecimalEncoder, self).default(obj)
        except:
            return str(obj)

def logj(event: str, **fields):
    try:
        rec = {"ts": now_kst_str(), "run": RUN_ID, "ev": event}
        rec.update(fields)
        print(json.dumps(rec, ensure_ascii=False, cls=DecimalEncoder), flush=True)
    except Exception as e:
        # 로그 에러가 나도 매매 로직은 계속 돌아가도록 print로 대체
        print(f"[LOG_ERROR_BYPASS] event={event} error={str(e)}", flush=True)

# [수정됨] 사용자 요청 호가 단위 적용 (10만->100원, 50만->500원)
def adjust_price_to_tick(price):
    try:
        p = Decimal(str(price))
        
        if p >= 2000000:   tick = Decimal("1000")
        elif p >= 500000:  tick = Decimal("500")  # [수정] 50만원 이상 -> 500원
        elif p >= 100000:  tick = Decimal("100")  # [수정] 10만원 이상 -> 100원
        elif p >= 10000:   tick = Decimal("10")
        elif p >= 1000:    tick = Decimal("1")
        elif p >= 100:     tick = Decimal("0.1")
        elif p >= 10:      tick = Decimal("0.01")
        else: tick = Decimal("0.0001")
        
        floor_val = (p / tick).to_integral_value(rounding='ROUND_FLOOR')
        return floor_val * tick
    except Exception:
        return Decimal(str(price))

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
        
        self.cur_candle = None
        self.in_pos = False         
        self.entry_price = None     
        self.sl_price = None        
        self.tp_uuid = None         
        self.tp_check_ts = 0
        
        self.watch_target_b = None  
        self.hold_start_b = None    
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

            rsi_series, ma20_val, prev_close, prev_volume = calc_indicators(df)
            if rsi_series is None: return 
            
            current_rsi = Decimal(str(rsi_series.iloc[-1]))
            ma20 = Decimal(str(ma20_val))

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
                        self.watch_target_r = adjust_price_to_tick(target_raw)
                        
                        logj("rebound_watch_on", 
                             status="New Pattern Found",
                             close=c['c'], 
                             target=self.watch_target_r,
                             rsi=f"{current_rsi:.1f}",
                             vol_krw=f"{int(c['vol']):,}")

            # [전략 A] 돌파 매매
            if c['vol'] >= TURNOVER_THRESH and c['o'] < c['c']: 
                is_valid_breakout = (current_rsi < RSI_LIMIT) and (c['c'] >= ma20)
                with self.lock:
                    if is_valid_breakout:
                        self.watch_target_b = c['c']
                        self.hold_start_b = None
                        logj("watch_on_Breakout", target=c['c'], rsi=f"{current_rsi:.1f}")
                    else:
                        self.watch_target_b = None

        except Exception:
            # [적용됨] 에러 발생 시 Traceback(상세 라인) 로그 출력
            logj("err_analyze", trace=traceback.format_exc())

    def _enter(self, price, strategy_name):
        try:
            if self.in_pos: return
            krw = self.api.get_balance_krw()
            if krw < BUY_KRW_AMOUNT: return

            logj("buy_try", price=price, strat=strategy_name)
            
            r = self.api.buy_market(BUY_KRW_AMOUNT)
            
            if r and r.get('uuid'):
                self.in_pos = True
                self.entry_price = Decimal(str(price))
                
                target_raw = self.entry_price * (Decimal("1") + TP_PCT)
                target_price = adjust_price_to_tick(target_raw)
                
                self.sl_price = self.entry_price * (Decimal("1") - SL_PCT)

                vol = Decimal("0")
                for _ in range(30): 
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

        except Exception:
            # [적용됨] 에러 발생 시 Traceback(상세 라인) 로그 출력
            logj("err_enter_crit", trace=traceback.format_exc())

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

                if self.watch_target_b:
                    if p > self.watch_target_b:
                        if self.hold_start_b is None: self.hold_start_b = time.time()
                        elif time.time() - self.hold_start_b >= BREAKOUT_HOLD_SEC:
                            logj("breakout_hit_A", price=p)
                            self._enter(p, "BREAKOUT_A")
                            return
                    else:
                        self.hold_start_b = None
                
                if self.is_rebound_ready and self.watch_target_r:
                    if (p >= self.watch_target_r) and (p >= self.cur_candle['o']):
                        logj("rebound_hit_B", price=p)
                        self._enter(p, "REBOUND_B")
                        return

        except Exception:
            # [적용됨] 틱 처리 중 에러 발생 시 Traceback 출력
            logj("err_tick", trace=traceback.format_exc())

    def start(self):
        logj("start", 
             setting=f"Coin:{SYMBOL}, Vol:>10억, TP:{TP_PCT}, Rebound:33%", 
             msg="Bot Started with TRACEBACK & CUSTOM TICK")
        
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