#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-XRP Breakout Bot (Final Async Version)

전략 상세:
1. [트리거] 3분봉 거래대금 >= 30억 & 양봉
2. [필터1 - MA] 기준봉 종가가 20MA(이동평균) 위에 있을 것 (추세 지지)
3. [필터2 - RSI] 기준봉 마감 기준 RSI(9) < 75 (고점 추격 방지)
4. [진입] 위 조건 만족 후, 실시간 가격이 기준봉 종가를 '3초 연속 돌파'하면 진입
5. [청산] TP(+0.7%) / SL(-0.7%) 고정 (손익비 1:1)
6. [시스템] 지표 계산 시 메인 루프가 멈추지 않도록 Thread 사용

Author: Gemini
"""

import os
import time
import json
import threading
import random
import uuid
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import pyupbit
from pyupbit import WebSocketManager
import pandas as pd  # 지표 계산용

# ==========================================
# [사용자 설정 영역]
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL           = "KRW-XRP"         # 거래 대상 (리플)
BUY_KRW_AMOUNT   = Decimal("400000") # 1회 매수 금액 (원)

CANDLE_MINUTES   = 3                 # 3분봉 사용
TURNOVER_THRESH  = Decimal("3000000000")  # 기준 거래대금 (30억 원)

# 지표 필터 설정
RSI_PERIOD       = 9      # RSI 기간 (9일)
RSI_LIMIT        = 75.0   # RSI 75 이상이면 진입 금지 (과열)
MA_PERIOD        = 20     # 이동평균선 기간 (20일 생명선)

# 진입/청산 설정
BREAKOUT_HOLD_SEC = 3.0             # 돌파 후 3초 유지 시 진입
TP_PCT            = Decimal("0.007") # 익절 +0.7%
SL_PCT            = Decimal("0.007") # 손절 -0.7%

# API 관리
REQS_PER_SEC     = 8
REQS_PER_MIN     = 200
# ==========================================

RUN_ID = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:6]}"

def now_kst_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

def logj(event: str, **fields):
    """JSON 형식 로그 출력"""
    rec = {"ts": now_kst_str(), "run": RUN_ID, "ev": event}
    rec.update(fields)
    print(json.dumps(rec, ensure_ascii=False), flush=True)


# ===== [Class] TokenBucket (API 과부하 방지) =====
class TokenBucket:
    def __init__(self, per_sec:int, per_min:int):
        self.per_sec = per_sec
        self.per_min = per_min
        self.lock = threading.Lock()
        self.sec_tokens = per_sec
        self.min_tokens = per_min
        self.last_sec = int(time.time())
        self.last_min = int(time.time()//60)

    def _refill(self):
        t = time.time()
        s = int(t)
        m = int(t//60)
        if s != self.last_sec:
            self.sec_tokens = self.per_sec
            self.last_sec = s
        if m != self.last_min:
            self.min_tokens = self.per_min
            self.last_min = m

    def acquire(self):
        while True:
            with self.lock:
                self._refill()
                if self.sec_tokens > 0 and self.min_tokens > 0:
                    self.sec_tokens -= 1
                    self.min_tokens -= 1
                    return
            time.sleep(0.02)


# ===== [Class] Upbit API Wrapper =====
class UpbitWrap:
    def __init__(self):
        if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
            print("Warning: API Key가 설정되지 않았습니다.")
        self.u = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.tb = TokenBucket(REQS_PER_SEC, REQS_PER_MIN)
        self.MAX_RETRY = 5

    def _safe(self, fn, *args, desc="", validator=None, soft=False, **kwargs):
        delay = 0.5
        for attempt in range(1, self.MAX_RETRY+1):
            self.tb.acquire()
            try:
                r = fn(*args, **kwargs)
                if validator and not validator(r):
                    raise RuntimeError(f"Invalid response: {desc or fn.__name__}")
                return r
            except Exception as e:
                logj("api_err", where=desc or fn.__name__, attempt=attempt, err=str(e))
                if attempt < self.MAX_RETRY:
                    time.sleep(delay)
                    delay *= 1.5
                    continue
                if soft: return None
                raise

    def get_ohlcv(self, count=100):
        """Pandas DataFrame으로 캔들 데이터 조회"""
        try:
            self.tb.acquire()
            return pyupbit.get_ohlcv(SYMBOL, interval="minute3", count=count)
        except Exception as e:
            logj("api_err", where="get_ohlcv", err=str(e))
            return None

    def get_balance_krw(self):
        def _ok(v):
            try: Decimal(str(v)); return True
            except: return False
        r = self._safe(self.u.get_balance, "KRW", desc="get_balance", validator=_ok, soft=True)
        return Decimal(str(r or "0"))

    def get_coin_free(self):
        bals = self._safe(self.u.get_balances, desc="get_balances", soft=True) or []
        coin = SYMBOL.split("-")[1].upper()
        for b in bals:
            if b.get("currency","").upper() == coin:
                return Decimal(str(b.get("balance") or "0"))
        return Decimal("0")

    def buy_market(self, krw):
        return self._safe(self.u.buy_market_order, SYMBOL, float(krw), desc="buy_market", soft=True)

    def sell_market(self, vol):
        return self._safe(self.u.sell_market_order, SYMBOL, float(vol), desc="sell_market", soft=True)


# ===== [Function] 지표 계산 (Pandas) =====
def calc_indicators(df):
    """
    RSI(9)와 MA(20)을 계산하여 마지막 값 반환
    """
    if df is None or len(df) < 30:
        return None, None

    close = df['close']

    # RSI (9)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    
    avg_gain = gain.rolling(window=RSI_PERIOD, min_periods=1).mean()
    avg_loss = loss.rolling(window=RSI_PERIOD, min_periods=1).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    # MA (20)
    ma20 = close.rolling(window=MA_PERIOD).mean()

    return rsi.iloc[-1], ma20.iloc[-1]


# ===== [Class] Main Strategy Bot =====
class BreakoutBot:
    def __init__(self):
        self.api = UpbitWrap()
        self._stop = False
        self.lock = threading.RLock() # 틱 데이터 보호용

        # 캔들 및 타겟 관리
        self.cur_candle = None
        self.watch_target_close = None   # 감시할 기준 가격
        self.breakout_hold_since = None  # 돌파 유지 시간 측정

        # 포지션 관리
        self.in_position = False
        self.entry_price = None
        self.tp = None
        self.sl = None

        self.last_price = None

    # --- 3분 캔들 로직 ---
    @staticmethod
    def _bucket_start(ts: datetime):
        mm = (ts.minute // CANDLE_MINUTES) * CANDLE_MINUTES
        return ts.replace(second=0, microsecond=0, minute=mm)

    def _roll_candle_if_needed(self, t):
        start = self._bucket_start(t)
        end = start + timedelta(minutes=CANDLE_MINUTES)

        if self.cur_candle is None:
            self._new_candle(start, end)
            return

        if self.cur_candle["start_ts"] == start:
            self.cur_candle["end_ts"] = end
            return

        # 캔들이 바뀌는 시점: 이전 캔들 확정 -> 분석 시작
        self._finalize_candle()
        self._new_candle(start, end)

    def _new_candle(self, start, end):
        self.cur_candle = {
            "open": None, "high": None, "low": None, "close": None,
            "turnover": Decimal("0"), "start_ts": start, "end_ts": end,
        }

    def _finalize_candle(self):
        c = self.cur_candle
        if not c or c["open"] is None: return

        logj("candle_close", t=c["end_ts"].strftime("%H:%M"),
             c=str(c["close"]), turnover=f"{c['turnover']:,.0f}")

        # 1. 기본 조건: 거래대금 30억 이상 & 양봉
        if c["turnover"] >= TURNOVER_THRESH and c["open"] < c["close"]:
            # [중요] API 호출 시 멈춤(Blocking)을 방지하기 위해 쓰레드 사용
            threading.Thread(target=self._analyze_indicator, args=(c,), daemon=True).start()

    def _analyze_indicator(self, candle_data):
        """별도 쓰레드에서 지표 계산 및 진입 조건 설정"""
        try:
            # 과거 데이터 조회 (약 60개)
            df = self.api.get_ohlcv(count=60)
            
            if df is not None:
                # 지표 계산
                rsi, ma20 = calc_indicators(df)
                last_close = df['close'].iloc[-1]

                # 필터 조건 확인
                cond_rsi = rsi < RSI_LIMIT
                cond_ma  = last_close >= ma20

                logj("indicator_check", 
                     rsi=f"{rsi:.2f}", ma20=f"{ma20:.0f}", 
                     pass_rsi=str(cond_rsi), pass_ma=str(cond_ma))

                if cond_rsi and cond_ma:
                    # 모든 조건 만족 -> 감시 대상 설정
                    with self.lock: # 안전하게 변수 업데이트
                        self.watch_target_close = candle_data["close"]
                        self.breakout_hold_since = None
                    logj("watch_set", target=str(candle_data["close"]), msg="Setup Complete")
                else:
                    logj("watch_skip", msg="Filtered by Indicators")
            else:
                logj("err", msg="OHLCV Data fetch failed")
                
        except Exception as e:
            logj("err", where="analyze_thread", msg=str(e))

    def _apply_trade(self, price: Decimal, volume: Decimal, t: datetime):
        self._roll_candle_if_needed(t)
        c = self.cur_candle
        if c["open"] is None: 
            c["open"] = price; c["high"] = price; c["low"] = price
        c["close"] = price
        if price > c["high"]: c["high"] = price
        if price < c["low"]: c["low"] = price
        c["turnover"] += (price * volume)

    # --- 주문 실행 ---
    def _enter(self):
        if self.in_position: return
        
        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT:
            logj("skip", msg="KRW 부족", have=str(krw))
            return

        r = self.api.buy_market(BUY_KRW_AMOUNT)
        if r and r.get("uuid"):
            self.in_position = True
            self.entry_price = self.last_price
            self.entry_time = now_kst_str()
            
            # 익절/손절 계산
            self.tp = self.entry_price * (Decimal("1") + TP_PCT)
            self.sl = self.entry_price * (Decimal("1") - SL_PCT)
            
            logj("buy_done", price=str(self.entry_price), tp=str(self.tp), sl=str(self.sl))
        else:
            logj("buy_fail", resp=str(r))

    def _exit(self, reason, price):
        vol = self.api.get_coin_free()
        if vol > 0:
            r = self.api.sell_market(vol)
            logj("sell_done", reason=reason, price=str(price), resp=str(r))
        
        # 상태 초기화
        self.in_position = False
        self.entry_price = None
        self.tp = None
        self.sl = None
        with self.lock:
            self.watch_target_close = None
            self.breakout_hold_since = None
        logj("reset", msg="대기 상태로 복귀")

    # --- 틱 데이터 처리 (메인) ---
    def _on_tick(self, d):
        if not d.get("trade_price"): return
        
        price = Decimal(str(d["trade_price"]))
        vol = Decimal(str(d.get("trade_volume") or 0))
        ts = d["timestamp"]
        t = datetime.fromtimestamp(ts/1000, tz=timezone.utc).astimezone()

        with self.lock:
            self.last_price = price
            self._apply_trade(price, vol, t)

            # 1. 매수 진입 로직
            if not self.in_position and self.watch_target_close is not None:
                # 현재가가 기준봉 종가보다 높으면 타이머 시작
                if price > self.watch_target_close:
                    if self.breakout_hold_since is None:
                        self.breakout_hold_since = time.time()
                    # 3초 이상 유지 시
                    elif time.time() - self.breakout_hold_since >= BREAKOUT_HOLD_SEC:
                        logj("breakout_success", price=str(price))
                        self._enter()
                        self.breakout_hold_since = None
                else:
                    self.breakout_hold_since = None # 돌파 실패 시 리셋

            # 2. 청산 로직 (TP/SL)
            if self.in_position and self.tp and self.sl:
                if price >= self.tp:
                    self._exit("익절(TP)", price)
                elif price <= self.sl:
                    self._exit("손절(SL)", price)

    # --- 실행 루프 ---
    def start(self):
        logj("start", symbol=SYMBOL, rsi_limit=RSI_LIMIT, tp=f"{TP_PCT*100}%", sl=f"{SL_PCT*100}%")
        
        while not self._stop:
            wm = None
            try:
                wm = WebSocketManager("trade", [SYMBOL])
                while not self._stop:
                    data = wm.get()
                    if data:
                        self._on_tick(data)
            except Exception as e:
                logj("ws_error", err=str(e))
                time.sleep(3)
            finally:
                if wm: wm.terminate()

if __name__ == "__main__":
    BreakoutBot().start()