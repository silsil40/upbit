#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-SOL Scalping Bot (Final Commented Version)
목표: 솔라나(SOL)
전략 A: 급등주 돌파 (양봉 + 10억 이상 + RSI 안정적)
전략 B: 낙폭과대 반등 (3연속 음봉 후 10억 터진 투매 -> 33% 반등 시 진입)
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
# 20억이 아니라 10억으로 설정되어 있습니다. 이보다 많이 터지면 조건 만족입니다.
TURNOVER_THRESH    = Decimal("1000000000") 

# 지표 설정
RSI_PERIOD         = 9
RSI_LIMIT          = 75.0 # [전략 A] 돌파 시 RSI가 이 값보다 낮아야 함 (너무 고점이면 위험)
RSI_REBOUND_LIMIT  = 25.0 # [전략 B] 반등 시 RSI가 이 값보다 낮아야 함 (과매도 상태여야 함)
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

def logj(event: str, **fields):
    rec = {"ts": now_kst_str(), "run": RUN_ID, "ev": event}
    rec.update(fields)
    print(json.dumps(rec, ensure_ascii=False), flush=True)

# 호가 단위 계산 함수 (가격을 업비트 호가창 단위에 맞게 버림 처리)
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

# API 과부하 방지를 위한 토큰 버킷 (요청 횟수 제한 관리)
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

    # 안전한 API 호출 (실패 시 3회 재시도)
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

# 보조지표(RSI, 이동평균선) 계산 함수
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
    
    # 반환값: RSI시리즈, 현재 MA20, 직전 종가, 직전 거래량
    return rsi, ma20.iloc[-1], df['close'].iloc[-2], df['volume'].iloc[-2]

class BreakoutBot:
    def __init__(self):
        self.api = UpbitWrap()
        self.lock = threading.RLock()
        
        # [상태 변수들]
        self.cur_candle = None
        self.in_pos = False         # 포지션 보유 여부
        self.entry_price = None     # 진입 가격
        self.sl_price = None        # 손절 가격
        self.tp_uuid = None         # 익절 주문 번호
        self.tp_check_ts = 0
        
        # [전략 A 변수]
        self.watch_target_b = None  # 돌파 감시 가격
        self.hold_start_b = None    # 돌파 유지 시간 측정용
        
        # [전략 B 변수]
        self.watch_target_r = None   # 반등 감시 가격 (33% 지점)
        self.is_rebound_ready = False # 반등 감시 모드 활성화 여부

    # 3분봉 마감 시 호출되는 함수 (비동기 쓰레드로 전략 분석 실행)
    def _finalize_candle(self, c):
        threading.Thread(target=self._analyze_and_manage_strategy, args=(c,), daemon=True).start()

    # ==================================================================
    # [핵심] 전략 분석 및 감시 설정 로직
    # ==================================================================
    def _analyze_and_manage_strategy(self, c):
        try:
            df = self.api.get_ohlcv()
            if df is None: return

            # [데이터 동기화 Fix]
            # 웹소켓은 봉이 끝났다고 알려줬는데, API 데이터에는 아직 그 봉이 없을 수 있음.
            # 이 경우 강제로 방금 끝난 봉(c)을 데이터프레임(df) 뒤에 붙여서 지표 오류를 방지함.
            last_api_time = df.index[-1]
            if last_api_time < c['start']:
                new_row = pd.DataFrame([{
                    'open': float(c['o']), 'high': float(c['h']), 'low': float(c['l']),
                    'close': float(c['c']), 'volume': float(c['vol']),
                    'value': float(c['vol'] * c['c'])
                }], index=[c['start']])
                df = pd.concat([df, new_row])

            # 지표 계산 (이제 df는 최신 상태임)
            rsi_series, ma20, prev_close, prev_volume = calc_indicators(df)
            current_rsi = rsi_series.iloc[-1] # [중요] 방금 마감된 봉의 RSI (폭락 직후 RSI 확인)

            # ----------------------------------------------------
            # 1. [전략 B] 낙폭 반등 (슬라이딩 윈도우 방식)
            # ----------------------------------------------------
            
            # (A) 감시 리셋 (Reset)
            # 봉이 새로 마감되었다는 건, 이전 봉에서 33% 반등에 실패했다는 뜻.
            # 따라서 무조건 기존 감시는 해제하고, 이번 봉 기준으로 새로 판단해야 함.
            with self.lock:
                if self.is_rebound_ready:
                     logj("rebound_expired", reason="Candle Closed (Missed)")
                self.is_rebound_ready = False
                self.watch_target_r = None

            # (B) 슬라이딩 윈도우 체크 (Sliding Window)
            # 전체 데이터가 아니라, 무조건 '가장 최근 3개' 봉만 잘라서 패턴을 확인
            if len(df) >= 3:
                recent_3 = df.iloc[-3:] # [전전, 직전, 현재(c)]
                
                # 조건 1: 3개가 모두 음봉인가? (하나라도 양봉 섞이면 즉시 탈락 -> 초기화 효과)
                closes = recent_3['close'].values
                opens = recent_3['open'].values
                is_three_crows = all(closes < opens)

                # 조건 2: 현재(마지막) 봉의 RSI가 25 미만인가? (과매도 상태)
                is_oversold = current_rsi < RSI_REBOUND_LIMIT

                # 조건 3: 현재(마지막) 봉의 거래대금이 10억 이상 터졌는가?
                # (20억이 터졌다면 당연히 10억 이상이므로 OK)
                is_vol_spike = (c['vol'] >= TURNOVER_THRESH)

                if is_three_crows and is_oversold and is_vol_spike:
                    with self.lock:
                        self.is_rebound_ready = True
                        
                        # 목표가 계산: 현재 봉 종가 + (몸통 길이 * 33%)
                        body_len = c['o'] - c['c']
                        target_raw = c['c'] + (body_len * Decimal("0.33"))
                        self.watch_target_r = adjust_price_to_tick(float(target_raw))
                        
                        logj("rebound_watch_on", 
                             status="New Pattern Found",
                             close=str(c['c']), 
                             target=str(self.watch_target_r),
                             rsi=f"{current_rsi:.1f}",
                             vol_krw=f"{int(c['vol']):,}")

            # ----------------------------------------------------
            # 2. [전략 A] 돌파 매매 (기존 로직 유지)
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

    # 매수 주문 실행 함수
    def _enter(self, price, strategy_name):
        if self.in_pos: return
        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT: return

        logj("buy_try", price=price, strat=strategy_name)
        r = self.api.buy_market(BUY_KRW_AMOUNT)
        
        if r and r.get('uuid'):
            self.in_pos = True
            self.entry_price = Decimal(str(price))
            
            # 익절 주문 미리 걸기 (지정가)
            target_price = adjust_price_to_tick(float(self.entry_price) * (1 + float(TP_PCT)))
            self.sl_price = self.entry_price * (Decimal("1") - SL_PCT)

            # 코인 잔고가 조회될 때까지 잠시 대기 (체결 딜레이 고려)
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
        
        # 진입 성공 시 모든 감시 해제
        with self.lock:
            self.watch_target_b = None
            self.hold_start_b = None
            self.watch_target_r = None
            self.is_rebound_ready = False

    # 손절 실행 함수
    def _sl(self, price):
        logj("sl_trigger", price=price)
        if self.tp_uuid: self.api.cancel_order(self.tp_uuid) # 익절 주문 취소
        time.sleep(0.2) 
        
        vol = self.api.get_coin_free()
        if vol > 0: self.api.sell_market(vol) # 시장가 전량 매도
        self._reset("SL_Done")

    # 상태 초기화 함수
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

    # 실시간 시세 수신 (WebSocket)
    def _on_tick(self, d):
        if 'trade_price' not in d: return
        p = Decimal(str(d['trade_price']))
        v = Decimal(str(d['trade_volume']))
        ts = d['timestamp']
        
        # 3분봉 기준 시간 계산
        t = datetime.fromtimestamp(ts/1000)
        start = t.replace(second=0, microsecond=0, minute=(t.minute//3)*3)
        
        # 봉이 바뀌었는지 체크
        if self.cur_candle is None or self.cur_candle['start'] != start:
            if self.cur_candle: self._finalize_candle(self.cur_candle) # 이전 봉 마감 처리
            self.cur_candle = {'start':start, 'o':p, 'c':p, 'h':p, 'l':p, 'vol':Decimal(0)}
        
        # 현재 봉 데이터 갱신
        self.cur_candle['c'] = p
        if p > self.cur_candle['h']: self.cur_candle['h'] = p
        if p < self.cur_candle['l']: self.cur_candle['l'] = p
        self.cur_candle['vol'] += (p*v)

        with self.lock:
            # 1. 포지션 관리 (익절/손절)
            if self.in_pos:
                if self.sl_price and p <= self.sl_price:
                    self._sl(p) # 손절 조건 도달
                    return
                elif self.tp_uuid:
                    # 익절 주문이 체결되었는지 잔고 확인으로 체크 (2초 간격)
                    if time.time() - self.tp_check_ts > 2:
                        if self.api.get_coin_free() < Decimal("0.0001"):
                            self._reset("TP_Done")
                        self.tp_check_ts = time.time()
                return

            # 2. 진입 감시
            # [전략 A] 돌파 매매
            if self.watch_target_b:
                if p > self.watch_target_b:
                    if self.hold_start_b is None: self.hold_start_b = time.time()
                    elif time.time() - self.hold_start_b >= BREAKOUT_HOLD_SEC:
                        logj("breakout_hit_A", price=str(p))
                        self._enter(p, "BREAKOUT_A")
                        return
                else:
                    self.hold_start_b = None
            
            # [전략 B] 반등 매매 (33% 지점 회복 여부 감시)
            if self.is_rebound_ready and self.watch_target_r:
                # 조건: 현재가가 타겟 이상이고, 현재 봉이 양봉이어야 함
                if (p >= self.watch_target_r) and (p >= self.cur_candle['o']):
                    logj("rebound_hit_B", price=str(p))
                    self._enter(p, "REBOUND_B")
                    return

    def start(self):
        logj("start", 
             setting=f"Coin:{SYMBOL}, Vol:>10억, TP:{TP_PCT}, Rebound:33%", 
             msg="Bot Started with Rolling Window & Sync Fix")
        
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