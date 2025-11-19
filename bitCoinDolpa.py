#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-XRP Breakout Bot (No-Trailing, No-Partial, Fixed TP/SL)

전략 요약:
- 3분봉 거래대금 >= 30억 & 양봉 → 해당 봉 종가 기록
- 실시간 가격이 종가를 '3초 연속 초과'하면 시장가 진입(400,000원)
- TP = +0.6% (전량)
- SL = -0.8% (전량)
- 부분매도 없음, 트레일링 없음
- 청산 후 즉시 IDLE로 복귀
"""

import os, time, json, threading, random, uuid
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone, timedelta

import pyupbit
from pyupbit import WebSocketManager

# ===== 설정 =====
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL           = os.getenv("SYMBOL", "KRW-XRP")
BUY_KRW_AMOUNT   = Decimal(os.getenv("BUY_KRW_AMOUNT", "400000"))

CANDLE_MINUTES   = 3
TURNOVER_THRESH  = Decimal("3000000000")  # 30억 KRW

# 돌파 유지 조건
BREAKOUT_HOLD_SEC = 3.0

# TP / SL (고정)
TP_PCT = Decimal("0.006")   # +0.6%
SL_PCT = Decimal("0.008")   # -0.8%

# 요청 제한
REQS_PER_SEC     = 8
REQS_PER_MIN     = 200

RUN_ID = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:6]}"

def now_kst_dt():
    return datetime.now(timezone.utc).astimezone()

def now_kst_str():
    return now_kst_dt().strftime("%Y-%m-%d %H:%M:%S")

def logj(event: str, **fields):
    rec = {"ts": now_kst_str(), "run": RUN_ID, "ev": event}
    rec.update(fields)
    print(json.dumps(rec, ensure_ascii=False), flush=True)


# ===== 토큰버킷 =====
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


# ===== Upbit API 래퍼 =====
class UpbitWrap:
    def __init__(self):
        if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
            raise SystemExit("UPBIT 키(UPBIT_ACCESS_KEY/UPBIT_SECRET_KEY)가 비어 있음")
        self.u = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.tb = TokenBucket(REQS_PER_SEC, REQS_PER_MIN)
        self.MAX_RETRY = 5
        self.BACKOFF_MAX = 20.0

    def _safe(self, fn, *args, desc="", validator=None, soft=False, **kwargs):
        delay = 1.0
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
                    time.sleep(min(delay, self.BACKOFF_MAX) * random.uniform(0.8, 1.2))
                    delay = min(delay*2, self.BACKOFF_MAX)
                    continue
                if soft:
                    return None
                raise

    def get_balance_krw(self):
        def _ok(v):
            try: Decimal(str(v)); return True
            except: return False
        r = self._safe(self.u.get_balance, "KRW", desc="get_balance(KRW)", validator=_ok, soft=True)
        return Decimal(str(r or "0"))

    def get_coin_free(self):
        bals = self._safe(self.u.get_balances, desc="get_balances", soft=True) or []
        coin = SYMBOL.split("-")[1].upper()
        for b in bals:
            if b.get("currency","").upper() == coin:
                return Decimal(str(b.get("balance") or "0"))
        return Decimal("0")

    def buy_market_krw(self, krw: Decimal):
        return self._safe(self.u.buy_market_order, SYMBOL, float(krw), desc="buy_market", soft=True)

    def sell_market(self, volume: Decimal):
        return self._safe(self.u.sell_market_order, SYMBOL, float(volume), desc="sell_market", soft=True)


# ===== 메인 전략 =====
class BreakoutSimpleBot:
    """
    매우 단순화된 Breakout Bot:
    - watch_target_close 돌파 3초 유지 → 전량 매수
    - TP / SL 도달 → 전량 매도
    """

    def __init__(self):
        self.api = UpbitWrap()
        self._stop = False

        self.last_price = None
        self.lock = threading.RLock()

        # 캔들 데이터
        self.cur_candle = None
        self.watch_target_close = None
        self.breakout_hold_since = None

        # 포지션
        self.in_position = False
        self.entry_price = None
        self.entry_time = None

        # TP/SL
        self.tp = None
        self.sl = None

    # === 3분 캔들 관리 ===
    @staticmethod
    def _bucket_start(ts: datetime):
        mm = (ts.minute // CANDLE_MINUTES) * CANDLE_MINUTES
        return ts.replace(second=0, microsecond=0, minute=mm)

    def _roll_candle_if_needed(self, t):
        start = self._bucket_start(t)
        end = start + timedelta(minutes=CANDLE_MINUTES)

        if self.cur_candle is None:
            self.cur_candle = {
                "open": None, "high": None, "low": None, "close": None,
                "turnover": Decimal("0"),
                "start_ts": start, "end_ts": end,
            }
            return

        if self.cur_candle["start_ts"] == start:
            self.cur_candle["end_ts"] = end
            return

        # 캔들 확정
        self._finalize_candle()

        # 새 캔들 시작
        self.cur_candle = {
            "open": None, "high": None, "low": None, "close": None,
            "turnover": Decimal("0"),
            "start_ts": start, "end_ts": end,
        }

    def _finalize_candle(self):
        c = self.cur_candle
        if not c or c["open"] is None:
            return

        logj("candle_close",
             start=c["start_ts"].strftime("%Y-%m-%d %H:%M:%S"),
             end=c["end_ts"].strftime("%Y-%m-%d %H:%M:%S"),
             o=str(c["open"]), h=str(c["high"]), l=str(c["low"]),
             c=str(c["close"]), turnover=str(c["turnover"]))

        # 30억 + 양봉
        if c["turnover"] >= TURNOVER_THRESH and c["open"] < c["close"]:
            self.watch_target_close = c["close"]
            self.breakout_hold_since = None
            logj("watch_set",
                 close=str(self.watch_target_close),
                 turnover=str(c["turnover"]))

    def _apply_trade(self, price: Decimal, volume: Decimal, t: datetime):
        self._roll_candle_if_needed(t)
        c = self.cur_candle
        if c["open"] is None:
            c["open"] = price
            c["high"] = price
            c["low"]  = price
        c["close"] = price
        if price > c["high"]: c["high"] = price
        if price < c["low"]:  c["low"]  = price
        c["turnover"] += (price * volume)

    # === 포지션 관리 ===
    def _enter(self):
        if self.in_position:
            return
        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT:
            logj("krw_short", have=str(krw))
            return

        r = self.api.buy_market_krw(BUY_KRW_AMOUNT)
        if not isinstance(r, dict) or not r.get("uuid"):
            logj("buy_fail", resp=str(r))
            return

        self.in_position = True
        self.entry_price = self.last_price
        self.entry_time  = now_kst_str()

        # 전량 TP / SL
        self.tp = self.entry_price * (Decimal("1") + TP_PCT)
        self.sl = self.entry_price * (Decimal("1") - SL_PCT)

        logj("buy_done",
             entry=str(self.entry_price),
             tp=str(self.tp),
             sl=str(self.sl),
             uuid=r.get("uuid"))

    def _exit(self, reason, price):
        free = self.api.get_coin_free()
        if free > 0:
            r = self.api.sell_market(free)
            logj("sell_all", reason=reason, price=str(price),
                 vol=str(free), resp=str(r))
        self._reset()

    def _reset(self):
        self.in_position = False
        self.entry_price = None
        self.tp = None
        self.sl = None
        self.watch_target_close = None
        self.breakout_hold_since = None
        logj("reset")

    # === WS Tick Handling ===
    def _on_tick(self, raw):
        price = raw.get("trade_price")
        vol   = raw.get("trade_volume")
        ts    = raw.get("timestamp")
        if price is None:
            return

        price = Decimal(str(price))
        vol = Decimal(str(vol or 0))
        t = datetime.fromtimestamp(ts/1000, tz=timezone.utc).astimezone()

        with self.lock:
            self.last_price = price
            self._apply_trade(price, vol, t)

            # --- 매수 전 ---
            if not self.in_position and self.watch_target_close is not None:
                if price > self.watch_target_close:
                    if self.breakout_hold_since is None:
                        self.breakout_hold_since = time.time()
                    elif time.time() - self.breakout_hold_since >= BREAKOUT_HOLD_SEC:
                        logj("breakout_hit", price=str(price))
                        self._enter()
                        self.breakout_hold_since = None
                else:
                    self.breakout_hold_since = None

            # --- 매수 후 TP/SL ---
            if self.in_position:
                if price >= self.tp:
                    self._exit("TP", price)
                elif price <= self.sl:
                    self._exit("SL", price)

    # === WS 루프 ===
    def _ws_loop(self):
        backoff = 1.0
        while not self._stop:
            wm = None
            try:
                wm = WebSocketManager("trade", [SYMBOL])
                while not self._stop:
                    self._on_tick(wm.get())
            except Exception as e:
                logj("ws_err", err=str(e))
            finally:
                if wm: 
                    try: wm.terminate()
                    except: pass
            time.sleep(1)

    def start(self):
        logj("start", symbol=SYMBOL, tp=str(TP_PCT), sl=str(SL_PCT))
        t = threading.Thread(target=self._ws_loop, daemon=True)
        t.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self._stop = True
        logj("stop")


if __name__ == "__main__":
    BreakoutSimpleBot().start()
