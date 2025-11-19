#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-XRP Breakout Bot (전량매수/전량매도 + 재진입 금지 버전)

전략 요약
- 3분봉 기준
- 거래대금 >= 30억 AND 양봉이면 → watch_close 세팅
- 이후 실시간 가격이 종가 돌파 후 3초 유지되면 매수(40만 원)
- TP +0.6% / SL -0.8%
- 부분매도/트레일링 없음 (전량 매도)
- 매도한 그 3분봉에서는 재진입 절대 금지
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
TURNOVER_THRESH  = Decimal("3000000000")  # 30억
BREAKOUT_HOLD_SEC = 3.0

# 고정 TP/SL
TP_PCT = Decimal("0.006")   # +0.6%
SL_PCT = Decimal("0.008")   # -0.8%

# 요청 제한
REQS_PER_SEC = 8
REQS_PER_MIN = 200

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
    def __init__(self, per_sec, per_min):
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

# ===== Upbit 래퍼 =====
class UpbitWrap:
    def __init__(self):
        if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
            raise SystemExit("UPBIT 키 없음")
        self.u = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.tb = TokenBucket(REQS_PER_SEC, REQS_PER_MIN)
        self.MAX_RETRY = 5
        self.BACKOFF_MAX = 20.0

    def _safe(self, fn, *args, soft=False, desc="", validator=None, **kwargs):
        delay = 1.0
        for attempt in range(1, self.MAX_RETRY+1):
            self.tb.acquire()
            try:
                r = fn(*args, **kwargs)
                if validator and not validator(r):
                    raise RuntimeError("Invalid")
                return r
            except Exception as e:
                logj("api_err", where=desc or fn.__name__, attempt=attempt, err=str(e))
                if attempt < self.MAX_RETRY:
                    time.sleep(min(delay, self.BACKOFF_MAX)*random.uniform(0.8,1.2))
                    delay = min(delay*2, self.BACKOFF_MAX)
                    continue
                if soft:
                    return None
                raise

    def get_balance_krw(self):
        def _ok(x):
            try: Decimal(str(x)); return True
            except: return False
        b = self._safe(self.u.get_balance, "KRW", soft=True, desc="get_balance", validator=_ok)
        return Decimal(str(b or "0"))

    def get_coin_free(self):
        bals = self._safe(self.u.get_balances, soft=True) or []
        coin = SYMBOL.split("-")[1].upper()
        for b in bals:
            if b.get("currency","").upper() == coin:
                return Decimal(str(b.get("balance") or "0"))
        return Decimal("0")

    def buy_market(self, krw):
        return self._safe(self.u.buy_market_order, SYMBOL, float(krw), soft=True, desc="buy_market")

    def sell_market(self, vol):
        return self._safe(self.u.sell_market_order, SYMBOL, float(vol), soft=True, desc="sell_market")

# ===== 메인봇 =====
class BreakoutBot:
    """
    재진입 금지 버전
    - 매도한 그 3분봉에서는 재진입 금지
    """
    def __init__(self):
        self.api = UpbitWrap()
        self._stop = False
        self.lock = threading.RLock()

        self.cur_candle = None
        self.last_price = None

        # 진입 트리거
        self.watch_target_close = None
        self.breakout_hold_since = None

        # 포지션 상태
        self.in_position = False
        self.entry_price = None
        self.tp = None
        self.sl = None

        # 재진입 금지 (해당 봉 start_ts)
        self.no_entry_until = None

    # ===== 3분봉 처리 =====
    @staticmethod
    def _bucket_start(ts):
        mm = (ts.minute // CANDLE_MINUTES) * CANDLE_MINUTES
        return ts.replace(second=0, microsecond=0, minute=mm)

    def _roll_candle_if_needed(self, t):
        start = self._bucket_start(t)
        end = start + timedelta(minutes=CANDLE_MINUTES)

        if self.cur_candle is None:
            self.cur_candle = {
                "start": start, "end": end,
                "open": None, "high": None, "low": None, "close": None,
                "turnover": Decimal("0")
            }
            return

        if self.cur_candle["start"] == start:
            self.cur_candle["end"] = end
            return

        # 구봉 마감
        self._finalize_candle()

        # 새 봉
        self.cur_candle = {
            "start": start, "end": end,
            "open": None, "high": None, "low": None, "close": None,
            "turnover": Decimal("0")
        }

    def _apply_trade(self, price, vol, t):
        self._roll_candle_if_needed(t)
        c = self.cur_candle
        if c["open"] is None:
            c["open"] = c["high"] = c["low"] = price
        c["close"] = price
        if price > c["high"]: c["high"] = price
        if price < c["low"]:  c["low"]  = price
        c["turnover"] += (price * vol)

    def _finalize_candle(self):
        c = self.cur_candle
        if c["open"] is None:
            return

        logj("candle_close",
             start=c["start"].strftime("%Y-%m-%d %H:%M:%S"),
             o=str(c["open"]), h=str(c["high"]),
             l=str(c["low"]), c=str(c["close"]),
             turnover=str(c["turnover"]))

        # 재진입 금지 봉이면 skip
        if self.no_entry_until == c["start"]:
            logj("skip_reentry_candle", start=c["start"].strftime("%H:%M"))
            return

        # 조건 만족할 때 watch 세팅
        if c["turnover"] >= TURNOVER_THRESH and c["open"] < c["close"]:
            self.watch_target_close = c["close"]
            self.breakout_hold_since = None
            logj("watch_set", close=str(c["close"]), turnover=str(c["turnover"]))

    # ===== 진입 =====
    def _enter(self):
        if self.in_position:
            return
        if not self.watch_target_close:
            return

        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT:
            logj("krw_short", have=str(krw))
            return

        r = self.api.buy_market(BUY_KRW_AMOUNT)
        if not r or not r.get("uuid"):
            logj("buy_fail", resp=str(r))
            return

        self.in_position = True
        self.entry_price = self.last_price

        self.tp = (self.entry_price * (Decimal("1") + TP_PCT)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        self.sl = (self.entry_price * (Decimal("1") - SL_PCT)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

        logj("buy_done",
             entry=str(self.entry_price),
             tp=str(self.tp),
             sl=str(self.sl))

    # ===== 청산 =====
    def _exit_all(self):
        free = self.api.get_coin_free()
        if free > 0:
            r = self.api.sell_market(free)
            if r and r.get("uuid"):
                logj("sell_all", vol=str(free), price=str(self.last_price))
        self.in_position = False
        self.entry_price = None
        self.tp = None
        self.sl = None

        # 재진입 금지 설정
        if self.cur_candle:
            self.no_entry_until = self.cur_candle["start"]
            logj("no_reentry_set", until=self.no_entry_until.strftime("%H:%M"))

        self.watch_target_close = None
        self.breakout_hold_since = None

    # ===== 틱 처리 =====
    def _on_tick(self, d):
        price = d.get("trade_price")
        vol = d.get("trade_volume")
        ts  = d.get("timestamp")
        if price is None or vol is None or ts is None:
            return

        price = Decimal(str(price))
        vol   = Decimal(str(vol))
        t     = datetime.fromtimestamp(ts/1000, tz=timezone.utc).astimezone()

        with self.lock:
            self.last_price = price
            self._apply_trade(price, vol, t)

            # 진입 대기 상태
            if not self.in_position and self.watch_target_close:
                if price > self.watch_target_close:
                    if self.breakout_hold_since is None:
                        self.breakout_hold_since = time.time()
                    elif (time.time() - self.breakout_hold_since) >= BREAKOUT_HOLD_SEC:
                        logj("breakout_hit", price=str(price))
                        self._enter()
                        self.breakout_hold_since = None
                else:
                    self.breakout_hold_since = None

            # 포지션 보유 중 TP/SL
            if self.in_position:
                if price >= self.tp:
                    logj("tp_hit", price=str(price))
                    self._exit_all()
                elif price <= self.sl:
                    logj("sl_hit", price=str(price))
                    self._exit_all()

    # ===== WS loop =====
    def _ws_loop(self):
        backoff = 1.0
        while not self._stop:
            wm = None
            err = None
            try:
                wm = WebSocketManager("trade", [SYMBOL])
                backoff = 1.0
                while not self._stop:
                    raw = wm.get()
                    if isinstance(raw, dict):
                        self._on_tick(raw)
            except Exception as e:
                err = e
            finally:
                try:
                    if wm: wm.terminate()
                except:
                    pass

            if self._stop:
                break

            if err:
                wait = min(backoff, 20.0)*random.uniform(0.8,1.2)
                logj("ws_error", err=str(err), reconnect_in=wait)
                time.sleep(wait)
                backoff = min(backoff*2, 20.0)

    def start(self):
        logj("start", symbol=SYMBOL, mode="breakout_full", buy_krw=str(BUY_KRW_AMOUNT))
        t = threading.Thread(target=self._ws_loop, daemon=True)
        t.start()
        try:
            while True:
                time.sleep(0.3)
        except KeyboardInterrupt:
            pass
        finally:
            self._stop = True
            t.join(timeout=2.0)
            logj("stop")


if __name__ == "__main__":
    BreakoutBot().start()
