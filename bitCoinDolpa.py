#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-XRP Mixed Scalping + Breakout Bot (실전용 v1)

전략 요약
[공통]
- 코인: KRW-XRP
- 1회 매수금액: 400,000원 (환경변수 BUY_KRW_AMOUNT로 변경 가능)
- 동시 포지션: 1개만

[스캘핑 모드 (Mean Reversion)]
- 기준: 1분봉
- 최근 3분(high/low 기준) 고점 대비 저점 낙폭이 -1.0% 이하
- 그리고 방금 닫힌 1분봉 종가가 최근 저점 대비 +0.15% 이상 반등일 때
  → 시장가 40만 원 매수
- TP: +0.4% (전량)
- SL: -0.5% (전량)
- 최대 보유 시간: 120초

[Breakout 모드 (거래대금 급증 예외)]
- 기준: 1분봉
- 방금 닫힌 1분봉 거래대금 >= 직전 20개 1분봉 거래대금 평균 * 5
- 방금 닫힌 1분봉 종가 >= 직전 10개 1분봉 high 최대 * 1.003 (0.3% 돌파)
  → 시장가 40만 원 매수
- TP: +0.8%
- SL: -0.4%
- 최대 보유 시간: 300초

주의:
- 실전 전 꼭 모의/소액으로 검증하고 사용하세요.
- 키는 환경변수 UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY 사용.
"""

import os
import time
import json
import threading
import random
import uuid
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone, timedelta

import pyupbit
from pyupbit import WebSocketManager

# ===== 환경 설정 =====
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL         = os.getenv("SYMBOL", "KRW-XRP")
BUY_KRW_AMOUNT = Decimal(os.getenv("BUY_KRW_AMOUNT", "400000"))

RUN_ID = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:6]}"

# 스캘핑 파라미터
SCALP_DROP_PCT    = Decimal("0.01")   # -1.0%
SCALP_REBOUND_PCT = Decimal("0.0015") # +0.15%
SCALP_TP_PCT      = Decimal("0.004")  # +0.4%
SCALP_SL_PCT      = Decimal("0.005")  # -0.5%
SCALP_MAX_SEC     = 120               # 2분

# Breakout 파라미터
BRK_VOL_MULT      = Decimal("5")      # 5배 이상 거래대금 폭증
BRK_LOOKBACK_MIN  = 10                # 10분 high 돌파
BRK_TP_PCT        = Decimal("0.008")  # +0.8%
BRK_SL_PCT        = Decimal("0.004")  # -0.4%
BRK_MAX_SEC       = 300               # 5분

# 요청 제한
REQS_PER_SEC = 8
REQS_PER_MIN = 200

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

# ===== Upbit 래퍼 =====
class UpbitWrap:
    def __init__(self):
        if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
            raise SystemExit("UPBIT 키(UPBIT_ACCESS_KEY/UPBIT_SECRET_KEY)가 비어 있습니다.")
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
                msg = str(e)
                logj("api_err", where=desc or fn.__name__, attempt=attempt, err=msg)
                if attempt < self.MAX_RETRY:
                    time.sleep(min(delay, self.BACKOFF_MAX) * random.uniform(0.8,1.2))
                    delay = min(delay*2, self.BACKOFF_MAX)
                    continue
                if soft:
                    return None
                raise

    def get_balance_krw(self) -> Decimal:
        def _ok(x):
            try: Decimal(str(x)); return True
            except: return False
        b = self._safe(self.u.get_balance, "KRW", desc="get_balance(KRW)", validator=_ok, soft=True)
        return Decimal(str(b or "0"))

    def get_coin_free(self) -> Decimal:
        bals = self._safe(self.u.get_balances, desc="get_balances", soft=True) or []
        coin = SYMBOL.split("-")[1].upper()
        for b in bals:
            if str(b.get("currency","")).upper() == coin:
                return Decimal(str(b.get("balance") or "0"))
        return Decimal("0")

    def buy_market_krw(self, krw: Decimal) -> dict|None:
        return self._safe(self.u.buy_market_order, SYMBOL, float(krw),
                          desc=f"buy_market {krw}", soft=True)

    def sell_market(self, volume: Decimal) -> dict|None:
        return self._safe(self.u.sell_market_order, SYMBOL, float(volume),
                          desc=f"sell_market {volume}", soft=True)

# ===== 메인 봇 =====
class MixedXrpBot:
    """
    - 1분봉 실시간 생성(WS trade 기준)
    - 캔들 닫힐 때마다:
        * 스캘핑 진입 여부 체크
        * Breakout 진입 여부 체크
    - 포지션 보유 시에는:
        * 모든 진입 신호 무시
        * 틱마다 TP/SL/시간초과 체크 후 전량 청산
    """
    def __init__(self):
        self.api = UpbitWrap()
        self._stop = False

        # 실시간
        self.last_price: Decimal|None = None

        # 1분봉 캔들 관리
        self.cur_candle = None   # dict(open, high, low, close, value, start_ts)
        self.candles = []        # 최근 N개 1분봉 (파이썬 dict 리스트)

        # 포지션 상태
        self.position = None     # None or dict{mode, entry, tp, sl, qty, entry_ts}

        # 락
        self.lock = threading.RLock()

    # ===== 캔들 관련 =====
    @staticmethod
    def _bucket_start(ts: datetime) -> datetime:
        return ts.replace(second=0, microsecond=0)

    def _on_trade_to_candle(self, price: Decimal, volume: Decimal, t: datetime):
        start = self._bucket_start(t)
        if self.cur_candle is None:
            self.cur_candle = {
                "start": start,
                "open": price, "high": price, "low": price, "close": price,
                "value": price * volume,
            }
            return

        # 같은 1분 안이면 업데이트
        if self.cur_candle["start"] == start:
            c = self.cur_candle
            c["close"] = price
            if price > c["high"]:
                c["high"] = price
            if price < c["low"]:
                c["low"] = price
            c["value"] += price * volume
        else:
            # 이전 캔들 확정
            closed = self.cur_candle
            self.candles.append(closed)
            # 최근 60개까지만 보유(안전빵)
            if len(self.candles) > 60:
                self.candles = self.candles[-60:]

            logj("candle_close",
                 start=closed["start"].strftime("%Y-%m-%d %H:%M:%S"),
                 o=str(closed["open"]), h=str(closed["high"]),
                 l=str(closed["low"]), c=str(closed["close"]),
                 value=str(closed["value"]))

            # 캔들 확정 시 진입 판단
            self._on_candle_close(closed)

            # 새 캔들 시작
            self.cur_candle = {
                "start": start,
                "open": price, "high": price, "low": price,
                "close": price, "value": price * volume,
            }

    # ===== 진입 로직 =====
    def _can_open_new_position(self) -> bool:
        return self.position is None

    def _check_scalping_entry(self) -> bool:
        # 최근 3개 1분봉이 있어야 함
        if len(self.candles) < 3:
            return False
        window = self.candles[-3:]
        recent_high = max(c["high"] for c in window)
        recent_low  = min(c["low"] for c in window)
        if recent_high <= 0 or recent_low <= 0:
            return False

        drop_pct = (recent_low / recent_high) - Decimal("1")
        if drop_pct > -SCALP_DROP_PCT:  # -1% 이상 안 빠졌으면 X
            return False

        # 방금 닫힌 캔들 종가가 recent_low 대비 +0.15% 이상 반등
        last_close = Decimal(str(self.candles[-1]["close"]))
        rebound_pct = (last_close / recent_low) - Decimal("1")
        if rebound_pct >= SCALP_REBOUND_PCT:
            logj("scalp_signal",
                 recent_high=str(recent_high),
                 recent_low=str(recent_low),
                 drop=str(drop_pct),
                 rebound=str(rebound_pct),
                 close=str(last_close))
            return True
        return False

    def _check_breakout_entry(self) -> bool:
        # 최소 20+10개는 있어야 지표 계산
        if len(self.candles) < 25:
            return False

        cur = self.candles[-1]
        cur_close = Decimal(str(cur["close"]))
        cur_value = Decimal(str(cur["value"]))

        # 거래대금 평균
        prev_vals = [Decimal(str(c["value"])) for c in self.candles[-21:-1]]  # 직전 20개
        avg_val = sum(prev_vals) / Decimal(str(len(prev_vals)))
        if avg_val <= 0:
            return False
        if cur_value < avg_val * BRK_VOL_MULT:
            return False

        # 10분 high 돌파
        prev_10 = self.candles[-11:-1]
        recent_high10 = max(c["high"] for c in prev_10)
        if recent_high10 <= 0:
            return False

        if cur_close >= recent_high10 * Decimal("1.003"):
            logj("break_signal",
                 cur_close=str(cur_close),
                 cur_value=str(cur_value),
                 avg_val=str(avg_val),
                 recent_high10=str(recent_high10))
            return True
        return False

    def _enter_position(self, mode: str, price: Decimal):
        # mode: "scalp" or "break"
        if not self._can_open_new_position():
            return

        krw = self.api.get_balance_krw()
        if krw < BUY_KRW_AMOUNT:
            logj("krw_short", have=str(krw), need=str(BUY_KRW_AMOUNT))
            return

        r = self.api.buy_market_krw(BUY_KRW_AMOUNT)
        if not isinstance(r, dict):
            logj("buy_fail", mode=mode, resp=str(r))
            return

        entry_price = price
        qty = BUY_KRW_AMOUNT / entry_price

        if mode == "scalp":
            tp = entry_price * (Decimal("1") + SCALP_TP_PCT)
            sl = entry_price * (Decimal("1") - SCALP_SL_PCT)
            max_sec = SCALP_MAX_SEC
        else:
            tp = entry_price * (Decimal("1") + BRK_TP_PCT)
            sl = entry_price * (Decimal("1") - BRK_SL_PCT)
            max_sec = BRK_MAX_SEC

        self.position = {
            "mode": mode,
            "entry": entry_price,
            "tp": tp,
            "sl": sl,
            "qty": qty,
            "entry_ts": time.time(),
            "max_sec": max_sec,
        }
        logj("enter",
             mode=mode,
             entry=str(entry_price),
             tp=str(tp),
             sl=str(sl),
             qty=str(qty),
             max_sec=max_sec)

    def _on_candle_close(self, closed_candle: dict):
        # 포지션이 없을 때만 진입 신호 사용
        if not self._can_open_new_position():
            return

        # 1) 스캘핑 진입
        if self._check_scalping_entry():
            self._enter_position("scalp", Decimal(str(closed_candle["close"])))
            return

        # 2) Breakout 진입
        if self._check_breakout_entry():
            self._enter_position("break", Decimal(str(closed_candle["close"])))
            return

    # ===== 포지션 관리 (틱마다 TP/SL/시간 체크) =====
    def _check_position_exit(self, price: Decimal):
        if self.position is None:
            return
        pos = self.position
        entry = pos["entry"]
        tp = pos["tp"]
        sl = pos["sl"]
        qty = pos["qty"]
        mode = pos["mode"]
        max_sec = pos["max_sec"]
        elapsed = time.time() - pos["entry_ts"]

        reason = None
        exit_price = price

        if price >= tp:
            reason = "TP"
        elif price <= sl:
            reason = "SL"
        elif elapsed >= max_sec:
            reason = "TIME"

        if not reason:
            return

        free = self.api.get_coin_free()
        if free <= 0:
            logj("exit_no_free", mode=mode, reason=reason, price=str(price))
            self.position = None
            return

        # 안전하게: 보유수량과 free 중 최소치 사용
        sell_qty = min(free, qty)
        r = self.api.sell_market(sell_qty)
        pnl = (exit_price - entry) * sell_qty

        if isinstance(r, dict):
            logj("exit",
                 mode=mode,
                 reason=reason,
                 entry=str(entry),
                 exit=str(exit_price),
                 qty=str(sell_qty),
                 pnl=str(pnl))
        else:
            logj("exit_fail", mode=mode, reason=reason, resp=str(r))

        self.position = None

    # ===== WS 루프 =====
    def _ws_loop(self):
        backoff = 1.0
        while not self._stop:
            wm = None
            last_err = None
            try:
                wm = WebSocketManager("trade", [SYMBOL])
                backoff = 1.0
                while not self._stop:
                    raw = wm.get()
                    if not isinstance(raw, dict):
                        continue
                    price = raw.get("trade_price")
                    volume = raw.get("trade_volume")
                    ts = raw.get("timestamp")
                    if price is None or volume is None or ts is None:
                        continue

                    price_d = Decimal(str(price))
                    volume_d = Decimal(str(volume))
                    t = datetime.fromtimestamp(ts/1000, tz=timezone.utc).astimezone()

                    with self.lock:
                        self.last_price = price_d
                        # 1분봉 갱신/확정
                        self._on_trade_to_candle(price_d, volume_d, t)
                        # 포지션 있으면 TP/SL/시간초과 체크
                        self._check_position_exit(price_d)

            except Exception as e:
                last_err = e
            finally:
                try:
                    if wm is not None:
                        wm.terminate()
                except Exception:
                    pass

            if self._stop:
                break
            if last_err:
                sleep_s = min(backoff, 20.0) * random.uniform(0.8, 1.2)
                logj("ws_error", err=str(last_err), reconnect_in=sleep_s)
                time.sleep(sleep_s)
                backoff = min(backoff*2, 20.0)

    def start(self):
        logj("start",
             symbol=SYMBOL,
             mode="mixed_scalp+breakout",
             buy_krw=str(BUY_KRW_AMOUNT),
             scalp_drop=str(SCALP_DROP_PCT),
             scalp_tp=str(SCALP_TP_PCT),
             scalp_sl=str(SCALP_SL_PCT),
             brk_vol_mult=str(BRK_VOL_MULT),
             brk_tp=str(BRK_TP_PCT),
             brk_sl=str(BRK_SL_PCT))

        t = threading.Thread(target=self._ws_loop, daemon=True)
        t.start()
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            logj("interrupted")
        finally:
            self._stop = True
            try:
                t.join(timeout=2.0)
            except Exception:
                pass
            logj("stop")

if __name__ == "__main__":
    MixedXrpBot().start()
