#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upbit KRW-XRP Breakout + Trailing Bot (v2.0, WS 기반)
- 3분봉 거래대금 >= 30억 AND 양봉(확정 봉)의 종가 돌파가 3초 유지되면 → 시장가 1회 매수(₩400,000)
- 일봉 상태(양봉/음봉)에 따라 분할익절/트레일링 다르게 적용
  * 양봉: +1%에 50%, +2%에 추가 25%, 트레일링 -1%에 잔여 25% 손절
  * 음봉: +0.6%에 50%, +1%에 추가 25%, 트레일링 -0.8%에 잔여 25% 손절
- 손절 시, 기존 분할 익절 주문이 미체결이면 우선 취소 후 전량 매도
- WS 재연결/토큰버킷/백오프/주문 UUID 추적 포함
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

# 3초 유지 진입
BREAKOUT_HOLD_SEC = 3.0

# 일봉 상태별 파라미터
BULL_TP1_PCT   = Decimal("0.01")   # +1%
BULL_TP2_PCT   = Decimal("0.02")   # +2%
BULL_TRAIL_PCT = Decimal("0.01")   # -1%

BEAR_TP1_PCT   = Decimal("0.006")  # +0.6%
BEAR_TP2_PCT   = Decimal("0.01")   # +1%
BEAR_TRAIL_PCT = Decimal("0.008")  # -0.8%

# 일봉 상태 캐시 주기(초)
DAILY_STATE_TTL = 30.0

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
                # Upbit 응답 balance는 '가용(Free)'
                return Decimal(str(b.get("balance") or "0"))
        return Decimal("0")

    def buy_market_krw(self, krw: Decimal) -> dict|None:
        return self._safe(self.u.buy_market_order, SYMBOL, float(krw),
                          desc=f"buy_market {krw}", soft=True)

    def sell_market(self, volume: Decimal) -> dict|None:
        return self._safe(self.u.sell_market_order, SYMBOL, float(volume),
                          desc=f"sell_market {volume}", soft=True)

    def get_order(self, uuid_: str) -> dict|None:
        def _ok(r): return isinstance(r, dict) and r.get("state") is not None
        return self._safe(self.u.get_order, uuid_, desc=f"get_order {uuid_}", validator=_ok, soft=True)

    def cancel_order(self, uuid_: str) -> dict|None:
        def _ok(r): return isinstance(r, dict) and r.get("uuid")
        return self._safe(self.u.cancel_order, uuid_, desc=f"cancel_order {uuid_}", validator=_ok, soft=True)

# ===== 메인 봇 =====
class BreakoutTrailingBot:
    """
    상태:
      IDLE  -> (3분봉 확정) 거래대금>=30억 & 양봉이면 watch_close 업데이트
      WATCH -> 종가 돌파가 3초 '연속 유지'되면 시장가 1회 매수 -> IN_POSITION
      IN_POSITION:
          - peak 갱신, trailing_stop 상향
          - 일봉(양/음)에 따라 TP1/TP2/Trail 기준 분기
          - 손절 시 TP 주문 취소 후 전량 매도
          - 전량 매도 완료 시 완전 리셋 -> IDLE
    """
    def __init__(self):
        self.api = UpbitWrap()
        self._stop = False

        # 실시간
        self.last_price: Decimal|None = None
        self.lock = threading.RLock()

        # 3분봉 집계
        self.cur_candle = None  # dict(open, high, low, close, turnover, start_ts, end_ts)

        # 신호/진입
        self.watch_target_close: Decimal|None = None
        self.breakout_hold_since: float | None = None  # 돌파 유지 시작 시각 (epoch)

        # 포지션/트레일링
        self.in_position = False
        self.entry_price: Decimal|None = None
        self.entry_time  = None
        self.peak_price:  Decimal|None = None
        self.trailing_stop: Decimal|None = None

        # 분할익절 단계 (0→1→2)
        self.partial_stage = 0
        self.tp1_uuid: str|None = None
        self.tp2_uuid: str|None = None

        # 일봉 상태 캐시
        self._daily_type = "unknown"
        self._daily_checked_at = 0.0

    # === 3분 경계/캔들 ===
    @staticmethod
    def _bucket_start(ts: datetime) -> datetime:
        mm = (ts.minute // CANDLE_MINUTES) * CANDLE_MINUTES
        return ts.replace(second=0, microsecond=0, minute=mm)

    def _roll_candle_if_needed(self, t: datetime):
        start = self._bucket_start(t)
        end = start + timedelta(minutes=CANDLE_MINUTES)

        if self.cur_candle is None:
            self.cur_candle = {
                "open": None, "high": None, "low": None, "close": None,
                "turnover": Decimal("0"),
                "start_ts": start, "end_ts": end
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
            "start_ts": start, "end_ts": end
        }

    def _finalize_candle(self):
        c = self.cur_candle
        if not c or c["open"] is None or c["close"] is None:
            return

        logj("candle_close",
             start=c["start_ts"].strftime("%Y-%m-%d %H:%M:%S"),
             end=c["end_ts"].strftime("%Y-%m-%d %H:%M:%S"),
             o=str(c["open"]), h=str(c["high"]), l=str(c["low"]), c=str(c["close"]),
             turnover=str(c["turnover"]))

        # 조건: 거래대금 >= 30억 AND 양봉(시가 < 종가)
        if c["turnover"] >= TURNOVER_THRESH and c["open"] < c["close"]:
            self.watch_target_close = c["close"]
            # 진입 보류 상태 해제
            self.breakout_hold_since = None
            logj("watch_set", close=str(self.watch_target_close),
                 turnover=str(c["turnover"]), candle=f"{CANDLE_MINUTES}m", bullish=True)

    def _apply_trade_to_candle(self, price: Decimal, volume: Decimal, t: datetime):
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

    # === 일봉 상태 ===
    def _get_daily_type_cached(self) -> str:
        now = time.time()
        if now - self._daily_checked_at < DAILY_STATE_TTL:
            return self._daily_type
        try:
            df = pyupbit.get_ohlcv(SYMBOL, interval="day", count=2)
            today_open = df.iloc[-1]['open']
            current = pyupbit.get_current_price(SYMBOL)
            if current is None or today_open is None:
                self._daily_type = "unknown"
            else:
                self._daily_type = "bull" if current > today_open else "bear"
        except Exception as e:
            logj("err_daily_candle", err=str(e))
            self._daily_type = "unknown"
        self._daily_checked_at = now
        return self._daily_type

    # === 유틸/상태 ===
    def _reset_all(self):
        self.in_position    = False
        self.entry_price    = None
        self.entry_time     = None
        self.peak_price     = None
        self.trailing_stop  = None
        self.watch_target_close = None
        self.partial_stage  = 0
        self.tp1_uuid       = None
        self.tp2_uuid       = None
        self.breakout_hold_since = None
        logj("reset_to_idle")

    # === 주문 ===
    def _ensure_krw(self, need: Decimal) -> bool:
        krw = self.api.get_balance_krw()
        if krw < need:
            logj("krw_short", have=str(krw), need=str(need))
            return False
        return True

    def _enter_once(self):
        if self.in_position:
            return
        if not self._ensure_krw(BUY_KRW_AMOUNT):
            return
        r = self.api.buy_market_krw(BUY_KRW_AMOUNT)
        if not isinstance(r, dict) or not r.get("uuid"):
            logj("buy_fail", resp=str(r))
            return
        # 엔트리/트레일링 초기화
        self.in_position   = True
        self.entry_price   = self.last_price
        self.entry_time    = now_kst_str()
        self.peak_price    = self.entry_price
        # trailing은 일봉 타입에 따라 계산
        dty = self._get_daily_type_cached()
        trail_pct = (BULL_TRAIL_PCT if dty == "bull" else BEAR_TRAIL_PCT if dty == "bear" else Decimal("0.01"))
        self.trailing_stop = (self.peak_price * (Decimal("1") - trail_pct)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        self.partial_stage = 0
        self.tp1_uuid      = None
        self.tp2_uuid      = None
        logj("buy_done", uuid=r.get("uuid"), entry=str(self.entry_price),
             krw=str(BUY_KRW_AMOUNT), trail_stop=str(self.trailing_stop), daily=dty)

    def _place_market_sell(self, ratio: Decimal, tag: str) -> str|None:
        free = self.api.get_coin_free()
        if free <= 0:
            return None
        vol = (free * ratio).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        if vol <= 0:
            return None
        r = self.api.sell_market(vol)
        if isinstance(r, dict) and r.get("uuid"):
            logj(tag, uuid=r.get("uuid"), vol=str(vol))
            return r.get("uuid")
        logj(f"{tag}_fail", resp=str(r))
        return None

    def _cancel_if_open(self, uuid_: str|None, tag: str):
        if not uuid_:
            return
        od = self.api.get_order(uuid_)
        if not isinstance(od, dict):
            return
        state = str(od.get("state","")).lower()
        if state in ("wait","watch"):
            cr = self.api.cancel_order(uuid_)
            if isinstance(cr, dict) and cr.get("uuid"):
                logj(f"{tag}_cancelled", uuid=uuid_, state=state)
            else:
                logj(f"{tag}_cancel_fail", uuid=uuid_, state=state, resp=str(cr))

    def _exit_all(self):
        # 손절 전에 미체결 TP 주문 정리
        self._cancel_if_open(self.tp1_uuid, "tp1")
        self._cancel_if_open(self.tp2_uuid, "tp2")
        free = self.api.get_coin_free()
        if free > 0:
            r = self.api.sell_market(free)
            if isinstance(r, dict) and r.get("uuid"):
                logj("sell_all_done", uuid=r.get("uuid"), vol=str(free), exit_price=str(self.last_price))
            else:
                logj("sell_all_fail", vol=str(free), resp=str(r))
        self._reset_all()

    # === 분할익절/트레일링 ===
    def _update_trailing_and_tp(self, price: Decimal):
        if not self.in_position or self.entry_price is None:
            return

        # 일봉 타입에 따라 TP/Trail 결정
        dty = self._get_daily_type_cached()
        if dty == "bull":
            tp1 = self.entry_price * (Decimal("1") + BULL_TP1_PCT)  # +1%
            tp2 = self.entry_price * (Decimal("1") + BULL_TP2_PCT)  # +2%
            trail_pct = BULL_TRAIL_PCT
        elif dty == "bear":
            tp1 = self.entry_price * (Decimal("1") + BEAR_TP1_PCT)  # +0.6%
            tp2 = self.entry_price * (Decimal("1") + BEAR_TP2_PCT)  # +1%
            trail_pct = BEAR_TRAIL_PCT
        else:
            tp1 = self.entry_price * Decimal("1.006")
            tp2 = self.entry_price * Decimal("1.01")
            trail_pct = Decimal("0.01")

        # 1차 익절: 전체의 50%
        if self.partial_stage == 0 and price >= tp1:
            self.tp1_uuid = self._place_market_sell(Decimal("0.5"), "tp1_sell_50pct")
            if self.tp1_uuid:
                self.partial_stage = 1
                logj("partial_sell_1st", price=str(price), daily=dty)

        # 2차 익절: 전체의 25% (잔여 50%의 절반)
        if self.partial_stage == 1 and price >= tp2:
            self.tp2_uuid = self._place_market_sell(Decimal("0.5"), "tp2_sell_25pct")
            if self.tp2_uuid:
                self.partial_stage = 2
                logj("partial_sell_2nd", price=str(price), daily=dty)

        # 최고가 갱신 → trailing stop 상향
        if self.peak_price is None or price > self.peak_price:
            self.peak_price = price
            self.trailing_stop = (self.peak_price * (Decimal("1") - trail_pct)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
            logj("trail_raise", peak=str(self.peak_price), stop=str(self.trailing_stop), daily=dty)

        # 트레일링 손절(잔여 전량)
        if self.trailing_stop and price <= self.trailing_stop:
            logj("trail_hit", price=str(price), stop=str(self.trailing_stop), daily=dty)
            self._exit_all()

    # === 시세 처리 ===
    def _on_trade_tick(self, d: dict):
        price = d.get("trade_price")
        vol   = d.get("trade_volume")
        ts    = d.get("timestamp")
        if price is None or vol is None or ts is None:
            return
        price = Decimal(str(price))
        vol   = Decimal(str(vol))
        t     = datetime.fromtimestamp(ts/1000, tz=timezone.utc).astimezone()

        with self.lock:
            self.last_price = price
            # 3분봉 집계
            self._apply_trade_to_candle(price, vol, t)

            # (매수 전) 돌파 감시 & 3초 유지 로직
            if (not self.in_position) and (self.watch_target_close is not None):
                if price > self.watch_target_close:
                    if self.breakout_hold_since is None:
                        self.breakout_hold_since = time.time()
                    elif (time.time() - self.breakout_hold_since) >= BREAKOUT_HOLD_SEC:
                        logj("breakout_hit", price=str(price), ref_close=str(self.watch_target_close), hold_sec=BREAKOUT_HOLD_SEC)
                        self._enter_once()
                        # 진입 후 감시 리셋
                        self.breakout_hold_since = None
                else:
                    # 돌파 해제되면 타이머 초기화
                    self.breakout_hold_since = None

            # (매수 후) 분할익절/트레일링
            self._update_trailing_and_tp(price)

    # === WS 루프 ===
    def _ws_loop(self):
        backoff = 1.0
        while not self._stop:
            wm = None; last_err = None
            try:
                wm = WebSocketManager("trade", [SYMBOL])
                backoff = 1.0
                while not self._stop:
                    raw = wm.get()
                    if not isinstance(raw, dict):
                        continue
                    self._on_trade_tick(raw)
            except Exception as e:
                last_err = e
            finally:
                try:
                    if wm is not None: wm.terminate()
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
        logj("start", symbol=SYMBOL, mode="breakout+tp(50%+25%)+trailing",
             candle=f"{CANDLE_MINUTES}m", turnover=str(TURNOVER_THRESH),
             breakout_hold=f"{BREAKOUT_HOLD_SEC}s", buy_krw=str(BUY_KRW_AMOUNT))
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
    BreakoutTrailingBot().start()
