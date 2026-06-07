#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
장세전환 추세추종 봇 (실거래 골격) + 드라이런/자가점검/감사로그

전략(백테스트와 동일): BTC/ETH 균등, USDT 무기한선물, 15분봉, 무레버리지(1x)
 - 레짐: 단기SMA(10일) vs 장기SMA(40일) ± 밴드1.5% → 롱/숏/현금
 - 사이즈: 변동성타게팅(목표 연40%, 14일 실현변동성, 상한 1x)
 - 손절: 진입 후 누적 -10% → 현금. 재진입 없음.
 - 리밸런싱: 목표비중 ±10%(REBAL_BAND) 벗어날 때만. 킬스위치: 고점 대비 -40%.

★ 실행 모드 (맨 위 플래그) ★
 1) DRY_RUN=True            → 모의. 실시간 공개시세로 계산, 주문은 가상 체결.
                              키 불필요·무위험. 1주일 배관/로직 검증용. ★지금 이걸로 시작★
 2) DRY_RUN=False, TESTNET=False → 실거래(진짜 돈). 50만원 단계에서 사용.
 3) (참고) 바이낸스 선물 testnet 은 ccxt 가 현재 sandbox 를 막아 사실상 사용 불가.

★ 로그 ★ regime_bot.log(텍스트) + regime_bot_audit.jsonl(사이클별 JSON 판단/검증 기록)
★ 안전 ★ 실거래 키는 환경변수만(소스/깃 금지). binance_env.sh 는 .gitignore.
"""

import os, time, json, math, logging, signal as _sig
from datetime import datetime, timezone
import numpy as np
import pandas as pd

# ===================== 설정 =====================
DRY_RUN        = True                       # ★ 모의 모드 (지금 이걸로 시작)
TESTNET        = False                      # 선물 testnet 은 ccxt 미지원 → 사용하지 말 것
START_EQUITY   = 10000.0                    # 드라이런 가상 시작 잔고(USDT)
SYMBOLS        = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
TF             = "15m"; BARS_DAY = 96; ANN = BARS_DAY * 365
SD, LD, BAND   = 10, 40, 0.015
VOL_TARGET_ANN = 0.40; VOL_WINDOW_D = 14
STOP           = 0.10; REBAL_BAND = 0.10
FEE, SLIP      = 0.0005, 0.0005            # 드라이런 가상 수수료/슬리피지
KILL_DD        = 0.40; LEVERAGE = 1
WARMUP_BARS    = LD * BARS_DAY + 600
LOOP_BUFFER_S  = 20
STATE_PATH     = "regime_bot_state.json"
LOG_PATH       = "regime_bot.log"
AUDIT_PATH     = "regime_bot_audit.jsonl"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"),
                              logging.StreamHandler()])
log = logging.getLogger("regime_bot")

_RUNNING = True
def _stop(*_):
    global _RUNNING; _RUNNING = False; log.info("종료 신호 - 현재 루프 후 정지")
_sig.signal(_sig.SIGINT, _stop); _sig.signal(_sig.SIGTERM, _stop)


def audit(rec):
    rec["ts"] = datetime.now(timezone.utc).isoformat()
    with open(AUDIT_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ===================== 거래소 =====================
def make_exchange():
    import ccxt
    if DRY_RUN:
        ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})
        ex.load_markets()
        log.info(">>> DRY-RUN (모의: 실시간 시세 + 가상 체결, 키/돈 불필요)")
        return ex
    key = os.environ.get("BINANCE_KEY"); sec = os.environ.get("BINANCE_SECRET")
    if not key or not sec:
        raise SystemExit("환경변수 BINANCE_KEY / BINANCE_SECRET 가 필요합니다.")
    ex = ccxt.binance({"apiKey": key, "secret": sec, "enableRateLimit": True,
                       "options": {"defaultType": "future", "adjustForTimeDifference": True}})
    if TESTNET:
        ex.set_sandbox_mode(True); log.warning(">>> TESTNET (주의: ccxt 선물 sandbox 미지원으로 실패 가능)")
    else:
        log.warning(">>> 실거래 모드 (진짜 돈!) <<<")
    ex.load_markets()
    for s in SYMBOLS:
        try:
            ex.set_leverage(LEVERAGE, s); ex.set_margin_mode("isolated", s)
        except Exception as e:
            log.warning(f"레버리지/마진 설정 경고 {s}: {e}")
    return ex


def fetch_klines(ex, symbol, need=WARMUP_BARS, limit=1500):
    out, since = [], ex.milliseconds() - need * 15 * 60000
    tf_ms = ex.parse_timeframe(TF) * 1000
    now = ex.milliseconds(); last = None
    while since < now:
        b = ex.fetch_ohlcv(symbol, TF, since=since, limit=limit)
        if not b: break
        out += b
        if last is not None and b[-1][0] <= last: break    # 진전 없으면 중단
        last = b[-1][0]; since = last + tf_ms; time.sleep(ex.rateLimit / 1000)
    if not out: return None
    df = pd.DataFrame(out, columns=["ts","open","high","low","close","volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = df.set_index("ts").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df.iloc[:-1].tail(need)


# ===================== 신호 =====================
def signal_for(df):
    close = df["close"]
    short = close.rolling(SD * BARS_DAY).mean().iloc[-1]
    long_ = close.rolling(LD * BARS_DAY).mean().iloc[-1]
    up, dn = long_ * (1 + BAND), long_ * (1 - BAND)
    if   short > up: sgn = 1.0
    elif short < dn: sgn = -1.0
    else:            sgn = 0.0
    ret = close.pct_change().fillna(0)
    rv = ret.rolling(VOL_WINDOW_D * BARS_DAY).std().iloc[-1] * np.sqrt(ANN)
    size = min(1.0, VOL_TARGET_ANN / rv) if (rv and rv > 0) else 0.0
    diag = {"close": float(close.iloc[-1]), "short_sma": float(short),
            "long_sma": float(long_), "band_up": float(up), "band_dn": float(dn),
            "rv": float(rv) if rv == rv else None, "size": float(size)}
    return sgn, float(size), float(close.iloc[-1]), float(ret.iloc[-1]), diag


# ===================== 상태 =====================
def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f: st = json.load(f)
    else:
        st = {"coins": {s: {"tsign": 0.0, "tpnl": 0.0, "stopped": False, "held_sign": 0.0}
                        for s in SYMBOLS}, "equity_peak": 0.0, "halted": False}
    if "sim" not in st:        # 드라이런 가상 계좌
        st["sim"] = {"cash": START_EQUITY, "positions": {s: 0.0 for s in SYMBOLS},
                     "price": {s: 0.0 for s in SYMBOLS}}
    return st

def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


# ===================== 계좌/포지션 (DRY_RUN 분기) =====================
def get_equity(ex, st):
    if DRY_RUN:
        sim = st["sim"]
        return sim["cash"] + sum(sim["positions"][s] * sim["price"].get(s, 0.0) for s in SYMBOLS)
    return float(ex.fetch_balance()["total"].get("USDT", 0.0))

def get_position(ex, st, symbol):
    if DRY_RUN:
        return float(st["sim"]["positions"].get(symbol, 0.0))
    for p in ex.fetch_positions([symbol]):
        amt = float(p.get("contracts") or 0)
        if amt != 0:
            return amt if p.get("side") == "long" else -amt
    return 0.0

def market_limits(ex, symbol):
    m = ex.market(symbol)
    return (m["limits"]["amount"].get("min") or 0.0,
            (m["limits"].get("cost") or {}).get("min") or 0.0)


def place_to(ex, st, symbol, target_qty, cur_qty, price):
    """현재→목표 조정. (action, reason). DRY_RUN 이면 가상 체결."""
    delta = target_qty - cur_qty
    if abs(delta) < 1e-12:
        return "HOLD", "변화없음"
    amt = abs(float(ex.amount_to_precision(symbol, abs(delta))))
    if amt <= 0:
        return "SKIP_MIN", "조정량이 수량 최소단위 미만"
    min_amt, min_cost = market_limits(ex, symbol)
    notional = amt * price
    reducing = abs(target_qty) < abs(cur_qty)
    if not reducing and (amt < min_amt or (min_cost and notional < min_cost)):
        return "SKIP_MIN", f"최소미달 amt={amt}<{min_amt} or notional={notional:.2f}<{min_cost}"
    side = "buy" if delta > 0 else "sell"
    if DRY_RUN:
        # 가상 체결: 현금 = -delta*price (매수 감소/매도·숏 증가), 수수료 차감, 포지션 갱신
        st["sim"]["cash"] -= delta * price + abs(delta) * price * (FEE + SLIP)
        st["sim"]["positions"][symbol] = target_qty
        log.info(f"  [DRY] {symbol} {side} {amt} (≈{notional:.2f} USDT) 가상체결")
        return "TRADE", f"DRY {side} {amt}"
    params = {"reduceOnly": True} if reducing and np.sign(target_qty) == np.sign(cur_qty) else {}
    o = ex.create_order(symbol, "market", side, amt, params=params)
    log.info(f"  주문 {symbol} {side} {amt} (≈{notional:.2f} USDT) id={o.get('id')}")
    return "TRADE", f"{side} {amt}"


def verify_position(ex, st, symbol, target_qty):
    cur = get_position(ex, st, symbol)
    if DRY_RUN:
        return "MATCH", cur
    min_amt, _ = market_limits(ex, symbol)
    tol = max(min_amt, abs(target_qty) * 0.03, 1e-8)
    return ("MATCH" if abs(cur - target_qty) <= tol else "MISMATCH"), cur


# ===================== 자가 점검 =====================
def startup_selftest(ex, st):
    log.info("===== STARTUP SELF-TEST =====")
    ok = True
    try:
        eq = get_equity(ex, st); log.info(f"[SELFTEST] 잔고조회 OK: {eq:.2f} USDT"
                                          + (" (가상)" if DRY_RUN else ""))
    except Exception as e:
        log.error(f"[SELFTEST] 잔고조회 FAIL: {e}"); ok = False
    for s in SYMBOLS:
        try:
            min_amt, min_cost = market_limits(ex, s)
            df = fetch_klines(ex, s)
            sgn, size, price, _, diag = signal_for(df)
            st["sim"]["price"][s] = price
            pos = get_position(ex, st, s)
            regime = {1.0:"롱",-1.0:"숏",0.0:"현금"}[sgn]
            log.info(f"[SELFTEST] {s} OK | 봉수={len(df)} 마지막={df.index[-1]} "
                     f"| 레짐={regime} size={size:.2f} close={price:.2f} "
                     f"| 최소수량={min_amt} 최소금액={min_cost} | 포지션={pos}")
            audit({"event": "selftest", "symbol": s, "ok": True, "regime": regime,
                   "min_amt": min_amt, "min_cost": min_cost, "position": pos, **diag})
        except Exception as e:
            log.error(f"[SELFTEST] {s} FAIL: {e}"); ok = False
            audit({"event": "selftest", "symbol": s, "ok": False, "error": str(e)})
    log.info(f"===== SELF-TEST {'전체 OK' if ok else '실패 항목 있음 - 확인 필요'} =====")
    return ok


# ===================== 1회 사이클 =====================
def cycle(ex, st):
    equity = get_equity(ex, st)
    if equity <= 0:
        log.warning("잔고 0 - 스킵"); return
    st["equity_peak"] = max(st.get("equity_peak", 0.0), equity)
    dd = equity / st["equity_peak"] - 1 if st["equity_peak"] > 0 else 0.0
    log.info(f"[CYCLE]{' DRY' if DRY_RUN else ''} equity={equity:.2f} peak={st['equity_peak']:.2f} "
             f"낙폭={dd*100:+.1f}% (킬스위치 -{KILL_DD*100:.0f}%까지 {(-(KILL_DD)-dd)*100:+.1f}%p)")

    if st["equity_peak"] > 0 and equity <= st["equity_peak"] * (1 - KILL_DD):
        log.error(f"!! 킬스위치 발동 (낙폭 {dd*100:.1f}%) - 전량청산 후 정지")
        for s in SYMBOLS:
            cur = get_position(ex, st, s)
            if cur != 0: place_to(ex, st, s, 0.0, cur, st["sim"]["price"].get(s) or signal_for(fetch_klines(ex, s))[2])
        st["halted"] = True; save_state(st)
        audit({"event": "killswitch", "equity": equity, "peak": st["equity_peak"], "dd": dd})
        return
    if st.get("halted"):
        log.warning("halted - 매매 안 함 (점검 후 state에서 halted=false로)"); return

    alloc = equity / len(SYMBOLS)
    for s in SYMBOLS:
        cs = st["coins"][s]
        df = fetch_klines(ex, s)
        if df is None or len(df) < LD * BARS_DAY:
            log.warning(f"{s} 데이터 부족 - 스킵")
            audit({"event": "cycle", "symbol": s, "action": "SKIP_DATA"}); continue
        sgn, size, price, last_ret, diag = signal_for(df)
        st["sim"]["price"][s] = price
        cur_qty = get_position(ex, st, s)
        cur_frac = (cur_qty * price) / alloc if alloc > 0 else 0.0

        if cs["held_sign"] != 0:
            cs["tpnl"] += cs["held_sign"] * last_ret
            if cs["tpnl"] <= -STOP: cs["stopped"] = True
        if sgn != cs["tsign"]:
            cs["stopped"] = False; cs["tpnl"] = 0.0; cs["tsign"] = sgn
        desired = 0.0 if cs["stopped"] else sgn * size
        regime = {1.0:"롱",-1.0:"숏",0.0:"현금"}[sgn]

        action, reason, verify, cur_after = "HOLD", "밴드내", "NA", cur_qty
        target_qty = cur_qty
        if (np.sign(desired) != np.sign(cur_frac)) or (abs(desired - cur_frac) > REBAL_BAND):
            target_qty = float(ex.amount_to_precision(s, desired * alloc / price))
            action, reason = place_to(ex, st, s, target_qty, cur_qty, price)
            if action == "TRADE":
                verify, cur_after = verify_position(ex, st, s, target_qty)
                cs["held_sign"] = float(np.sign(desired))
                lvl = log.info if verify == "MATCH" else log.error
                lvl(f"{s} 레짐={regime} 비중 {cur_frac:+.2f}→{desired:+.2f} | 체결검증={verify} "
                    f"(목표qty={target_qty} 실제={cur_after})")
            else:
                log.info(f"{s} 레짐={regime} 목표 {desired:+.2f} 이지만 {action}: {reason}")
        else:
            cs["held_sign"] = float(np.sign(cur_frac))
            log.info(f"{s} 레짐={regime} 비중 {cur_frac:+.2f}≈목표{desired:+.2f} - 유지")

        audit({"event": "cycle", "symbol": s, "regime": regime, "stopped": cs["stopped"],
               "tpnl": round(cs["tpnl"], 4), "cur_frac": round(cur_frac, 4),
               "desired_frac": round(desired, 4), "action": action, "reason": reason,
               "target_qty": target_qty, "verify": verify, "qty_after": cur_after, **diag})
    save_state(st)


# ===================== 메인 =====================
def sleep_to_next_bar():
    now = time.time(); period = 15 * 60
    nxt = (math.floor(now / period) + 1) * period + LOOP_BUFFER_S
    while _RUNNING and time.time() < nxt:
        time.sleep(min(5, max(0.1, nxt - time.time())))

def reconcile_startup(ex, st):
    log.info("===== 시작 정합성 점검 =====")
    for s in SYMBOLS:
        cur = get_position(ex, st, s); sign = float(np.sign(cur))
        prev = st["coins"][s]["held_sign"]
        status = "MATCH" if prev == sign else "CORRECTED"
        if status == "CORRECTED":
            st["coins"][s]["held_sign"] = sign
        log.info(f"[정합] {s} 상태={prev} 실제={sign} → {status}")
        audit({"event": "reconcile", "symbol": s, "state_sign": prev,
               "actual_sign": sign, "result": status})
    save_state(st)

def main():
    ex = make_exchange()
    st = load_state()
    startup_selftest(ex, st)
    reconcile_startup(ex, st)
    log.info(f"시작. equity={get_equity(ex, st):.2f} USDT{' (가상)' if DRY_RUN else ''}")
    while _RUNNING:
        try:
            cycle(ex, st)
        except Exception as e:
            log.exception(f"사이클 에러(계속): {e}")
            audit({"event": "error", "error": str(e)})
        if _RUNNING: sleep_to_next_bar()
    log.info("정지됨.")


if __name__ == "__main__":
    main()