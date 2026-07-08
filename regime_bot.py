#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
장세전환 추세추종 봇 — 실거래 버전 (보강 1·3·4 + 패치 5·6 반영)

전략(불변): BTC/ETH 균등, USDT 무기한선물, 15분봉, 무레버리지(1x)
 - 레짐: 단기SMA(10일=960봉) vs 장기SMA(40일=3840봉) ±1.5% → 롱/숏/현금
 - 사이즈: 변동성타게팅(목표 연40%, 14일 실현변동성, 상한 1x)
 - 손절: 진입가 대비 -10% → 현금(다음 전환까지). 재진입 없음.
 - 리밸런싱: 목표비중 ±10% 벗어날 때만. 킬스위치: 평가액 고점 대비 -40%.

★ 보강 (실거래 안전) ★
 (1) 킬스위치 평가액 = 미실현손익 포함(marginBalance). 시작 시 wallet/margin 로그로 검증.
 (3) 손절 = "진입가 대비 현재가"로 매 사이클 재계산(누적 아님 → 다운타임 드리프트 없음).
 (4) 코인별 즉시 상태저장(중간 크래시 일관성).

★ 패치 (5) — 코드리뷰 반영 ★
 (5a) 킬스위치 경로 강화: halted 선(先)기록 → 청산 시도, 심볼별 예외 격리,
      가격은 fetch_ticker 사용. halted 상태에서도 잔여 포지션은 매 사이클 청산 재시도.
 (5b) reduceOnly 조건 수정: 전량청산(target=0)에도 reduceOnly 적용. 반전 주문은 제외.
 (5d) 데이터 신선도 가드: 마지막 완성봉이 45분 이상 낡으면 해당 코인 매매 스킵.
 (5e) 코인별 try/except 격리. (5f) create_order 실패 시 크래시 대신 FAIL 기록.

★ 패치 (6) — 로그 개선 ★
 (6a) 체결 로그에 매수/매도/청산 구분 + 청산·축소 시 실현손익(USDT, %) 표기.
 (6b) 코인별 [포지션] 한 줄 요약: 방향/수량/진입가/현재가/평가손익.
 (6c) [CYCLE] 라인에 누적손익(시작 대비 ± USDT, %) 추가.
      ※ equity_start 는 이 버전 첫 실행 시점 평가액으로 기록됨.
        실제 투입원금 기준으로 보려면 state 파일의 equity_start 를 수동 수정.
 (6d) 로그 용량 상한: regime_bot.log 5MB×3개 회전, audit 20MB 초과 시 .old 로 교체.
      → 디스크/메모리 무한 증식 방지.

★ 모드 ★
 - DRY_RUN=True  : 모의(공개시세+가상체결, 키 불필요). ← 기본값(안전)
 - DRY_RUN=False : 실거래(진짜 돈). 실계정 선물키 필요. systemd 권장.
 ※ 실거래 전환 시 반드시 state 파일을 새로 시작할 것(드라이런 고점 잔재 → 즉시 킬스위치 방지).
 ※ 입출금 시: 봇 정지 → state의 equity_peak/equity_start 수동 조정 → 재시작.
"""

import os, time, json, math, logging, signal as _sig
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
import numpy as np
import pandas as pd

# ===================== 설정 =====================
DRY_RUN        = os.environ.get("DRY_RUN", "true").strip().lower() not in ("false", "0", "no")  # 기본 True(안전). 실거래: binance_env.sh 에 export DRY_RUN=false
SYMBOLS        = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
TF             = "15m"; BARS_DAY = 96; ANN = BARS_DAY * 365
SD, LD, BAND   = 10, 40, 0.015
VOL_TARGET_ANN = 0.40; VOL_WINDOW_D = 14
STOP           = 0.10; REBAL_BAND = 0.10
FEE, SLIP      = 0.0005, 0.0005
KILL_DD        = 0.40; LEVERAGE = 1
START_EQUITY   = 10000.0                    # 드라이런 가상 시작 잔고
WARMUP_BARS    = LD * BARS_DAY + 600
LOOP_BUFFER_S  = 20
STALE_MIN      = 45                          # (5d) 마지막 봉이 이보다 낡으면 스킵
LOG_MAX_BYTES  = 5_000_000                   # (6d) 로그 파일 1개 최대 5MB
LOG_BACKUPS    = 3                           # (6d) 회전 보관 개수
AUDIT_MAX_BYTES= 20_000_000                  # (6d) audit 20MB 초과 시 .old 교체
STATE_PATH     = "regime_bot_state.json"
LOG_PATH       = "regime_bot.log"
AUDIT_PATH     = "regime_bot_audit.jsonl"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[RotatingFileHandler(LOG_PATH, maxBytes=LOG_MAX_BYTES,
                                                  backupCount=LOG_BACKUPS, encoding="utf-8"),
                              logging.StreamHandler()])
log = logging.getLogger("regime_bot")

_RUNNING = True
def _stop(*_):
    global _RUNNING; _RUNNING = False; log.info("종료 신호 - 현재 루프 후 정지")
_sig.signal(_sig.SIGINT, _stop); _sig.signal(_sig.SIGTERM, _stop)


def audit(rec):
    try:                                     # (6d) audit 무한 증식 방지
        if os.path.exists(AUDIT_PATH) and os.path.getsize(AUDIT_PATH) > AUDIT_MAX_BYTES:
            os.replace(AUDIT_PATH, AUDIT_PATH + ".old")
    except Exception:
        pass
    rec["ts"] = datetime.now(timezone.utc).isoformat()
    with open(AUDIT_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ===================== 거래소 =====================
def make_exchange():
    import ccxt
    if DRY_RUN:
        ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})
        ex.load_markets(); log.info(">>> DRY-RUN (모의: 실시간 시세 + 가상 체결)")
        return ex
    key = os.environ.get("BINANCE_KEY"); sec = os.environ.get("BINANCE_SECRET")
    if not key or not sec:
        raise SystemExit("환경변수 BINANCE_KEY / BINANCE_SECRET 가 필요합니다.")
    ex = ccxt.binance({"apiKey": key, "secret": sec, "enableRateLimit": True,
                       "options": {"defaultType": "future", "adjustForTimeDifference": True}})
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
        if last is not None and b[-1][0] <= last: break
        last = b[-1][0]; since = last + tf_ms; time.sleep(ex.rateLimit / 1000)
    if not out: return None
    df = pd.DataFrame(out, columns=["ts","open","high","low","close","volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = df.set_index("ts").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df.iloc[:-1].tail(need)


def get_price(ex, st, symbol):
    """(5a) 청산 등 '가격 하나'만 필요할 때: 4천봉 페이지네이션 대신 티커 1회.
    드라이런: 직전 사이클 저장가 우선, 없으면 티커(공개 API)."""
    if DRY_RUN:
        p = st.get("sim", {}).get("price", {}).get(symbol)
        if p:
            return float(p)
    t = ex.fetch_ticker(symbol)
    return float(t.get("last") or t.get("close"))


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
    return sgn, float(size), float(close.iloc[-1]), diag


# ===================== 상태 =====================
def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f: st = json.load(f)
    else:
        st = {"coins": {s: {"tsign": 0.0, "stopped": False, "held_sign": 0.0}
                        for s in SYMBOLS}, "equity_peak": 0.0, "halted": False}
    for s in SYMBOLS:                          # 누락 키 보정
        st["coins"].setdefault(s, {"tsign": 0.0, "stopped": False, "held_sign": 0.0})
        st["coins"][s].setdefault("stopped", False)
        st["coins"][s].setdefault("tsign", 0.0)
        st["coins"][s].setdefault("held_sign", 0.0)
    st.setdefault("equity_peak", 0.0)
    st.setdefault("halted", False)
    if DRY_RUN:                                # sim 하위키까지 모두 보정(옛/불완전 상태 호환)
        st.setdefault("sim", {})
        st["sim"].setdefault("cash", START_EQUITY)
        for k in ("positions", "entry", "price"):
            st["sim"].setdefault(k, {})
            for s in SYMBOLS:
                st["sim"][k].setdefault(s, 0.0)
    return st

def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


# ===================== 계좌/포지션 =====================
def get_equity(ex, st):
    """(1) 미실현손익 포함 평가액."""
    if DRY_RUN:
        sim = st["sim"]
        return sim["cash"] + sum(sim["positions"][s] * sim["price"].get(s, 0.0) for s in SYMBOLS)
    bal = ex.fetch_balance(); info = bal.get("info", {}) or {}
    for k in ("totalMarginBalance", "marginBalance"):      # 미실현 포함 우선
        if info.get(k) is not None:
            return float(info[k])
    wallet = float(bal["total"].get("USDT", 0.0))           # fallback: 지갑+미실현 합산
    upnl = 0.0
    try:
        for p in ex.fetch_positions(SYMBOLS):
            upnl += float(p.get("unrealizedPnl") or 0.0)
    except Exception:
        pass
    return wallet + upnl

def get_position(ex, st, symbol):
    """(부호있는 수량, 평균 진입가) 반환. (3) 손절용 진입가 포함."""
    if DRY_RUN:
        q = float(st["sim"]["positions"].get(symbol, 0.0))
        e = float(st["sim"]["entry"].get(symbol, 0.0))
        return q, e
    for p in ex.fetch_positions([symbol]):
        amt = float(p.get("contracts") or 0)
        if amt != 0:
            qty = amt if p.get("side") == "long" else -amt
            return qty, float(p.get("entryPrice") or 0.0)
    return 0.0, 0.0

def market_limits(ex, symbol):
    m = ex.market(symbol)
    return (m["limits"]["amount"].get("min") or 0.0,
            (m["limits"].get("cost") or {}).get("min") or 0.0)


def place_to(ex, st, symbol, target_qty, cur_qty, price, entry=0.0):
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

    # (6a) 실현손익 계산: 청산/축소/반전 시 닫히는 수량 × (현재가-진입가) 기준(근사: 신호가 사용)
    realized, realized_pct = None, None
    if cur_qty != 0 and entry > 0:
        if target_qty == 0 or np.sign(target_qty) != np.sign(cur_qty):
            closed = abs(cur_qty)
        elif reducing:
            closed = abs(cur_qty) - abs(target_qty)
        else:
            closed = 0.0
        if closed > 0:
            realized = (price - entry) * closed * np.sign(cur_qty)
            realized_pct = np.sign(cur_qty) * (price / entry - 1.0) * 100
    tag = ("청산" if target_qty == 0 else
           "반전" if (cur_qty != 0 and np.sign(target_qty) != np.sign(cur_qty)) else
           "축소" if reducing else
           ("매수" if side == "buy" else "매도"))
    pnl_txt = f" | 실현손익 {realized:+.2f} USDT ({realized_pct:+.2f}%)" if realized is not None else ""

    if DRY_RUN:
        # 가상 체결 + (3) 드라이런 진입가(가중평균) 갱신
        old_q, old_e = cur_qty, st["sim"]["entry"].get(symbol, 0.0)
        if target_qty == 0:                                  new_e = 0.0
        elif old_q == 0 or np.sign(target_qty) != np.sign(old_q): new_e = price
        elif abs(target_qty) > abs(old_q):                   # 같은 방향 추가
            new_e = (abs(old_q)*old_e + (abs(target_qty)-abs(old_q))*price) / abs(target_qty)
        else:                                                new_e = old_e   # 축소
        st["sim"]["entry"][symbol] = new_e
        st["sim"]["cash"] -= delta * price + abs(delta) * price * (FEE + SLIP)
        st["sim"]["positions"][symbol] = target_qty
        log.info(f"  [체결·DRY] {symbol} {tag} {side} {amt} (≈{notional:.2f} USDT){pnl_txt}")
        return "TRADE", f"DRY {side} {amt}"
    # (5b) reduceOnly: 전량청산(target=0) 포함, 같은 방향 축소 포함. 반전(부호 뒤집힘)만 제외.
    #      → 봇 인식과 실제 포지션이 어긋나도 청산 주문이 반대 포지션을 열 수 없음.
    params = {"reduceOnly": True} if reducing and (target_qty == 0 or np.sign(target_qty) == np.sign(cur_qty)) else {}
    try:                                                     # (5f) 주문 실패 = 크래시 아님
        o = ex.create_order(symbol, "market", side, amt, params=params)
    except Exception as e:
        log.error(f"  주문 실패 {symbol} {side} {amt}: {e}")
        audit({"event": "order_fail", "symbol": symbol, "side": side,
               "amt": amt, "reduceOnly": bool(params), "error": str(e)})
        return "FAIL", f"주문 실패: {e}"
    log.info(f"  [체결] {symbol} {tag} {side} {amt} (≈{notional:.2f} USDT) id={o.get('id')}{pnl_txt}")
    return "TRADE", f"{side} {amt}"


def verify_position(ex, st, symbol, target_qty):
    cur, _ = get_position(ex, st, symbol)
    if DRY_RUN:
        return "MATCH", cur
    min_amt, _ = market_limits(ex, symbol)
    tol = max(min_amt, abs(target_qty) * 0.03, 1e-8)
    return ("MATCH" if abs(cur - target_qty) <= tol else "MISMATCH"), cur


def log_position(symbol, qty, entry, price):
    """(6b) 코인별 한 줄 포지션 요약."""
    if qty == 0:
        log.info(f"[포지션] {symbol} 현금 (보유 없음)")
        return
    upnl = qty * (price - entry) if entry > 0 else 0.0
    upct = np.sign(qty) * (price / entry - 1.0) * 100 if entry > 0 else 0.0
    side_k = "롱" if qty > 0 else "숏"
    log.info(f"[포지션] {symbol} {side_k} {abs(qty)} | 진입 {entry:.2f} → 현재 {price:.2f} "
             f"| 평가손익 {upnl:+.2f} USDT ({upct:+.2f}%)")


# ===================== 자가 점검 =====================
def startup_selftest(ex, st):
    log.info("===== STARTUP SELF-TEST =====")
    ok = True
    # (1) 평가액 검증 로그 (실거래)
    if not DRY_RUN:
        try:
            bal = ex.fetch_balance(); info = bal.get("info", {}) or {}
            log.info(f"[잔고검증] wallet={info.get('totalWalletBalance')} "
                     f"margin={info.get('totalMarginBalance')} "
                     f"미실현={info.get('totalUnrealizedProfit')} → 킬스위치는 margin 사용")
        except Exception as e:
            log.warning(f"[잔고검증] 실패: {e}")
    try:
        eq = get_equity(ex, st); log.info(f"[SELFTEST] 평가액 OK: {eq:.2f} USDT"
                                          + (" (가상)" if DRY_RUN else ""))
    except Exception as e:
        log.error(f"[SELFTEST] 평가액 조회 FAIL: {e}"); ok = False
    for s in SYMBOLS:
        try:
            min_amt, min_cost = market_limits(ex, s)
            df = fetch_klines(ex, s)
            sgn, size, price, diag = signal_for(df)
            if DRY_RUN: st["sim"]["price"][s] = price
            qty, entry = get_position(ex, st, s)
            regime = {1.0:"롱",-1.0:"숏",0.0:"현금"}[sgn]
            log.info(f"[SELFTEST] {s} OK | 봉수={len(df)} 마지막={df.index[-1]} "
                     f"| 레짐={regime} size={size:.2f} close={price:.2f} "
                     f"| 최소수량={min_amt} 최소금액={min_cost} | 포지션={qty} 진입가={entry:.2f}")
            audit({"event":"selftest","symbol":s,"ok":True,"regime":regime,
                   "min_amt":min_amt,"min_cost":min_cost,"position":qty,"entry":entry,**diag})
        except Exception as e:
            log.error(f"[SELFTEST] {s} FAIL: {e}"); ok = False
            audit({"event":"selftest","symbol":s,"ok":False,"error":str(e)})
    log.info(f"===== SELF-TEST {'전체 OK' if ok else '실패 항목 있음 - 확인 필요'} =====")
    return ok


# ===================== 킬스위치/청산 =====================
def flatten_all(ex, st, why):
    """(5a) 전 코인 청산. 심볼별 예외 격리 + 티커 가격. 실패해도 다른 코인은 계속."""
    all_flat = True
    for s in SYMBOLS:
        try:
            cur, entry = get_position(ex, st, s)
            if cur == 0:
                continue
            px = get_price(ex, st, s)
            action, reason = place_to(ex, st, s, 0.0, cur, px, entry)
            if action not in ("TRADE", "HOLD"):
                all_flat = False
                log.error(f"[{why}] {s} 청산 미완: {action} {reason}")
            else:
                log.info(f"[{why}] {s} 청산 주문 완료")
        except Exception as e:
            all_flat = False
            log.error(f"[{why}] {s} 청산 실패(다음 사이클 재시도): {e}")
            audit({"event": "flatten_fail", "why": why, "symbol": s, "error": str(e)})
    return all_flat


# ===================== 1회 사이클 =====================
def cycle(ex, st):
    equity = get_equity(ex, st)
    if equity <= 0:
        log.warning("평가액 0 - 스킵"); return
    st.setdefault("equity_start", equity)        # (6c) 최초 실행 시점 기준값
    st["equity_peak"] = max(st.get("equity_peak", 0.0), equity)
    dd = equity / st["equity_peak"] - 1 if st["equity_peak"] > 0 else 0.0
    tot = equity - st["equity_start"]
    tot_pct = tot / st["equity_start"] * 100 if st["equity_start"] > 0 else 0.0
    log.info(f"[CYCLE]{' DRY' if DRY_RUN else ''} equity={equity:.2f} "
             f"| 누적 {tot:+.2f} USDT ({tot_pct:+.1f}%) "
             f"| peak={st['equity_peak']:.2f} 낙폭={dd*100:+.1f}% "
             f"(킬스위치 -{KILL_DD*100:.0f}%까지 {(-(KILL_DD)-dd)*100:+.1f}%p)")

    if st["equity_peak"] > 0 and equity <= st["equity_peak"] * (1 - KILL_DD):
        log.error(f"!! 킬스위치 발동 (낙폭 {dd*100:.1f}%) - 전량청산 후 정지")
        st["halted"] = True; save_state(st)      # (5a) 청산 시도 '전에' 먼저 기록(크래시 나도 halted 유지)
        audit({"event":"killswitch","equity":equity,"peak":st["equity_peak"],"dd":dd})
        ok = flatten_all(ex, st, "killswitch")
        if not ok:
            log.error("킬스위치 청산 일부 실패 - halted 상태에서 매 사이클 재시도. 수동 확인 요망.")
        save_state(st)
        return
    if st.get("halted"):
        # (5a) halted여도 잔여 포지션은 계속 청산 시도(신규 진입만 금지) → 청산 실패 자가치유
        flatten_all(ex, st, "halted")
        log.warning("halted - 매매 안 함 (점검 후 state에서 halted=false로)"); return

    alloc = equity / len(SYMBOLS)
    for s in SYMBOLS:
        try:                                     # (5e) 코인별 격리: BTC 에러가 ETH를 막지 않음
            cs = st["coins"][s]
            df = fetch_klines(ex, s)
            if df is None or len(df) < LD * BARS_DAY:
                log.warning(f"{s} 데이터 부족 - 스킵")
                audit({"event":"cycle","symbol":s,"action":"SKIP_DATA"}); continue
            # (5d) 신선도 가드: 마지막 완성봉이 너무 낡으면 낡은 신호로 매매하지 않음
            age_min = (pd.Timestamp.now(tz="UTC") - df.index[-1]).total_seconds() / 60
            if age_min > STALE_MIN:
                log.warning(f"{s} 데이터 낡음(마지막봉 {age_min:.0f}분 전) - 스킵")
                audit({"event":"cycle","symbol":s,"action":"SKIP_STALE","age_min":round(age_min,1)})
                continue
            sgn, size, price, diag = signal_for(df)
            if DRY_RUN: st["sim"]["price"][s] = price
            cur_qty, entry = get_position(ex, st, s)
            cur_frac = (cur_qty * price) / alloc if alloc > 0 else 0.0

            # (3) 손절: 진입가 대비 현재가 (누적 아님)
            if cur_qty != 0 and entry > 0:
                pnl = np.sign(cur_qty) * (price / entry - 1.0)
                if pnl <= -STOP:
                    cs["stopped"] = True
            if sgn != cs["tsign"]:
                cs["stopped"] = False; cs["tsign"] = sgn
            desired = 0.0 if cs["stopped"] else sgn * size
            regime = {1.0:"롱",-1.0:"숏",0.0:"현금"}[sgn]

            action, reason, verify, cur_after = "HOLD", "밴드내", "NA", cur_qty
            target_qty = cur_qty
            if (np.sign(desired) != np.sign(cur_frac)) or (abs(desired - cur_frac) > REBAL_BAND):
                raw_qty = desired * alloc / price
                min_amt, _ = market_limits(ex, s)
                if abs(raw_qty) < (min_amt or 0.0):     # 목표 0/최소단위 미만 → 전량청산(0). amount_to_precision(0) 회피
                    target_qty = 0.0
                else:                                    # abs 로 정밀도 적용 후 부호 복원(음수 직접 전달 회피)
                    target_qty = math.copysign(float(ex.amount_to_precision(s, abs(raw_qty))), raw_qty)
                action, reason = place_to(ex, st, s, target_qty, cur_qty, price, entry)
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

            # (6b) 포지션 요약 한 줄: 매매 직후엔 최신 상태 재조회, 아니면 기존 값 재사용
            if action == "TRADE":
                qty_now, entry_now = get_position(ex, st, s)
            else:
                qty_now, entry_now = cur_qty, entry
            log_position(s, qty_now, entry_now, price)

            audit({"event":"cycle","symbol":s,"regime":regime,"stopped":cs["stopped"],
                   "entry":round(entry,2),"cur_frac":round(cur_frac,4),
                   "desired_frac":round(desired,4),"action":action,"reason":reason,
                   "target_qty":target_qty,"verify":verify,"qty_after":cur_after,**diag})
            save_state(st)        # (4) 코인별 즉시 저장
        except Exception as e:                   # (5e)
            log.exception(f"{s} 처리 에러(다음 코인 계속): {e}")
            audit({"event":"error","symbol":s,"error":str(e)})
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
        cur, _ = get_position(ex, st, s); sign = float(np.sign(cur))
        prev = st["coins"][s]["held_sign"]
        status = "MATCH" if prev == sign else "CORRECTED"
        if status == "CORRECTED": st["coins"][s]["held_sign"] = sign
        log.info(f"[정합] {s} 상태={prev} 실제={sign} → {status}")
        audit({"event":"reconcile","symbol":s,"state_sign":prev,"actual_sign":sign,"result":status})
    save_state(st)

def main():
    ex = make_exchange()
    st = load_state()
    # (안전) 실거래인데 고점이 현재 평가액보다 비정상적으로 높으면(드라이런 잔재 등) 리셋 → 즉시 킬스위치 방지
    if not DRY_RUN:
        eq0 = get_equity(ex, st)
        if st.get("equity_peak", 0) > eq0 * 1.5:
            log.warning(f"[안전] equity_peak({st['equity_peak']:.2f})가 현재({eq0:.2f})보다 비정상 → 리셋")
            st["equity_peak"] = eq0
        st.pop("sim", None)
    startup_selftest(ex, st)
    reconcile_startup(ex, st)
    log.info(f"시작. equity={get_equity(ex, st):.2f} USDT{' (가상)' if DRY_RUN else ''}")
    while _RUNNING:
        try:
            cycle(ex, st)
        except Exception as e:
            log.exception(f"사이클 에러(계속): {e}")
            audit({"event":"error","error":str(e)})
        if _RUNNING: sleep_to_next_bar()
    log.info("정지됨.")


if __name__ == "__main__":
    main()