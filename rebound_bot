#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[리바운드 전략 드라이런 봇 — 급락 쏠림 역진입(롱만) SOL·XRP]   ※ 실제 주문 코드 없음 (가상 체결만)

원칙: 백테스트(third_v3_backtest.simulate, 롱만 + 손절 상한 10%)를 '미래로 그대로' 돌린다.
      5분봉이 마감될 때마다 백테스트와 똑같은 순서·규칙으로 가상 체결 → 결과를 백테스트와 공정하게 비교 가능.

확정 스펙
  코인    : SOL·XRP 바이낸스 USD-M 무기한선물, 롱만, 원금 반반, 코인별 장부 · 수익 복리 재투입
  배율    : 최근 7일 15분 변동폭 ÷ 0.250%(BTC 2022~24 중앙값), 0.5~4 로 제한
  진입    : 15분 거래량 ≥ 평소(24h) 1.5배 & 15분 수익률 ≤ -0.4%×배율 & 변동성 국면(24h/7일) ≥ 1.5
            → 신호 봉 종가에 지정가 매수, 15분(3봉) 안에 닿으면 체결, 아니면 취소 / 첫 진입 = 한도 × 1/4
  추가    : 마지막 추가가 대비 -0.8%×배율마다 보유의 25% (한도 = 평가액 × LEV)
  익절    : 평단 +0.3%×배율 이상에서 급등 쏠림 → 최대물량 25% 지정가(15분) / 평단 +3%×배율 전량 지정가
  손절    : 평단 -1%×배율 이하에서 급락 쏠림 → 25% 시장가 / 36시간 지나도 평단 아래면 전량 시장가
            전량 손절 = 평단 -min(6%×배율, 10%) 스탑-시장가 (갭이면 시가)
  비용    : 지정가 0.02% / 시장가 0.05% + 슬리피지 0.03% / 실제 펀딩(롱은 +펀딩 지불)

실행 (EC2, ~/upbit/ 에 두고)
  nohup python3 rebound_bot.py > /dev/null 2>&1 &     # 또는 start_rebound.sh
  python3 rebound_bot.py --report                       # 성적표
파일: rebound_bot_state.json(상태) · rebound_bot_trades.csv(매매) · rebound_bot_equity.csv(시간별 평가액) · rebound_bot.log
"""

import os, sys, json, time, logging, math
from logging.handlers import RotatingFileHandler
import numpy as np
import pandas as pd

# ===================== 설정 =====================
DRY_RUN     = True                     # 이 봇은 주문 코드 자체가 없음. 표시용.
SYMBOLS     = {"SOL": "SOL/USDT:USDT", "XRP": "XRP/USDT:USDT"}
PRINCIPAL   = 1_000_000 / 1400         # 가상 원금(USDT). 100만원 ≈ 714 USDT 기준. 코인별 반반
LEV         = 1.0                      # 기록은 1배. 2·3배 성적은 리포트에서 근사 환산
BASE_SIG    = 0.0025                   # BTC 2022~24 중앙 15분 변동폭 (백테스트와 동일 고정값)
P = dict(vr=1.5, mv=0.004, regime=1.5, probe=0.25, add_gap=0.008, add_frac=0.25,
         tp_min=0.003, tp_max=0.03, cut_min=0.01, stop=0.06, stop_cap=0.10, exit_frac=0.25,
         time_stop_h=36, limit_bars=3)
MAKER, TAKER, SLIP = 0.0002, 0.0005, 0.0003
HIST_BARS   = 2600                     # 지표 계산용 5분봉 (7일 = 2016봉 + 여유)
BAR_MS      = 300_000
DIR         = os.path.dirname(os.path.abspath(__file__))
STATE_F     = os.path.join(DIR, "rebound_bot_state.json")
TRADES_F    = os.path.join(DIR, "rebound_bot_trades.csv")
EQUITY_F    = os.path.join(DIR, "rebound_bot_equity.csv")
LOG_F       = os.path.join(DIR, "rebound_bot.log")
# 백테스트 기대치 (리포트 비교용)
BT_REF = dict(pf="1.22~1.35", win="63~64%", per_week="약 2회", cagr_1x="+7.7%", mdd_1x="-11.6%")
# ================================================

log = logging.getLogger("rebound")


def setup_log():
    log.setLevel(logging.INFO)
    h = RotatingFileHandler(LOG_F, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    log.addHandler(h)
    s = logging.StreamHandler(); s.setFormatter(h.formatter); log.addHandler(s)


# ---------------- 지표 (백테스트와 동일 정의) ----------------
def features(df):
    c, v = df["close"], df["volume"]
    r15 = c / c.shift(3) - 1
    v15 = v.rolling(3).sum()
    vr = v15 / v15.rolling(288).mean().shift(3)
    rv = c.pct_change()
    regime = rv.rolling(288).std() / rv.rolling(2016).std()
    sig15 = r15.rolling(2016).std().shift(1)
    return r15, vr, regime.fillna(0.0), sig15


def scale_of(sig):
    if sig is None or not np.isfinite(sig) or BASE_SIG <= 0:
        return 1.0
    return float(np.clip(sig / BASE_SIG, 0.5, 4.0))


# ---------------- 코인별 엔진 (백테스트 simulate 한 봉 처리와 동일 순서) ----------------
class Engine:
    FIELDS = ("equity", "qty", "avg", "cmax", "last_add", "sc", "cp", "t_in", "cs", "n_add",
              "pend_entry", "pend_tp", "mkt")

    def __init__(self, coin, equity):
        self.coin = coin
        self.equity = equity
        self.qty = self.avg = self.cmax = self.last_add = 0.0
        self.sc, self.cp, self.t_in, self.cs, self.n_add = 1.0, 0.0, None, None, 0
        self.pend_entry = self.pend_tp = self.mkt = None
        self.closed = []                         # 이번 봉에서 끝난 사이클 (저장 후 비움)

    # 상태 저장/복원
    def dump(self):
        return {k: getattr(self, k) for k in self.FIELDS}

    def load(self, d):
        for k in self.FIELDS:
            if k in d:
                setattr(self, k, d[k])

    @property
    def inpos(self):
        return self.qty > 0

    def _close(self, t, why):
        self.closed.append(dict(coin=self.coin, start=self.cs, end=t, pnl=round(self.cp, 4),
                                pnl_pct=round(self.cp / max(self.equity - self.cp, 1e-9) * 100, 3),
                                reason=why, max_notional=round(self.cmax, 2), adds=self.n_add,
                                hold_h=round((t - self.cs) / 3_600_000, 2),
                                equity_after=round(self.equity, 2)))
        log.info(f"[{self.coin}] ■ 사이클 종료({why}) 손익 {self.cp:+.2f} USDT · 평가액 {self.equity:.2f}")
        self.qty = self.avg = self.cmax = 0.0
        self.pend_tp = None

    def step(self, bar, feat, funding_rate):
        """bar: dict(ts, open, high, low, close) — 막 마감된 봉 / feat: 그 봉의 (r15, vr, regime, sig15)"""
        ts, o, h, l, c = bar["ts"], bar["open"], bar["high"], bar["low"], bar["close"]
        r15, vr, regime, sig = feat
        # ----- 이 봉에서 체결 처리 (결정은 직전 봉 종가에서 내려졌음) -----
        if self.inpos and self.mkt is not None:
            q = self.qty if self.mkt[0] == "all" else min(self.qty, self.mkt[1])
            px = o * (1 - SLIP)
            pnl = q * (px - self.avg) - q * px * TAKER
            self.equity += pnl; self.cp += pnl; self.qty -= q
            log.info(f"[{self.coin}] 시장가 {'전량' if self.mkt[0]=='all' else '부분'} 청산 {q:.4f} @ {px:.4f} ({pnl:+.2f})")
            if self.qty * px <= self.cmax * 0.03:
                self._close(ts, "시간손절" if self.mkt[0] == "all" else "쏠림손절")
            self.mkt = None
        if self.inpos:
            stop_d = min(P["stop"] * self.sc, P["stop_cap"])
            stop_px = self.avg * (1 - stop_d)
            tpx = self.avg * (1 + P["tp_max"] * self.sc)
            if l <= stop_px:
                px = min(o, stop_px) * (1 - SLIP)
                pnl = self.qty * (px - self.avg) - self.qty * px * TAKER
                self.equity += pnl; self.cp += pnl
                log.info(f"[{self.coin}] 전량 손절 @ {px:.4f} ({pnl:+.2f})")
                self._close(ts, "전량손절")
            elif h >= tpx:
                pnl = self.qty * (tpx - self.avg) - self.qty * tpx * MAKER
                self.equity += pnl; self.cp += pnl
                log.info(f"[{self.coin}] 전량 익절 @ {tpx:.4f} ({pnl:+.2f})")
                self._close(ts, "전량익절")
            else:
                if self.pend_tp is not None:
                    px, q, exp = self.pend_tp
                    if h >= px:
                        q = min(q, self.qty)
                        pnl = q * (px - self.avg) - q * px * MAKER
                        self.equity += pnl; self.cp += pnl; self.qty -= q; self.pend_tp = None
                        log.info(f"[{self.coin}] 부분 익절 {q:.4f} @ {px:.4f} ({pnl:+.2f})")
                        if self.qty * px <= self.cmax * 0.03:
                            self._close(ts, "쏠림익절")
                    elif ts >= exp:
                        self.pend_tp = None
                if self.inpos:
                    lvl = self.last_add * (1 - P["add_gap"] * self.sc)
                    budget = self.equity * LEV
                    if l <= lvl and self.qty * lvl < budget:
                        px = min(o, lvl)
                        add = min(self.qty * P["add_frac"], (budget - self.qty * px) / px)
                        if add > 0:
                            self.avg = (self.avg * self.qty + px * add) / (self.qty + add); self.qty += add
                            self.cmax = max(self.cmax, self.qty * px); self.last_add = px; self.n_add += 1
                            self.equity -= add * px * MAKER; self.cp -= add * px * MAKER
                            log.info(f"[{self.coin}] 추가 매수 {add:.4f} @ {px:.4f} → 평단 {self.avg:.4f}, 물량 {self.qty*px:.1f}")
        if not self.inpos and self.pend_entry is not None:
            px, exp = self.pend_entry
            if l <= px:
                budget = self.equity * LEV
                self.qty = budget * P["probe"] / px; self.avg = px; self.last_add = px; self.cmax = self.qty * px
                self.t_in, self.cs, self.sc, self.n_add = ts, ts, scale_of(sig), 0
                self.cp = -self.qty * px * MAKER; self.equity += self.cp
                self.pend_entry = None
                log.info(f"[{self.coin}] ▶ 진입 체결 {self.qty:.4f} @ {px:.4f} (물량 {self.qty*px:.1f} USDT, 배율 {self.sc:.2f}, "
                         f"손절선 -{min(P['stop']*self.sc, P['stop_cap'])*100:.1f}% / 전량익절 +{P['tp_max']*self.sc*100:.1f}%)")
            elif ts >= exp:
                log.info(f"[{self.coin}] 진입 지정가 미체결 → 취소")
                self.pend_entry = None
        if self.inpos and funding_rate:
            f = self.qty * c * funding_rate
            self.equity -= f; self.cp -= f
            log.info(f"[{self.coin}] 펀딩 {funding_rate*100:+.4f}% → {-f:+.3f} USDT")
        # ----- 이 봉 종가에서 결정 -----
        ok = np.isfinite(vr) and np.isfinite(r15)
        s_now = scale_of(sig)
        burst = ok and vr >= P["vr"] and abs(r15) >= P["mv"] * s_now
        if not self.inpos:
            if self.pend_entry is None and burst and regime >= P["regime"] and r15 < 0:
                exp = ts + P["limit_bars"] * BAR_MS
                self.pend_entry = (c, exp)
                log.info(f"[{self.coin}] ★ 급락 쏠림 신호 (15분 {r15*100:.2f}%, 거래량 {vr:.1f}배, 국면 {regime:.2f}) "
                         f"→ 지정가 매수 {c:.4f} 대기(15분)")
        else:
            un = c / self.avg - 1
            if (ts - self.t_in) / 3_600_000 >= P["time_stop_h"] and un < 0:
                self.mkt = ("all",)
                log.info(f"[{self.coin}] 36시간 경과·손실 {un*100:.2f}% → 다음 봉 전량 시장가")
            elif un <= -P["cut_min"] * self.sc and burst and r15 <= -P["mv"] * s_now:
                self.mkt = ("cut", self.cmax / c * P["exit_frac"])
                log.info(f"[{self.coin}] 손실 {un*100:.2f}% + 급락 쏠림 → 다음 봉 25% 시장가 손절")
            elif un >= P["tp_min"] * self.sc and burst and r15 >= P["mv"] * s_now and self.pend_tp is None:
                self.pend_tp = (c, self.cmax / c * P["exit_frac"], ts + P["limit_bars"] * BAR_MS)
                log.info(f"[{self.coin}] 이익 {un*100:.2f}% + 급등 쏠림 → 25% 지정가 익절 대기 {c:.4f}")
        return self.equity + (self.qty * (c - self.avg) if self.inpos else 0.0)


# ---------------- 데이터 ----------------
class Feed:
    def __init__(self):
        import ccxt
        self.ex = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})

    def ohlcv(self, symbol, since_ms=None, n=HIST_BARS):
        out, since = [], since_ms or (self.ex.milliseconds() - n * BAR_MS)
        while True:
            b = self.ex.fetch_ohlcv(symbol, "5m", since=since, limit=1500)
            if not b:
                break
            out += b
            if len(b) < 1500:
                break
            since = b[-1][0] + BAR_MS
            time.sleep(self.ex.rateLimit / 1000)
        df = pd.DataFrame(out, columns=["ts", "open", "high", "low", "close", "volume"]).drop_duplicates("ts")
        now = self.ex.milliseconds()
        return df[df["ts"] + BAR_MS <= now].sort_values("ts").reset_index(drop=True)   # 마감된 봉만

    def funding(self, symbol, ts_ms):
        """정산 시각(ts_ms)의 실제 펀딩비. 실패 시 0.01% (롱에 불리하게) 가정"""
        try:
            fr = self.ex.fetch_funding_rate_history(symbol, since=ts_ms - 60_000, limit=5)
            for f in fr:
                if abs(f["timestamp"] - ts_ms) <= 60_000:
                    return float(f["fundingRate"])
        except Exception as e:
            log.info(f"[경고] 펀딩 조회 실패 {symbol}: {e}")
        return 0.0001


# ---------------- 상태 ----------------
def load_state():
    if os.path.exists(STATE_F):
        with open(STATE_F, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_state(engines, last_ts):
    tmp = STATE_F + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"last_ts": last_ts, "engines": {k: e.dump() for k, e in engines.items()},
                   "saved": pd.Timestamp.now(tz="UTC").isoformat()}, f, ensure_ascii=False)
    os.replace(tmp, STATE_F)


def append_csv(path, rows):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df.to_csv(path, mode="a", header=not os.path.exists(path), index=False, encoding="utf-8-sig")


# ---------------- 메인 루프 ----------------
def run():
    setup_log()
    log.info("=" * 70)
    log.info(f"리바운드 전략 드라이런 시작 — 코인 {list(SYMBOLS)} · 원금 {PRINCIPAL:.1f} USDT(반반) · 기록 레버리지 {LEV:g}배")
    feed = Feed()
    st = load_state()
    engines = {k: Engine(k, PRINCIPAL / len(SYMBOLS)) for k in SYMBOLS}
    last_ts = {k: None for k in SYMBOLS}
    if st:
        for k, e in engines.items():
            if k in st["engines"]:
                e.load(st["engines"][k])
        last_ts = st.get("last_ts", last_ts)
        log.info(f"상태 복원: {', '.join(f'{k} 평가액 {e.equity:.2f}' for k, e in engines.items())}")
    last_eq_hour = None
    while True:
        try:
            eq_row = {"ts": None}
            for k, sym in SYMBOLS.items():
                df = feed.ohlcv(sym)
                if len(df) < 2100:
                    log.info(f"[{k}] 이력 부족({len(df)}봉) — 대기"); continue
                r15, vr, regime, sig = features(df)
                # 처리할 새 봉: last_ts 이후 마감된 봉 전부 (재시작 시 놓친 봉 따라잡기)
                start = 0 if last_ts[k] is None else int(np.searchsorted(df["ts"].values, last_ts[k], side="right"))
                if last_ts[k] is None:
                    start = len(df) - 1                      # 첫 실행은 최신 봉부터
                mtm = None
                for i in range(max(start, 2100), len(df)):
                    row = df.iloc[i]
                    ts = int(row["ts"])
                    fr = 0.0
                    t = pd.Timestamp(ts, unit="ms", tz="UTC")
                    if t.minute == 0 and t.hour % 8 == 0:          # 정산 봉: 이 봉에서 진입해도 펀딩 대상
                        fr = feed.funding(sym, ts)
                    mtm = engines[k].step(dict(ts=ts, open=row["open"], high=row["high"], low=row["low"], close=row["close"]),
                                          (r15.iloc[i], vr.iloc[i], regime.iloc[i], sig.iloc[i]), fr)
                    last_ts[k] = ts
                    append_csv(TRADES_F, engines[k].closed); engines[k].closed = []
                if mtm is not None:
                    eq_row[k] = round(mtm, 3); eq_row["ts"] = pd.Timestamp(last_ts[k], unit="ms", tz="UTC").isoformat()
            save_state(engines, last_ts)
            hr = pd.Timestamp.now(tz="UTC").floor("h")
            if eq_row["ts"] and hr != last_eq_hour:
                eq_row["total"] = round(sum(v for kk, v in eq_row.items() if kk in SYMBOLS), 3)
                append_csv(EQUITY_F, [eq_row]); last_eq_hour = hr
                pos = ", ".join(f"{k} {'보유 ' + format(e.qty*e.avg, '.0f') + 'USDT' if e.inpos else '대기'}" for k, e in engines.items())
                log.info(f"[시간 요약] 합계 평가액 {eq_row['total']:.2f} USDT · {pos}")
        except Exception as e:
            log.info(f"[오류] {type(e).__name__}: {e} — 60초 후 재시도")
            time.sleep(60)
            continue
        now = time.time()
        time.sleep(300 - (now % 300) + 20)          # 다음 5분봉 마감 + 20초


# ---------------- 리포트 ----------------
def report():
    print("=" * 70)
    print("리바운드 전략 드라이런 성적표")
    print("=" * 70)
    st = load_state()
    if st:
        tot = sum(e["equity"] for e in st["engines"].values())
        print(f"평가액(실현 기준): {tot:.2f} USDT (시작 {PRINCIPAL:.2f}) → {(tot/PRINCIPAL-1)*100:+.2f}%")
        for k, e in st["engines"].items():
            print(f"  {k}: {e['equity']:.2f} USDT · {'보유 중 평단 ' + format(e['avg'], '.4f') if e['qty'] > 0 else '대기'}")
    if not os.path.exists(TRADES_F):
        print("\n아직 끝난 매매 없음"); return
    t = pd.read_csv(TRADES_F)
    t["start"] = pd.to_datetime(t["start"], unit="ms", utc=True)
    days = max((pd.Timestamp.now(tz="UTC") - t["start"].min()).days, 1)
    w, l = t.pnl[t.pnl > 0], t.pnl[t.pnl <= 0]
    pf = w.sum() / -l.sum() if l.sum() < 0 else float("inf")
    print(f"\n매매 {len(t)}회 ({days}일, 주 {len(t)/days*7:.1f}회) · 승률 {(t.pnl>0).mean()*100:.0f}% · PF {pf:.2f}")
    print(f"평균 이익 {w.mean() if len(w) else 0:+.2f} / 평균 손실 {l.mean() if len(l) else 0:+.2f} USDT · 보유 중앙 {t.hold_h.median():.1f}h")
    print(t.groupby("coin").pnl.agg(회수="size", 합계="sum", 승률=lambda x: f"{(x>0).mean()*100:.0f}%").to_string())
    print("\n종료 사유"); print(t.groupby("reason").pnl.agg(회수="size", 평균="mean").round(2).to_string())
    print(f"\n레버리지 근사 (1배 기록 × 배수, 복리·한도 효과 제외): 2배 {t.pnl.sum()*2/PRINCIPAL*100:+.2f}% · 3배 {t.pnl.sum()*3/PRINCIPAL*100:+.2f}%")
    if os.path.exists(EQUITY_F):
        e = pd.read_csv(EQUITY_F)
        if len(e) > 1:
            s = e["total"]; print(f"MDD(시간별 평가액 기준): {(s/s.cummax()-1).min()*100:.2f}%")
    print(f"\n[백테스트 기대치(1배)] PF {BT_REF['pf']} · 승률 {BT_REF['win']} · 빈도 {BT_REF['per_week']} · "
          f"CAGR {BT_REF['cagr_1x']} · MDD {BT_REF['mdd_1x']}")
    print("[판정 기준] 3개월: 작동 점검(에러·체결 흐름) / 6개월(≈50회): PF ≥ 1.1 이면 실전 2배 전환 검토")


if __name__ == "__main__":
    report() if "--report" in sys.argv else run()
