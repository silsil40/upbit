#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[오더북 리바운드 (S4) — 드라이런 봇]   ※ 주문 없음, 가상 매매만 기록

전략 (백테스트 s4_strategy_backtest.py 와 동일, 사전 고정)
  코인   : XRPUSDT (USD-M 무기한)
  신호   : 5분봉이 끝난 순간 — 그 봉 거래량 ≥ 직전 24시간(288봉) 평균의 3배
           & ±1% 호가 불균형 (매수금액−매도금액)/(합) 의 30일 z점수 ≤ -2  (매도 호가가 두꺼움)
  진입   : D 분할 (2026-10-09) — 최대 물량의 1/3 을 다음 5분봉 시가에 시장가(0.05% + 슬리피지 0.03%),
           나머지 1/3 씩은 첫 진입가 -1% / -2% 에 지정가(0.02%, 가격이 1bp 이상 뚫고 지나가야 체결)
           4시간 안에 안 걸린 추가분은 청산 때 취소
  청산   : 진입 4시간 뒤 — 그 순간 가격에 지정가 매도, 다음 5분 안에 가격이 뚫고 지나가면 체결(수수료 0.02%),
           아니면 그다음 봉 시가에 시장가.  손절 없음.  보유 중 새 신호는 무시
  펀딩   : 실제 펀딩비 (보유 중 지나는 정산마다 롱이 지불)

호가 데이터
  - 30초마다 바이낸스 호가(1000단계)를 받아 중간가 ±1% 안의 매수·매도 금액을 계산해 저장 (ob_depth_live.csv)
  - 신호의 z점수에는 30일치 '5분봉 끝 시점 불균형'이 필요 → 시작할 때 공식 파일(data.binance.vision bookDepth)로 채움
  - 매일 어제 공식 파일을 받아 실시간 계산값과 비교 → ob_verify.csv / 로그 [검증] (상관계수·평균 차이·신호 일치)
    ※ 공식 파일과 실시간 계산이 다르게 나오면 신호가 어긋남 → 이게 드라이런의 가장 중요한 확인 항목

파일 (~/upbit/ 기준)
  ob_bot.log · ob_bot_state.json · ob_bot_trades.csv · ob_bot_equity.csv · ob_depth_live.csv · ob_imb_5m.csv · ob_verify.csv
명령
  venv/bin/python ob_bot.py            # 실행
  venv/bin/python ob_bot.py --report   # 성적표
  venv/bin/python ob_bot.py --verify 2026-10-07   # 그날 공식 파일 vs 실시간 계산 비교 (손으로)
"""

import os, sys, io, json, time, math, zipfile, logging, datetime as dt
import urllib.request, urllib.parse, urllib.error
from collections import deque
import numpy as np
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))
SYMBOL = "XRPUSDT"
PRINCIPAL = 700.0            # 가상 원금 (USDT)
LEV = 1.0                    # 기록용 배율 (1배 기준으로 기록, 성적표에서 배율 환산)
BAR_MS = 300_000
HOLD = 48                    # 4시간
VOL_WIN, VOL_MULT = 288, 3.0
Z_WIN, Z_MIN, Z_THR = 30 * 288, 15 * 288, -2.0
TAKER, MAKER, SLIP = 0.0005, 0.0002, 0.0003
DEPTH_EVERY = 30             # 초
FAPI = "https://fapi.binance.com"
BV = "https://data.binance.vision/data/futures/um/daily/bookDepth"
F_STATE, F_TRADES, F_EQ = "ob_bot_state.json", "ob_bot_trades.csv", "ob_bot_equity.csv"
F_LIVE, F_IMB, F_VER, F_LOG = "ob_depth_live.csv", "ob_imb_5m.csv", "ob_verify.csv", "ob_bot.log"
BT_REF = dict(pf="1.66~2.14 (D 분할)", win="60% 안팎", per_week="약 3회", avg="+0.31%/회(1배, 최대 물량 기준)")
PLAN = [(1 / 3, 0.0), (1 / 3, 0.01), (1 / 3, 0.02)]      # D 분할: (비중, 첫 진입가 대비 하락폭)
STRICT = 0.0001

log = logging.getLogger("ob")


def setup_log():
    log.setLevel(logging.INFO)
    if not log.handlers:
        h = logging.FileHandler(F_LOG, encoding="utf-8")
        h.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S"))
        log.addHandler(h)


# ---------------------------------------------------------------- 통신
def http_get(path, params, base=FAPI, raw=False):
    url = f"{base}{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
    for k in range(4):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                b = r.read()
                return b if raw else json.loads(b.decode())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if k == 3:
                raise
        except Exception:
            if k == 3:
                raise
        time.sleep(2 * (k + 1))


def klines(start_ms, limit=1000):
    b = http_get("/fapi/v1/klines", {"symbol": SYMBOL, "interval": "5m", "startTime": start_ms, "limit": limit})
    return [dict(ts=int(x[0]), open=float(x[1]), high=float(x[2]), low=float(x[3]), close=float(x[4]), volume=float(x[5])) for x in (b or [])]


def funding_between(t0_ms, t1_ms):
    b = http_get("/fapi/v1/fundingRate", {"symbol": SYMBOL, "startTime": t0_ms, "endTime": t1_ms, "limit": 1000}) or []
    return [(int(x["fundingTime"]), float(x["fundingRate"])) for x in b]


def depth_snapshot():
    """중간가 ±1% 안의 매수·매도 금액 (USDT)"""
    b = http_get("/fapi/v1/depth", {"symbol": SYMBOL, "limit": 1000})
    bids = np.array(b["bids"], float); asks = np.array(b["asks"], float)
    mid = (bids[0, 0] + asks[0, 0]) / 2
    bid1 = float((bids[bids[:, 0] >= mid * 0.99, 0] * bids[bids[:, 0] >= mid * 0.99, 1]).sum())
    ask1 = float((asks[asks[:, 0] <= mid * 1.01, 0] * asks[asks[:, 0] <= mid * 1.01, 1]).sum())
    return mid, bid1, ask1


def to_ms(idx):
    """DatetimeIndex → 밀리초 정수 (pandas 버전별 시각 단위 차이와 무관하게)"""
    return pd.DatetimeIndex(idx).tz_convert("UTC").tz_localize(None).astype("datetime64[ms]").astype("int64")


# ---------------------------------------------------------------- 공식 파일 (bookDepth)
def official_day(day):
    """공식 bookDepth 하루치 → 5분봉 끝 불균형 Series (index = 봉 끝 시각 ms) 와 30초 원자료(bid1, ask1)"""
    blob = http_get(f"/{SYMBOL}/{SYMBOL}-bookDepth-{day:%Y-%m-%d}.zip", None, base=BV, raw=True)
    if blob is None:
        return None, None
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        df = pd.read_csv(io.BytesIO(z.read(z.namelist()[0])))
    df.columns = [str(c).strip().lower() for c in df.columns]
    df["percentage"] = pd.to_numeric(df["percentage"], errors="coerce")
    p = df.pivot_table(index="timestamp", columns="percentage", values="notional", aggfunc="last")
    snap = pd.DataFrame({"bid1": p.get(-1), "ask1": p.get(1)})
    snap.index = pd.to_datetime(snap.index, utc=True, format="mixed")
    imb = ((snap["bid1"] - snap["ask1"]) / (snap["bid1"] + snap["ask1"])).resample("5min", label="right", closed="right").last()
    return pd.Series(imb.values, index=to_ms(imb.index)), snap


# ---------------------------------------------------------------- 신호 계산 (백테스트와 같은 식)
class SignalCalc:
    def __init__(self):
        self.vols = deque(maxlen=VOL_WIN)      # 직전 288봉 거래량 (현재 봉 제외)
        self.imbs = deque(maxlen=Z_WIN)        # 직전 8640개 5분 불균형 (현재 포함, 결측은 nan)

    def push(self, vol, imb):
        """현재 봉 마감 → (거래량 배수, z, 신호)"""
        vm = np.mean(self.vols) if len(self.vols) >= 100 else np.nan
        self.imbs.append(imb if imb is not None else np.nan)
        arr = np.fromiter(self.imbs, float); ok = ~np.isnan(arr)
        zv = np.nan
        if ok.sum() >= Z_MIN and not math.isnan(arr[-1]):
            m, s = arr[ok].mean(), arr[ok].std(ddof=1)
            zv = (arr[-1] - m) / s if s > 0 else np.nan
        self.vols.append(vol)
        vr = vol / vm if vm and vm > 0 else np.nan
        sig = bool(vr == vr and zv == zv and vr >= VOL_MULT and zv <= Z_THR)
        return vr, zv, sig


# ---------------------------------------------------------------- 가상 매매 엔진 (백테스트 simulate 와 같은 순서)
class Engine:
    FIELDS = ("equity", "pos", "pending", "exit_stage", "lim", "last_close")

    def __init__(self, equity):
        self.equity = equity
        self.pos = None            # dict(t_in, k_ts, p0, fills=[[비중, 가격, 체결봉 시작ms, 수수료, 펀딩누적]], adds=[[비중, 가격]], bar, z, vr)
        self.pending = None        # 신호 정보 (다음 봉에서 진입)
        self.exit_stage = None     # None / "fallback" (지정가 실패 → 다음 봉 시가 시장가)
        self.lim = None
        self.last_close = None
        self.closed = []

    def _close(self, t_out, px_out, xfee, why):
        p = self.pos
        r = sum(f * (px_out / px - 1 - fee - xfee - fc) for f, px, _, fee, fc in p["fills"])
        pnl = self.equity * LEV * r
        self.equity += pnl
        used = sum(f for f, *_ in p["fills"])
        self.closed.append(dict(entry_time=p["t_in"], exit_time=t_out, entry=round(p["p0"], 5), exit=round(px_out, 5),
                                ret_pct=round(r * 100, 4), pnl=round(pnl, 4), used_pct=round(used * 100, 1),
                                adds=len(p["fills"]) - 1, funding_pct=round(sum(f * fc for f, _, _, _, fc in p["fills"]) * 100, 4),
                                reason=why, bar=p.get("bar", ""), z=p.get("z"), vr=p.get("vr")))
        log.info(f"■ 청산({why}) 물량 {used*100:.0f}% · 수익 {r*100:+.2f}%(최대 물량 기준) · 평가액 {self.equity:.2f}")
        self.pos = None; self.exit_stage = None; self.lim = None

    def step(self, bar, vr, zv, sig, fundings):
        """bar: 막 마감된 5분봉 dict(ts=시작ms, open, high, low, close)"""
        ts, o, h, l = bar["ts"], bar["open"], bar["high"], bar["low"]
        just_exited = False
        if self.pos is not None:
            for ft, rate in fundings:                       # 직전 봉 시작 ~ 이 봉 시작 사이 정산: 그 전에 체결된 분량만
                if ts - BAR_MS < ft <= ts:
                    for fl in self.pos["fills"]:
                        if fl[2] < ft:
                            fl[4] += rate
            if self.exit_stage == "fallback":
                self._close(ts, o * (1 - SLIP), TAKER, "시간(지정가 실패→시장가)"); just_exited = True
            elif ts == self.pos["k_ts"]:
                self.lim = self.last_close
                if h > self.lim * 1.0001:
                    self._close(ts, self.lim, MAKER, "시간(지정가)"); just_exited = True
                else:
                    self.exit_stage = "fallback"
        if self.pos is None and not just_exited and self.pending is not None:
            px = o * (1 + SLIP)
            self.pos = dict(t_in=ts, k_ts=ts + HOLD * BAR_MS, p0=px, fills=[[PLAN[0][0], px, ts, TAKER, 0.0]],
                            adds=[[f, px * (1 - d)] for f, d in PLAN[1:]], **self.pending)
            log.info(f"▶ 진입 1/3 {px:.4f} ({self.pending['bar']}에서 신호, z {self.pending['z']:.2f}, 거래량 {self.pending['vr']:.1f}배) "
                     f"· 추가 지정가 {', '.join(f'{a[1]:.4f}' for a in self.pos['adds'])}")
        self.pending = None
        if self.pos is not None and self.exit_stage is None and ts < self.pos["k_ts"]:   # 보유 중(청산 봉 전) 추가분 체결
            still = []
            for f, lvl in self.pos["adds"]:
                if l < lvl * (1 - STRICT):
                    fp = min(o, lvl)
                    self.pos["fills"].append([f, fp, ts, MAKER, 0.0])
                    log.info(f"  + 추가 1/3 체결 {fp:.4f}")
                else:
                    still.append([f, lvl])
            self.pos["adds"] = still
        if sig and self.pos is None:
            self.pending = dict(bar="상승봉" if bar["close"] > o else "하락봉", z=round(float(zv), 3), vr=round(float(vr), 2))
            log.info(f"★ 신호 — 거래량 {vr:.1f}배 · 호가 z {zv:.2f} · {self.pending['bar']} → 다음 봉 시가 1/3 진입")
        elif sig:
            log.info(f"☆ 신호(보유 중이라 무시) — 거래량 {vr:.1f}배 · 호가 z {zv:.2f}")
        self.last_close = bar["close"]

    def to_dict(self):
        return {k: getattr(self, k) for k in self.FIELDS}

    def load(self, d):
        for k in self.FIELDS:
            if k in d:
                setattr(self, k, d[k])
        if self.pos is not None and "fills" not in self.pos:          # 예전(A) 형식 보유분이 있으면 D 형식으로 변환
            self.pos = dict(t_in=self.pos["t_in"], k_ts=self.pos["k_ts"], p0=self.pos["px_in"],
                            fills=[[1.0, self.pos["px_in"], self.pos["t_in"], TAKER, 0.0]], adds=[],
                            bar=self.pos.get("bar", ""), z=self.pos.get("z"), vr=self.pos.get("vr"))


# ---------------------------------------------------------------- 저장
def append_csv(path, rows):
    if rows:
        pd.DataFrame(rows).to_csv(path, mode="a", header=not os.path.exists(path), index=False)


def save_state(st):
    tmp = F_STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, F_STATE)


def load_imb_history():
    if os.path.exists(F_IMB):
        s = pd.read_csv(F_IMB)
        return dict(zip(s["ts"].astype("int64"), s["imb"].astype(float)))
    return {}


def save_imb_history(h):
    cut = max(h) - (Z_WIN + 600) * BAR_MS if h else 0
    pd.DataFrame(sorted((k, v) for k, v in h.items() if k >= cut), columns=["ts", "imb"]).to_csv(F_IMB, index=False)


def seed_official(h, days=31):
    """빠진 날의 5분 불균형을 공식 파일로 채움 (공식 파일은 하루 늦게 올라옴)"""
    today = dt.datetime.now(dt.timezone.utc).date()
    got = 0
    for d in range(days, 0, -1):
        day = today - dt.timedelta(days=d)
        t0 = int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc).timestamp() * 1000)
        have = sum(1 for k in range(t0 + BAR_MS, t0 + 86_400_000 + 1, BAR_MS) if k in h)
        if have >= 280:
            continue
        s, _ = official_day(day)
        if s is None:
            continue
        for k, v in s.items():
            if k not in h and v == v:
                h[k] = float(v)
        got += 1
    return got


def verify_day(day, live_path=F_LIVE):
    """그날 공식 파일 vs 실시간 계산 비교 → dict(status=ok/공식없음/겹침부족, ...)"""
    s_off, snap = official_day(day)
    if s_off is None:
        return dict(status="공식없음", day=str(day))
    if not os.path.exists(live_path):
        return dict(status="겹침부족", day=str(day), off_bars=int(s_off.notna().sum()), live_rows=0, overlap=0)
    L = pd.read_csv(live_path)
    L["t"] = pd.to_datetime(L["ts"], unit="ms", utc=True)
    t0 = pd.Timestamp(day, tz="UTC"); t1 = t0 + pd.Timedelta(days=1)
    L = L[(L["t"] > t0) & (L["t"] <= t1)]
    off_bars = int(s_off.notna().sum())
    off_span = f"{snap.index.min():%H:%M}~{snap.index.max():%H:%M}" if len(snap) else "-"
    if len(L) < 20:
        return dict(status="겹침부족", day=str(day), off_bars=off_bars, off_span=off_span, live_rows=len(L), overlap=0)
    li = ((L["bid1"] - L["ask1"]) / (L["bid1"] + L["ask1"])).values
    s_live = pd.Series(li, index=L["t"]).resample("5min", label="right", closed="right").last()
    s_live.index = to_ms(s_live.index)
    j = pd.concat([s_off.rename("off"), s_live.rename("live")], axis=1).dropna()
    if len(j) < 24:                                   # 2시간 미만 겹치면 비교 의미 없음
        return dict(status="겹침부족", day=str(day), off_bars=off_bars, off_span=off_span, live_rows=len(L), overlap=len(j))
    lvl = pd.concat([snap.resample("5min").last(), L.set_index("t")[["bid1", "ask1"]].resample("5min").last()],
                    axis=1, keys=["o", "l"]).dropna()
    ratio_b = float((lvl["l"]["bid1"] / lvl["o"]["bid1"]).median()) if len(lvl) else np.nan
    return dict(status="ok", day=str(day), n=len(j), corr=round(float(j["off"].corr(j["live"])), 4),
                mean_abs_diff=round(float((j["off"] - j["live"]).abs().mean()), 4), bid_level_ratio=round(ratio_b, 3),
                off_bars=off_bars)


# ---------------------------------------------------------------- 실행
def run():
    setup_log()
    st = json.load(open(F_STATE, encoding="utf-8")) if os.path.exists(F_STATE) else {}
    eng = Engine(PRINCIPAL); eng.load(st.get("engine", {}))
    calc = SignalCalc()
    hist = load_imb_history()
    log.info("=" * 70)
    log.info(f"오더북 리바운드(S4) 드라이런 시작 [D 분할: 1/3 시장가 + -1%·-2% 지정가] — {SYMBOL} · 원금 {eng.equity:.1f} USDT · 기록 배율 {LEV:g}배")
    n = seed_official(hist)
    log.info(f"호가 불균형 이력 {len(hist):,}개 (공식 파일로 {n}일 채움)")
    save_imb_history(hist)

    now = int(time.time() * 1000)
    last_ts = st.get("last_ts") or (now // BAR_MS - 2) * BAR_MS
    # 거래량 기준선·불균형 창 미리 채우기 (마지막 처리 봉 이전 30일)
    warm = []
    s = last_ts - (VOL_WIN + 5) * BAR_MS
    while s <= last_ts:
        b = klines(s)
        if not b:
            break
        warm += [x for x in b if x["ts"] <= last_ts]; s = b[-1]["ts"] + BAR_MS
        if len(b) < 1000:
            break
    for x in warm[-VOL_WIN:]:
        calc.vols.append(x["volume"])
    for end in range(last_ts + BAR_MS - (Z_WIN - 1) * BAR_MS, last_ts + BAR_MS + 1, BAR_MS):   # 봉 끝 시각 기준
        calc.imbs.append(hist.get(end, np.nan))
    last_depth = 0; last_hour = None; verify_try = 0
    verified = list(st.get("verified", []))
    live_start = (pd.Timestamp(int(pd.read_csv(F_LIVE, nrows=1)["ts"].iloc[0]), unit="ms", tz="UTC").date()
                  if os.path.exists(F_LIVE) else dt.datetime.now(dt.timezone.utc).date())
    live_buf = []; recent = deque(maxlen=600)                 # 최근 5시간 실시간 호가 (메모리)
    while True:
        try:
            now = int(time.time() * 1000)
            if now - last_depth >= DEPTH_EVERY * 1000:
                mid, b1, a1 = depth_snapshot()
                live_buf.append(dict(ts=now, mid=round(mid, 6), bid1=round(b1, 2), ask1=round(a1, 2)))
                recent.append((now, b1, a1))
                last_depth = now
                if len(live_buf) >= 10:
                    append_csv(F_LIVE, live_buf); live_buf = []
            # 새로 마감된 봉 처리 (마감 후 15초 여유)
            if now >= last_ts + 2 * BAR_MS + 15_000:
                bars = [x for x in klines(last_ts + BAR_MS) if x["ts"] + BAR_MS <= now - 5_000]
                if live_buf:
                    append_csv(F_LIVE, live_buf); live_buf = []
                if bars:
                    fund = funding_between(bars[0]["ts"] - 8 * 3_600_000, bars[-1]["ts"] + BAR_MS)
                    for bar in bars:
                        end = bar["ts"] + BAR_MS
                        w = [x for x in recent if end - BAR_MS < x[0] <= end]       # 그 봉 안의 마지막 스냅샷
                        imb = None
                        if w:
                            _, b1_, a1_ = w[-1]; imb = (b1_ - a1_) / (b1_ + a1_)
                            hist[end] = float(imb)
                        elif end in hist:
                            imb = hist[end]
                        vr, zv, sig = calc.push(bar["volume"], imb)
                        eng.step(bar, vr, zv, sig, fund)
                        last_ts = bar["ts"]
                    append_csv(F_TRADES, eng.closed); eng.closed = []
                    save_imb_history(hist)
                    save_state(dict(last_ts=last_ts, engine=eng.to_dict(), verified=verified))
                    hr = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H")
                    if hr != last_hour:
                        last_hour = hr
                        unreal = (sum(f * (bars[-1]["close"] / px - 1) for f, px, *_ in eng.pos["fills"]) * 100) if eng.pos else None
                        stat = f"보유 중 {unreal:+.2f}%" if eng.pos else "대기"
                        extra = f" · 호가 z {zv:.2f} · 거래량 {vr:.1f}배" if (zv == zv and vr == vr) else " · 호가 z 계산 전(이력 부족)"
                        log.info(f"[시간 요약] 평가액 {eng.equity:.2f} USDT · {stat}{extra}")
                        append_csv(F_EQ, [dict(ts=now, equity=round(eng.equity, 4), holding=int(eng.pos is not None))])
            # 공식 파일과 대조 — 검증 안 된 최근 3일을 1시간마다 시도 (공식 파일은 하루 늦게, 가끔 더 늦게 올라옴)
            if now - verify_try >= 3_600_000:
                verify_try = now
                today = dt.datetime.now(dt.timezone.utc).date()
                for back in (3, 2, 1):
                    day = today - dt.timedelta(days=back)
                    if str(day) in verified or day < live_start:
                        continue
                    v = verify_day(day)
                    if v["status"] == "ok":
                        append_csv(F_VER, [{k: v[k] for k in ("day", "n", "corr", "mean_abs_diff", "bid_level_ratio")}])
                        verified.append(str(day)); verified[:] = verified[-30:]
                        log.info(f"[검증] {day} 공식 vs 실시간 — 상관 {v['corr']:.3f} · 평균 차이 {v['mean_abs_diff']:.4f} · "
                                 f"매수 금액 비율 {v['bid_level_ratio']} ({v['n']}개 봉)")
                    elif v["status"] == "겹침부족":
                        verified.append(str(day)); verified[:] = verified[-30:]           # 다시 시도해도 같으니 건너뜀
                        log.info(f"[검증] {day} 건너뜀 — 공식 파일 5분봉 {v.get('off_bars', 0)}개({v.get('off_span', '-')} UTC) · "
                                 f"실시간 {v.get('live_rows', 0)}행 · 겹치는 봉 {v.get('overlap', 0)}개 (2시간 미만이라 비교 불가)")
                    elif back == 1:
                        log.info(f"[검증] {day} 공식 파일 아직 안 올라옴 — 1시간 뒤 재시도")
                save_state(dict(last_ts=last_ts, engine=eng.to_dict(), verified=verified))
            time.sleep(5)
        except Exception as e:
            log.info(f"[오류] {type(e).__name__}: {e} — 60초 후 재시도")
            time.sleep(60)


# ---------------------------------------------------------------- 성적표
def report():
    print("=" * 70); print("오더북 리바운드(S4) 드라이런 성적표"); print("=" * 70)
    st = json.load(open(F_STATE, encoding="utf-8")) if os.path.exists(F_STATE) else {}
    eq = st.get("engine", {}).get("equity", PRINCIPAL)
    print(f"평가액(실현 기준): {eq:.2f} USDT (시작 {PRINCIPAL:.2f}) → {(eq / PRINCIPAL - 1) * 100:+.2f}%")
    if st.get("engine", {}).get("pos"):
        p = st["engine"]["pos"]
        print(f"  보유 중: 첫 진입 {p['p0']:.4f} ({pd.Timestamp(p['t_in'], unit='ms', tz='Asia/Seoul'):%m-%d %H:%M}) · "
              f"체결 {len(p['fills'])}/3 · 남은 추가 지정가 {', '.join(f'{a[1]:.4f}' for a in p['adds']) or '없음'}")
    if os.path.exists(F_TRADES):
        t = pd.read_csv(F_TRADES)
        r = t["ret_pct"] / 100
        pf = r[r > 0].sum() / -r[r <= 0].sum() if (r <= 0).any() and r[r <= 0].sum() < 0 else float("nan")
        days = max((t["exit_time"].max() - t["entry_time"].min()) / 86_400_000, 1)
        print(f"\n끝난 매매 {len(t)}회 · 승률 {(r > 0).mean()*100:.0f}% · 평균 {r.mean()*100:+.3f}% · PF {pf:.2f} · 주당 {len(t) / days * 7:.1f}회")
        print(f"백테스트 기준: PF {BT_REF['pf']} · 승률 {BT_REF['win']} · {BT_REF['per_week']} · {BT_REF['avg']}")
        g = t.groupby("bar")["ret_pct"]
        print("\n신호 봉별 (백테스트: 하락봉 PF 2.63 · 상승봉 PF 1.11)")
        print(pd.DataFrame({"회수": g.size(), "평균%": g.mean().round(3), "합계%": g.sum().round(2)}).to_string())
        print("\n청산 방식"); print(t["reason"].value_counts().to_string())
        if "used_pct" in t.columns:
            print(f"\n분할: 평균 실제 물량 {t['used_pct'].mean():.0f}% · 추가분 체결률 {t['adds'].sum() / (len(t) * 2) * 100:.0f}% (백테스트: 59% · 40% 안팎)")
        print(f"\n배율 환산 (목표 1.5배): {(r * 1.5).sum()*100:+.1f}%  · 참고 1배 {r.sum()*100:+.1f}%")
    else:
        print("\n아직 끝난 매매 없음")
    if os.path.exists(F_VER):
        v = pd.read_csv(F_VER)
        print("\n[호가 검증] 공식 파일 vs 실시간 계산 (최근 7일)")
        print(v.tail(7).to_string(index=False))
        print("  → 상관 0.9 이상 · 평균 차이 작음 · 매수 금액 비율 1 근처면 백테스트 신호와 같은 신호")


if __name__ == "__main__":
    if "--verify" in sys.argv:                      # 손으로 검증: venv/bin/python ob_bot.py --verify 2026-10-07
        d = sys.argv[sys.argv.index("--verify") + 1] if len(sys.argv) > sys.argv.index("--verify") + 1 else \
            str(dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))
        print(verify_day(dt.date.fromisoformat(d)))
    elif "--report" in sys.argv:
        report()
    else:
        run()
