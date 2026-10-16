#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[리바운드 v2 — 드라이런 봇]  주문 코드 없음 · 가상 체결만 · 표준 라이브러리만 (메모리 수십 MB)

전략 (백테스트 pos_limit.py / rebound2_combo.py 와 같은 규칙)
  신호: SOL·XRP 5분봉, 4시간 수익률 z ≤ -2  &  OI 수량 4시간 변화 z ≤ -2  (최근 30일 기준, 그 값 제외) — 새로 켜지는 순간만
  진입: 신호 봉이 닫힌 직후의 현재가 P0 기준 · 3방식 동시 가상 체결 (방식·코인마다 포지션 하나, 겹치지 않음)
        L3  = 1/3씩 -0.5% / -1% / -2% 지정가      L10 = -1% 지정가      D = 1/3 시장가 + -1% + -2% 지정가
        지정가 1시간(12봉) 유효 · 5분봉 저가가 지정가를 1bp 이상 뚫어야 체결
  청산: 다리마다 체결 후 1시간 → 그 봉 시가 지정가(고가가 1bp 뚫으면 체결), 아니면 다음 봉 시가 시장가
        재난손절(코드): 3분할 전량 평단 -15% 에 5분봉 저가가 닿으면 전량 시장가 (+슬리피지 0.2% 가정)
  비용: 시장가 0.08% · 지정가 0.02% · 실제 펀딩 (청산 때 바이낸스 펀딩 기록으로 계산)
OI 시점: 백테스트처럼 '봉 b 시점 OI' 를 봉 b 가 닫힌 뒤(b+5분)에 사용. API 시각과 비전 시각이 다르면 RB2_OI_OFFSET(봉) 로 맞춤
          (rebound2_check.py 의 B 결과). OI 가 늦게 올라오면 최대 4분 기다리고, 그 지연은 신호 로그에 남김.

파일 (rebound_v2_data/)
  signals.csv  : 봉마다 코인·종가·OI·OI 지연·z_r4·z_oi4·신호   ← 나중에 비전 파일과 대조용
  trades.csv   : 가상 매매 (방식·코인·신호시각·P0·체결 다리·청산·수익률·사유·최대역행)
  oi_{코인}.csv : OI 기록 누적 (API 는 30일까지만 줘서, 재시작해도 30일 넘는 기록을 쓰려고)
  <파일이름>.log : 회전 로그 · 매시 요약 (rebound_bot.py 로 두면 rebound_bot.log — stats_rebound.sh 와 호환)

실행: 서버에서는 rebound_bot.py 로 저장 → ./restart_rebound.sh  (start/stop/stats_rebound.sh 그대로 사용)
      python3 rebound_bot.py --report  (성적표만 출력, --stats 도 같음)
환경변수: RB2_OI_OFFSET (기본 -1 — 봉 b 에 API 시각 b+5분 OI 를 씀. 2026-10 확인)
"""

import os, sys, json, time, math, csv, logging, urllib.request, urllib.parse
from logging.handlers import RotatingFileHandler
from collections import deque

SYMS = ["SOLUSDT", "XRPUSDT"]
FAPI = "https://fapi.binance.com"
BAR = 300
W30, MINP = 8640, 4320
LB = 48                       # 4시간
HOLD, WIN = 12, 12
COST_T, MAKER, TICK = 0.0008, 0.0002, 0.0001
STOP, STOP_SLIP = 0.15, 0.002
Z_TH = -2.0
OI_OFFSET = int(os.environ.get("RB2_OI_OFFSET", "-1"))     # rebound2_check B: 비전 'b' = API 'b+5분' (SOL·XRP 상관 1.000)
OI_WAIT = 240
CATCHUP_MAX = 2               # 이보다 오래된 봉에서 난 신호는 진입 안 함 (재시작·지연 후 따라잡기)
METHODS = {"L3": [("lim", 0.005, 1 / 3), ("lim", 0.010, 1 / 3), ("lim", 0.020, 1 / 3)],
           "L10": [("lim", 0.010, 1.0)],
           "D": [("mkt", 0.0, 1 / 3), ("lim", 0.010, 1 / 3), ("lim", 0.020, 1 / 3)]}
DIR = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(DIR, "rebound_v2_data")
LOGFILE = os.path.splitext(os.path.basename(os.path.abspath(__file__)))[0] + ".log"   # rebound_bot.py 로 두면 rebound_bot.log

log = logging.getLogger("rb2")


# ───────────────────────────── HTTP ─────────────────────────────
def http(path, params):
    url = f"{FAPI}{path}?{urllib.parse.urlencode(params)}"
    last = None
    for k in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "rb2"}), timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e
            time.sleep(2 * (k + 1))
    raise last


def klines(sym, start_s, end_s=None):
    """닫힌 5분봉만 [(open_s, o, h, l, c)]"""
    out, cur = [], start_s
    now = time.time()
    while True:
        p = {"symbol": sym, "interval": "5m", "limit": 1500, "startTime": int(cur * 1000)}
        if end_s:
            p["endTime"] = int(end_s * 1000)
        j = http("/fapi/v1/klines", p)
        rows = [(int(k[0]) // 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in j if int(k[6]) / 1000 < now]
        out += rows
        if len(j) < 1500 or not rows:
            break
        cur = rows[-1][0] + BAR
    return out


def oi_hist(sym, start_s, end_s):
    """[start_s, end_s] 구간 5분 OI. 요청 구간 밖 값(API 가 최근 값을 대신 줄 때)은 버리고,
    시각이 앞으로 안 나가면 멈춤 — 무한 반복 방지 (요청 최대 80번)"""
    out, cur = {}, start_s
    for _ in range(80):
        if cur >= end_s:
            break
        hi = min(end_s, cur + 500 * BAR)
        j = http("/futures/data/openInterestHist", {"symbol": sym, "period": "5m", "limit": 500,
                                                   "startTime": int(cur * 1000), "endTime": int(hi * 1000)})
        got = [(int(r["timestamp"]) // 1000, float(r["sumOpenInterest"])) for r in j]
        got = [(t, v) for t, v in got if cur <= t <= hi]
        if got:
            out.update(got)
            nxt = max(t for t, _ in got) + 1
            cur = nxt if nxt > cur else hi + 1
        else:
            cur = hi + 1
        time.sleep(0.2)
    return out


def funding_sum(sym, t0, t1):
    if t1 <= t0:
        return 0.0
    try:
        j = http("/fapi/v1/fundingRate", {"symbol": sym, "startTime": int(t0 * 1000) + 1, "endTime": int(t1 * 1000), "limit": 100})
        return sum(float(r["fundingRate"]) for r in j if t0 < int(r["fundingTime"]) / 1000 <= t1)
    except Exception as e:
        log.warning(f"{sym} 펀딩 조회 실패 ({e}) — 0 으로 계산")
        return 0.0


def price(sym):
    return float(http("/fapi/v1/ticker/price", {"symbol": sym})["price"])


# ───────────────────────────── 특징 (pos_map.features 와 같은 계산) ─────────────────────────────
class Roll:
    """직전 W30 칸(빈 칸 포함)의 평균·표본표준편차 — 값 있는 칸이 MINP 이상일 때만"""
    def __init__(self):
        self.q, self.n, self.s, self.ss, self.k = deque(), 0, 0.0, 0.0, 0

    def stats(self):
        if self.n < MINP:
            return None, None
        m = self.s / self.n
        var = (self.ss - self.s * self.s / self.n) / (self.n - 1)
        return m, (math.sqrt(var) if var > 0 else None)

    def add(self, v):
        self.q.append(v)
        if v is not None:
            self.n += 1
            self.s += v
            self.ss += v * v
        if len(self.q) > W30:
            u = self.q.popleft()
            if u is not None:
                self.n -= 1
                self.s -= u
                self.ss -= u * u
        self.k += 1
        if self.k % 2000 == 0:                                   # 누적 오차 정리
            w = [x for x in self.q if x is not None]
            self.n, self.s, self.ss = len(w), sum(w), sum(x * x for x in w)


class Feat:
    def __init__(self):
        self.last_ts = None
        self.lc, self.loi = deque(maxlen=LB + 1), deque(maxlen=LB + 1)
        self.rr, self.rd = Roll(), Roll()
        self.last_oi, self.oi_age = None, 99
        self.prev_sig = False

    def push(self, ts, close, oi):
        while self.last_ts is not None and ts - self.last_ts > BAR:   # 빈 봉 채우기
            self._append(self.last_ts + BAR, None, None)
        return self._append(ts, close, oi)

    def _append(self, ts, close, oi):
        self.last_ts = ts
        if oi is not None and oi > 0:
            self.last_oi, self.oi_age = oi, 0
        else:
            self.oi_age += 1
        oi_f = self.last_oi if self.oi_age <= 12 else None           # ffill(limit=12)
        self.lc.append(math.log(close) if close else None)
        self.loi.append(math.log(oi_f) if oi_f else None)
        full = len(self.lc) == LB + 1
        r4 = self.lc[-1] - self.lc[0] if full and self.lc[-1] is not None and self.lc[0] is not None else None
        d4 = self.loi[-1] - self.loi[0] if full and self.loi[-1] is not None and self.loi[0] is not None else None
        _, sd = self.rr.stats()
        z_r4 = r4 / sd if r4 is not None and sd else None
        m, sd2 = self.rd.stats()
        z_oi = (d4 - m) / sd2 if d4 is not None and sd2 else None
        self.rr.add(r4)
        self.rd.add(d4)
        cond = z_r4 is not None and z_oi is not None and z_r4 <= Z_TH and z_oi <= Z_TH
        edge = cond and not self.prev_sig
        self.prev_sig = cond
        return z_r4, z_oi, edge


# ───────────────────────────── 가상 체결 (rebound2_combo.sim 과 같은 순서) ─────────────────────────────
class Book:
    """방식·코인별 가상 장부. 백테스트처럼 '이전 매매의 모든 다리가 이 봉 시가 이전에 청산될 예정' 이면 새 진입 허용"""
    def __init__(self, name, sym, legs):
        self.name, self.sym, self.legs = name, sym, legs
        self.kavg = sum(w * k for _, k, w in legs)
        self.active = []

    @property
    def pos(self):
        return self.active[-1] if self.active else None

    def can_start(self, e):
        for P in self.active:
            if P["pend"] or any(L["x"] > e for L in P["openl"]) or any(L["xt"] > e for L in P["fb"]):
                return False
        return True

    def start(self, e, p0, sig_ts):
        openl, pend = [], []
        for kind, k, w in self.legs:
            if kind == "mkt":
                openl.append({"w": w, "pin": p0, "cin": COST_T, "ji": e, "x": e + HOLD})
            else:
                pend.append({"w": w, "lim": p0 * (1 - k)})
        self.active.append({"e": e, "p0": p0, "sig": sig_ts, "openl": openl, "pend": pend, "done": [], "fb": [],
                            "slp": p0 * (1 - self.kavg) * (1 - STOP), "mae": 0.0, "reason": "시간", "e_open": None})

    def on_bar(self, t, o, h, l):
        """닫힌 봉 t(봉 번호 = 시작시각//300) 처리. 끝난 매매 결과 목록 반환"""
        out = []
        for P in list(self.active):
            if t < P["e"]:
                continue
            if t == P["e"]:
                P["e_open"] = o
            rest = []
            for L in P["fb"]:                                        # 미뤄진 시장가 청산 = 이번 봉 시가
                if L["xt"] == t:
                    P["done"].append((L, o, COST_T, t))
                else:
                    rest.append(L)
            P["fb"] = rest
            keep = []
            for L in P["openl"]:                                     # 1) 시간청산
                if L["x"] == t:
                    if h > o * (1 + TICK):
                        P["done"].append((L, o, MAKER, t))
                    else:
                        L["xt"] = t + 1
                        P["fb"].append(L)
                else:
                    keep.append(L)
            P["openl"] = keep
            if P["pend"]:                                            # 2) 지정가 체결 (봉 e ~ e+11)
                rest = []
                for L in P["pend"]:
                    if l < L["lim"] * (1 - TICK):
                        P["openl"].append({"w": L["w"], "pin": L["lim"], "cin": MAKER, "ji": t, "x": t + 1 + HOLD})
                    else:
                        rest.append(L)
                P["pend"] = rest if t < P["e"] + WIN - 1 else []
            if P["openl"]:                                           # 3) 역행 기록 · 재난손절
                ws = sum(L["w"] for L in P["openl"])
                avg = sum(L["w"] * L["pin"] for L in P["openl"]) / ws
                P["mae"] = min(P["mae"], l / avg - 1)
                if l <= P["slp"]:
                    px = min(o, P["slp"]) * (1 - STOP_SLIP)
                    for L in P["openl"]:
                        P["done"].append((L, px, COST_T, t))
                    P["openl"], P["pend"], P["reason"] = [], [], "손절"
            if P["openl"] or P["pend"] or P["fb"]:
                continue
            self.active.remove(P)
            out.append(self._result(P) if P["done"] else {"filled": 0.0, "P": P})
        return out

    def _result(self, P):
        tot, wf, xs, legs = 0.0, 0.0, [], []
        for L, pout, cout, xt in P["done"]:
            f = funding_sum(self.sym, L["ji"] * BAR, xt * BAR)
            r = pout / L["pin"] - 1 - L["cin"] - cout - f
            tot += L["w"] * r
            wf += L["w"]
            xs.append(xt)
            legs.append({"w": round(L["w"], 4), "in": L["pin"], "in_t": L["ji"] * BAR, "out": pout, "out_t": xt * BAR, "fund": f})
        return {"filled": wf, "ret": tot, "exit_t": max(xs) * BAR, "legs": legs, "P": P}


# ───────────────────────────── 기록 ─────────────────────────────
def iso(s):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(s))


def append_csv(name, header, row):
    p = os.path.join(DATA, name)
    new = not os.path.exists(p)
    with open(p, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(header)
        w.writerow(row)


SIG_H = ["bar_utc", "coin", "close", "oi", "oi_ts_utc", "oi_wait_s", "z_r4", "z_oi4", "signal", "action"]
TR_H = ["method", "coin", "signal_bar_utc", "p0", "bar_e_open", "filled", "ret", "reason", "mae", "exit_utc", "legs"]


def log_trade(m, sym, res):
    P = res["P"]
    if res["filled"] == 0:
        append_csv("trades.csv", TR_H, [m, sym, iso(P["sig"]), P["p0"], P["e_open"], 0, "", "미체결", "", "", ""])
        log.info(f"[{m} {sym}] 미체결 (지정가 1시간 동안 안 걸림)")
        return
    append_csv("trades.csv", TR_H, [m, sym, iso(P["sig"]), P["p0"], P["e_open"], round(res["filled"], 4), round(res["ret"], 6),
                                    P["reason"], round(P["mae"], 5), iso(res["exit_t"]), json.dumps(res["legs"])])
    log.info(f"[{m} {sym}] 청산 {P['reason']} · 체결 {res['filled']*100:.0f}% · 수익 {res['ret']*100:+.3f}% (배정자본) · 최대역행 {P['mae']*100:.2f}%")


def load_oi_cache(sym):
    p = os.path.join(DATA, f"oi_{sym}.csv")
    d = {}
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            for r in csv.reader(f):
                try:
                    d[int(r[0])] = float(r[1])
                except (ValueError, IndexError):
                    pass
    return d


def save_oi(sym, items):
    p = os.path.join(DATA, f"oi_{sym}.csv")
    with open(p, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for ts, v in items:
            w.writerow([ts, v])


def stats_text():
    p = os.path.join(DATA, "trades.csv")
    if not os.path.exists(p):
        return "가상 매매 없음"
    by = {}
    with open(p, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not r["ret"]:
                by.setdefault(r["method"], []).append(None)
                continue
            by.setdefault(r["method"], []).append(float(r["ret"]))
    out = []
    for m in METHODS:
        xs = by.get(m, [])
        rr = [x for x in xs if x is not None]
        gp, gl = sum(x for x in rr if x > 0), -sum(x for x in rr if x < 0)
        pf = (gp / gl) if gl > 0 else (float("inf") if rr else float("nan"))
        out.append(f"{m}: 체결 {len(rr)} · 미체결 {len(xs)-len(rr)} · 합계 {sum(rr)*100:+.2f}% · 평균 {(sum(rr)/len(rr)*100 if rr else 0):+.3f}% · PF {pf:.2f}")
    return " | ".join(out)


# ───────────────────────────── 메인 ─────────────────────────────
class Coin:
    def __init__(self, sym):
        self.sym = sym
        self.feat = Feat()
        self.books = {m: Book(m, sym, legs) for m, legs in METHODS.items()}
        self.last_bar = None
        self.oi = load_oi_cache(sym)

    def oi_for(self, b):
        return self.oi.get(b - OI_OFFSET * BAR)

    def bootstrap(self):
        now = time.time()
        start = (int(now) // BAR) * BAR - (W30 + LB + 300) * BAR
        oi_start = max(start, int(now) - 29 * 86400)
        have = [t for t in self.oi if t >= oi_start]
        fetch_from = (max(have) + BAR) if have else oi_start
        new = oi_hist(self.sym, fetch_from, now)
        save_oi(self.sym, sorted((t, v) for t, v in new.items() if t not in self.oi))
        self.oi.update(new)
        ks = klines(self.sym, start)
        for t, o, h, l, c in ks:
            self.feat.push(t, c, self.oi_for(t))
        self.last_bar = ks[-1][0] if ks else None
        n_oi = sum(1 for t in self.oi if t >= start)
        log.info(f"[{self.sym}] 시작 기록: 5분봉 {len(ks)}개 ({iso(ks[0][0])} ~ {iso(ks[-1][0])}) · OI {n_oi}개 · OI 오프셋 {OI_OFFSET}봉")

    def wait_oi(self, b):
        t0 = time.time()
        while self.oi_for(b) is None and time.time() - t0 < OI_WAIT:
            try:
                new = oi_hist(self.sym, b - OI_OFFSET * BAR - 3 * BAR, time.time())
                fresh = [(t, v) for t, v in new.items() if t not in self.oi]
                if fresh:
                    save_oi(self.sym, sorted(fresh))
                    self.oi.update(new)
            except Exception as e:
                log.warning(f"[{self.sym}] OI 조회 실패: {e}")
            if self.oi_for(b) is None:
                time.sleep(15)
        return time.time() - t0

    def step(self):
        ks = klines(self.sym, self.last_bar + BAR)
        if not ks:
            return
        latest = ks[-1][0]
        for t, o, h, l, c in ks:
            for m, bk in self.books.items():                            # 기존 가상 포지션 먼저 (이 봉에서 체결·청산)
                for res in bk.on_bar(t // BAR, o, h, l):
                    log_trade(m, self.sym, res)
            waited = self.wait_oi(t) if t == latest else 0.0
            oi = self.oi_for(t)
            z_r4, z_oi, edge = self.feat.push(t, c, oi)
            action = ""
            if edge:
                fresh = (latest - t) // BAR < CATCHUP_MAX and time.time() - (t + BAR) < 600
                if fresh:
                    p0 = price(self.sym)
                    e = t // BAR + 1
                    started = []
                    for m, bk in self.books.items():
                        if bk.can_start(e):
                            bk.start(e, p0, t)
                            started.append(m)
                    action = "진입 " + ",".join(started) if started else "진입 안 함(보유 중)"
                    log.info(f"[{self.sym}] ★ 신호 {iso(t)} · z_r4 {z_r4:.2f} · z_oi4 {z_oi:.2f} · P0 {p0} · {action}")
                else:
                    action = "늦은 신호(건너뜀)"
                    log.info(f"[{self.sym}] 신호 {iso(t)} — 따라잡기 중이라 건너뜀")
            append_csv("signals.csv", SIG_H, [iso(t), self.sym, c, oi if oi is not None else "",
                                             iso(t - OI_OFFSET * BAR) if oi is not None else "", round(waited, 1),
                                             "" if z_r4 is None else round(z_r4, 3), "" if z_oi is None else round(z_oi, 3),
                                             int(bool(edge)), action])
        self.last_bar = latest


def setup_log():
    os.makedirs(DATA, exist_ok=True)
    h = RotatingFileHandler(os.path.join(DIR, LOGFILE), maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(h)
    if sys.stdout.isatty():                                          # nohup 실행 땐 화면 출력 안 함 (console.log 는 에러 전용)
        log.addHandler(logging.StreamHandler(sys.stdout))
    log.setLevel(logging.INFO)


def main():
    setup_log()
    if "--stats" in sys.argv or "--report" in sys.argv:
        print("=" * 70 + "\n리바운드 v2 드라이런 성적표 (가상 매매, 배정자본 기준 수익률 합)\n" + "=" * 70)
        print(stats_text().replace(" | ", "\n"))
        return
    log.info(f"리바운드 v2 드라이런 시작 (주문 없음) · 코인 {SYMS} · 방식 {list(METHODS)} · 재난손절 -{STOP*100:.0f}%")
    coins = []
    for s in SYMS:
        c = Coin(s)
        c.bootstrap()
        coins.append(c)
    last_hour = None
    while True:
        now = time.time()
        nxt = (int(now) // BAR + 1) * BAR + 5                          # 봉 마감 5초 뒤
        time.sleep(max(1, nxt - now))
        for c in coins:
            try:
                c.step()
            except Exception as e:
                log.exception(f"[{c.sym}] 사이클 오류: {e}")
        h = int(time.time()) // 3600
        if h != last_hour:
            last_hour = h
            opens = [f"{c.sym}:{m}" for c in coins for m, bk in c.books.items() if bk.active]
            zs = []
            for c in coins:
                f = c.feat
                zs.append(f"{c.sym} OI {'있음' if len(f.loi) and f.loi[-1] is not None else '없음'}")
            log.info(f"[요약] {stats_text()} · 보유 {opens or '-'} · {' · '.join(zs)}")


if __name__ == "__main__":
    main()