#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[펀딩 스퀴즈 — 드라이런]   주문 없음 · 가상 체결 · 표준 라이브러리만 · cron 으로 하루 한 번 (상주 프로세스 아님)

규칙 (백테스트 squeeze_addon.py 와 동일):
  신호일 d 의 '최근 7일 펀딩 합' = 정산 시각이 d-6 00:00 ~ d 23:59 (UTC 달력일) 인 펀딩의 합
  ≤ -0.10% 이고 그 코인 포지션이 없으면 → d+1 00:05 UTC 5분봉 시가에 가상 롱 (코인당 자금 1/4)
  7일 뒤 00:05 5분봉 시가에 청산 · 보유 중 펀딩 (진입, 청산] 반영 (롱: 양수면 냄, 음수면 받음) · 비용 왕복 0.16%
  청산한 날에 끝난 신호로는 바로 다시 들어가지 않음 (백테스트와 같은 겹침 규칙)
코인: SOLUSDT · XRPUSDT (레짐봇이 안 건드리는 코인 → 같은 계좌 상쇄 없음)

실행 방식: 매일 한국 09:07 (UTC 00:07) cron. 서버가 꺼져 있었으면 다음 실행 때 놓친 날을 하루씩 처리.
파일: squeeze_dry_state.json (상태) · squeeze_dry_daily.csv (매일 코인별 7일 합·기준까지 거리 — 백테스트 대조용)
      squeeze_dry_trades.csv (가상 매매) · squeeze_dry.log
명령: python3 squeeze_dry.py           (cron 이 부름)
      python3 squeeze_dry.py --report  (성적표)

판정 (사전 고정): 3개월 = 운영 점검 (매일 기록 · 7일 합이 백테스트 계산과 같은지)
                  매매 15번 이상(약 1년) = 평균 > 0 · PF ≥ 1.2 → 실전 소액 검토 / 평균 ≤ 0 → 정지
"""

import os, sys, json, csv, time, ssl, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone

SYMS = ["SOLUSDT", "XRPUSDT"]
TH, LOOK, HOLD, W, COST = -0.0010, 7, 7, 0.25, 0.0016
FAPI = "https://fapi.binance.com"
DIR = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(DIR, "squeeze_dry_state.json")
DAILY = os.path.join(DIR, "squeeze_dry_daily.csv")
TRADES = os.path.join(DIR, "squeeze_dry_trades.csv")
LOG = os.path.join(DIR, "squeeze_dry.log")
UTC = timezone.utc
CTX = None


def log(msg):
    line = f"{datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC {msg}"
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    if sys.stdout.isatty():
        print(line)


def http(path, params):
    global CTX
    if CTX is None:
        CTX = ssl.create_default_context()
    url = f"{FAPI}{path}?{urllib.parse.urlencode(params)}"
    last = None
    for k in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "squeeze-dry"}), timeout=20, context=CTX) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e
            time.sleep(2 * (k + 1))
    raise last


def ms(dt):
    return int(dt.timestamp() * 1000)


def day0(dt):
    return datetime(dt.year, dt.month, dt.day, tzinfo=UTC)


def fundings(sym, start, end):
    """[(시각, 비율)] — start ≤ 시각 < end"""
    out, cur = [], ms(start)
    for _ in range(20):
        j = http("/fapi/v1/fundingRate", {"symbol": sym, "startTime": cur, "endTime": ms(end) - 1, "limit": 1000})
        got = [(int(r["fundingTime"]), float(r["fundingRate"])) for r in j if cur <= int(r["fundingTime"]) < ms(end)]
        out += got
        if len(j) < 1000 or not got:
            break
        nxt = max(t for t, _ in got) + 1
        if nxt <= cur:
            break
        cur = nxt
    return sorted(set(out))


def open_0005(sym, day):
    t = day + timedelta(minutes=5)
    j = http("/fapi/v1/klines", {"symbol": sym, "interval": "5m", "startTime": ms(t), "limit": 1})
    if j and int(j[0][0]) == ms(t):
        return float(j[0][1])
    return None


def append_csv(path, header, row):
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(header)
        w.writerow(row)


def load_state():
    if os.path.exists(STATE):
        return json.load(open(STATE, encoding="utf-8"))
    y = day0(datetime.now(UTC)) - timedelta(days=1)
    return {"last_signal_day": (y - timedelta(days=1)).strftime("%Y-%m-%d"), "pos": {}, "last_exit": {}}


def save_state(s):
    tmp = STATE + ".tmp"
    json.dump(s, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    os.replace(tmp, STATE)


def process_day(st, d):
    """신호일 d (UTC 날짜) 처리 — d+1 00:05 가 이미 지났을 때만 부름"""
    e = d + timedelta(days=1)                                         # 진입/청산 시각 기준일 (00:05)
    for sym in SYMS:
        # 1) 청산: 오늘(e)이 만기면 e 00:05 시가로
        p = st["pos"].get(sym)
        if p and e.strftime("%Y-%m-%d") >= p["exit_day"]:
            px = open_0005(sym, e)
            if px is None:
                log(f"[{sym}] 청산가(00:05) 못 받음 — 다음 실행 때 다시")
                return False
            ent = datetime.strptime(p["entry_day"], "%Y-%m-%d").replace(tzinfo=UTC) + timedelta(minutes=5)
            f = sum(r for _, r in fundings(sym, ent + timedelta(milliseconds=1), e + timedelta(minutes=5, milliseconds=1)))
            ret = px / p["entry_px"] - 1 - f - COST
            append_csv(TRADES, ["coin", "signal_day", "entry_day", "entry_px", "exit_day", "exit_px", "s7", "funding", "ret"],
                       [sym, p["signal_day"], p["entry_day"], p["entry_px"], e.strftime("%Y-%m-%d"), px, p["s7"], round(f, 6), round(ret, 6)])
            log(f"[{sym}] 청산 · {p['entry_px']} → {px} · 펀딩 {f*100:+.3f}% · 수익 {ret*100:+.2f}% (코인 자금 기준)")
            st["last_exit"][sym] = e.strftime("%Y-%m-%d")
            del st["pos"][sym]
        # 2) 신호: d 의 7일 합
        fs = fundings(sym, d - timedelta(days=LOOK - 1), d + timedelta(days=1))
        s7 = sum(r for _, r in fs)
        action = "-"
        busy = st["last_exit"].get(sym, "0000-00-00")
        if sym in st["pos"]:
            action = "보유 중"
        elif s7 <= TH:
            if d.strftime("%Y-%m-%d") < busy:
                action = "신호(직전 청산과 겹쳐 건너뜀)"
            else:
                px = open_0005(sym, e)
                if px is None:
                    log(f"[{sym}] 진입가(00:05) 못 받음 — 다음 실행 때 다시")
                    return False
                st["pos"][sym] = {"signal_day": d.strftime("%Y-%m-%d"), "entry_day": e.strftime("%Y-%m-%d"), "entry_px": px,
                                  "exit_day": (e + timedelta(days=HOLD)).strftime("%Y-%m-%d"), "s7": round(s7, 6)}
                action = "★ 가상 롱"
                log(f"[{sym}] ★ 신호 {d:%Y-%m-%d} · 7일 펀딩 합 {s7*100:+.3f}% → {e:%Y-%m-%d} 00:05 가상 롱 {px} · 청산 예정 {st['pos'][sym]['exit_day']}")
        append_csv(DAILY, ["signal_day", "coin", "s7", "settlements", "gap_to_th", "action"],
                   [d.strftime("%Y-%m-%d"), sym, round(s7, 6), len(fs), round(s7 - TH, 6), action])
    return True


def run():
    st = load_state()
    now = datetime.now(UTC)
    last_ready = day0(now) - timedelta(days=1)                         # 오늘 00:05 가 지났으면 어제가 마지막 신호일
    if now < day0(now) + timedelta(minutes=6):
        last_ready -= timedelta(days=1)
    d = datetime.strptime(st["last_signal_day"], "%Y-%m-%d").replace(tzinfo=UTC) + timedelta(days=1)
    n = 0
    while d <= last_ready and n < 30:
        if not process_day(st, d):
            break
        st["last_signal_day"] = d.strftime("%Y-%m-%d")
        save_state(st)
        d += timedelta(days=1)
        n += 1
    if n == 0:
        log("처리할 날 없음")
    else:
        pos = ", ".join(f"{k}(~{v['exit_day']})" for k, v in st["pos"].items()) or "-"
        log(f"{n}일 처리 · 보유 {pos}")


def report():
    print("=" * 72)
    print("펀딩 스퀴즈 드라이런 성적표 (SOL·XRP · 7일 펀딩 합 ≤ -0.10% → 7일 롱)")
    print("=" * 72)
    st = load_state() if os.path.exists(STATE) else None
    if st:
        print(f"마지막 처리 신호일: {st['last_signal_day']} · 보유: " +
              (", ".join(f"{k} 진입 {v['entry_day']} @{v['entry_px']} (청산 예정 {v['exit_day']})" for k, v in st["pos"].items()) or "-"))
    if os.path.exists(DAILY):
        rows = list(csv.DictReader(open(DAILY, encoding="utf-8")))
        last = {}
        for r in rows:
            last[r["coin"]] = r
        for c, r in last.items():
            print(f"  {c}: {r['signal_day']} 7일 펀딩 합 {float(r['s7'])*100:+.3f}% (기준 -0.100% 까지 {float(r['gap_to_th'])*100:+.3f}%p) · 정산 {r['settlements']}번")
        print(f"  일별 기록 {len(rows)//len(SYMS)}일")
    rets = []
    if os.path.exists(TRADES):
        for r in csv.DictReader(open(TRADES, encoding="utf-8")):
            rets.append(float(r["ret"]))
            print(f"  매매 {r['coin']} {r['entry_day']} → {r['exit_day']} · 7일 합 {float(r['s7'])*100:+.3f}% · 펀딩 {float(r['funding'])*100:+.3f}% · {float(r['ret'])*100:+.2f}%")
    if rets:
        gp, gl = sum(x for x in rets if x > 0), -sum(x for x in rets if x < 0)
        pf = gp / gl if gl > 0 else float("inf")
        print(f"\n가상 매매 {len(rets)}번 · 평균 {sum(rets)/len(rets)*100:+.2f}% · PF {pf:.2f} · 합계(코인 자금 기준) {sum(rets)*100:+.1f}% · 계좌 기여(×1/4) {sum(rets)*W*100:+.1f}%")
    else:
        print("\n가상 매매 없음")
    print("\n[백테스트 기대(SOL·XRP, 2020-10~2026-09)] 연 14번 · 평균 +3.9% · PF 2.18 — 블랙박스 없음, 부풀려진 값으로 볼 것")
    print("[판정] 3개월: 운영 점검 / 15번 이상: 평균 > 0 · PF ≥ 1.2 → 실전 소액 검토, 평균 ≤ 0 → 정지")


if __name__ == "__main__":
    if "--report" in sys.argv:
        report()
    else:
        try:
            run()
        except Exception as e:
            log(f"오류: {type(e).__name__}: {e}")
            raise
