#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[김프 K5 드라이런 — cron 하루 1회 · 표준 라이브러리만 (pandas 없음, 실행 중 ~20MB, 상주 0)]

규칙 (백테스트 kimchi_pooled.py 의 K5 · 3일과 같음)
  김프 = 업비트 원화 일봉 종가 ÷ (바이낸스 USDT 무기한 일봉 종가 × 원/달러 환율) − 1   (UTC 0시 마감)
  신호: 어제 마감 김프 − 3일 전 마감 김프 ≥ +2%p  → 바이낸스 롱 (가상), 코인당 자금 1/4
  청산: 진입 3일 뒤 실행 때 현재가로 · 비용 왕복 0.10% · 실제 펀딩(롱 지불 / 음수면 수령)
  코인: BTC · ETH · SOL · XRP (코인별 한 번에 한 포지션)
실행: 매일 UTC 0시 5분 (한국 9시 5분) — cron
  5 0 * * *  (서버가 UTC 일 때)   /  5 9 * * *  (서버가 KST 일 때)
  cd ~/upbit && venv/bin/python kimchi_dry.py >> kimchi_dry.log 2>&1
성적표: venv/bin/python kimchi_dry.py --stats
파일: kimchi_dry_state.json (포지션·장부) · kimchi_dry_trades.jsonl (매매) · kimchi_dry_daily.jsonl (매일 김프 기록)
"""

import os, sys, json, time, math, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

os.chdir(os.path.dirname(os.path.abspath(__file__)))
COINS = ["BTC", "ETH", "SOL", "XRP"]
TH, HOLD_D, W, COST = 0.02, 3, 0.25, 0.0010
START_EQ = 1000.0
STATE, TRADES, DAILY = "kimchi_dry_state.json", "kimchi_dry_trades.jsonl", "kimchi_dry_daily.jsonl"


def log(msg):
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z {msg}", flush=True)


def get(url, tries=3):
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e; time.sleep(2 * (k + 1))
    raise RuntimeError(f"요청 실패 {url[:60]}: {last}")


def upbit_closes(coin):
    j = get("https://api.upbit.com/v1/candles/days?" + urllib.parse.urlencode({"market": f"KRW-{coin}", "count": 8}))
    return {x["candle_date_time_utc"][:10]: float(x["trade_price"]) for x in j}


def binance_closes(coin):
    j = get("https://fapi.binance.com/fapi/v1/klines?" + urllib.parse.urlencode({"symbol": f"{coin}USDT", "interval": "1d", "limit": 8}))
    now_ms = int(time.time() * 1000)
    return {datetime.fromtimestamp(k[0] / 1000, timezone.utc).strftime("%Y-%m-%d"): float(k[4]) for k in j if k[6] < now_ms}


def binance_price(coin):
    return float(get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={coin}USDT")["price"])


def fx_closes():
    for host in ("query2", "query1"):
        try:
            j = get(f"https://{host}.finance.yahoo.com/v8/finance/chart/KRW=X?range=1mo&interval=1d")
            res = j["chart"]["result"][0]
            out = {}
            for ts, c in zip(res["timestamp"], res["indicators"]["quote"][0]["close"]):
                if c:
                    out[datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")] = float(c)
            if out:
                return out
        except Exception as e:
            last = e
    raise RuntimeError(f"환율 실패: {last}")


def fx_on(fx, day):
    d = datetime.strptime(day, "%Y-%m-%d")
    for k in range(7):                                       # 주말·휴일은 직전 값
        key = (d - timedelta(days=k)).strftime("%Y-%m-%d")
        if key in fx:
            return fx[key]
    return None


def funding_sum(coin, t0_ms, t1_ms):
    j = get("https://fapi.binance.com/fapi/v1/fundingRate?" + urllib.parse.urlencode({"symbol": f"{coin}USDT", "startTime": t0_ms + 1, "endTime": t1_ms, "limit": 100}))
    return sum(float(x["fundingRate"]) for x in j)


def load_state():
    if os.path.exists(STATE):
        return json.load(open(STATE, encoding="utf-8"))
    return {"equity": START_EQ, "positions": {}, "started": datetime.now(timezone.utc).isoformat()}


def save_state(st):
    tmp = STATE + ".tmp"
    json.dump(st, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    os.replace(tmp, STATE)


def append(path, rec):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def run():
    st = load_state()
    now = datetime.now(timezone.utc); now_ms = int(now.timestamp() * 1000)
    today = now.strftime("%Y-%m-%d")
    # 1) 만기 청산
    for coin, p in list(st["positions"].items()):
        if p["exit_due"] <= today:
            px = binance_price(coin)
            try:
                fsum = funding_sum(coin, p["entry_ms"], now_ms)
            except Exception as e:
                fsum = 0.0003 * HOLD_D; log(f"[경고] {coin} 펀딩 조회 실패 → 가정치 {fsum*100:.3f}% ({e})")
            ret = px / p["entry_px"] - 1 - COST - fsum
            pnl = st["equity"] * W * ret if p.get("eq_at_entry") is None else p["eq_at_entry"] * W * ret
            st["equity"] += pnl
            rec = {"coin": coin, "entry": p["entry_day"], "exit": today, "entry_px": p["entry_px"], "exit_px": px,
                   "funding": round(fsum, 6), "ret": round(ret, 6), "pnl": round(pnl, 4), "equity": round(st["equity"], 4),
                   "group": "BTC·ETH" if coin in ("BTC", "ETH") else "SOL·XRP"}
            append(TRADES, rec); del st["positions"][coin]
            log(f"[청산] {coin} {p['entry_px']:.4f} → {px:.4f} | 펀딩 {fsum*100:+.3f}% | 수익 {ret*100:+.2f}% ({pnl:+.2f}) | 장부 {st['equity']:.2f}")
    save_state(st)
    # 2) 김프 계산 (어제 마감 기준)
    fx = fx_closes()
    d0 = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    d3 = (now - timedelta(days=4)).strftime("%Y-%m-%d")
    rows = {}
    for coin in COINS:
        try:
            u, b = upbit_closes(coin), binance_closes(coin)
            def prem(day):
                f = fx_on(fx, day)
                return (u[day] / (b[day] * f) - 1) if (day in u and day in b and f) else None
            p0, p3 = prem(d0), prem(d3)
            rows[coin] = (p0, p3)
            if p0 is None or p3 is None:
                log(f"[경고] {coin} 김프 계산 불가 (데이터 없음: {d0}/{d3})"); continue
            ch = p0 - p3
            hit = ch >= TH
            log(f"[김프] {coin} {d0} {p0*100:+.2f}% · 3일 전 {p3*100:+.2f}% · 변화 {ch*100:+.2f}%p (기준 +{TH*100:.0f}%p까지 {max(TH-ch,0)*100:.2f}%p){' → 신호' if hit else ''}")
            append(DAILY, {"day": d0, "coin": coin, "prem": round(p0, 6), "prem_3d_ago": round(p3, 6), "change": round(ch, 6),
                           "fx": fx_on(fx, d0), "upbit": u.get(d0), "binance": b.get(d0), "signal": hit})
            # 3) 진입
            if hit and coin not in st["positions"]:
                px = binance_price(coin)
                due = (now + timedelta(days=HOLD_D)).strftime("%Y-%m-%d")
                st["positions"][coin] = {"entry_day": today, "entry_px": px, "entry_ms": now_ms, "exit_due": due, "eq_at_entry": st["equity"]}
                log(f"[진입] {coin} 가상 롱 @ {px:.4f} (자금 1/4) · 청산 예정 {due}")
            elif hit:
                log(f"[보류] {coin} 신호지만 이미 보유 중")
        except Exception as e:
            log(f"[에러] {coin}: {e}")
    save_state(st)
    held = ", ".join(f"{c}(→{p['exit_due']})" for c, p in st["positions"].items()) or "없음"
    log(f"[요약] 장부 {st['equity']:.2f} (시작 {START_EQ:.0f}, {(st['equity']/START_EQ-1)*100:+.2f}%) · 보유 {held}")


def stats():
    st = load_state()
    T = [json.loads(l) for l in open(TRADES, encoding="utf-8")] if os.path.exists(TRADES) else []
    print("=" * 70); print("김프 K5 드라이런 성적표"); print("=" * 70)
    print(f"장부: {st['equity']:.2f} (시작 {START_EQ:.0f} → {(st['equity']/START_EQ-1)*100:+.2f}%) · 시작 {st.get('started','')[:10]}")
    print("보유: " + (", ".join(f"{c} @{p['entry_px']:.4f} (청산 {p['exit_due']})" for c, p in st["positions"].items()) or "없음"))
    def summ(lab, xs):
        if not xs:
            print(f"  {lab:8s}: 매매 없음"); return
        r = [x["ret"] for x in xs]; w = sum(v for v in r if v > 0); l = -sum(v for v in r if v <= 0)
        pf = w / l if l > 0 else float("inf")
        print(f"  {lab:8s}: {len(r)}회 · 승률 {sum(v > 0 for v in r)/len(r)*100:.0f}% · 평균 {sum(r)/len(r)*100:+.2f}% · PF {pf:.2f}")
    print("\n[성적]")
    summ("전체", T); summ("BTC·ETH", [x for x in T if x["group"] == "BTC·ETH"]); summ("SOL·XRP", [x for x in T if x["group"] == "SOL·XRP"])
    for c in COINS:
        summ(c, [x for x in T if x["coin"] == c])
    if os.path.exists(DAILY):
        D = [json.loads(l) for l in open(DAILY, encoding="utf-8")]
        last = {}
        for x in D:
            last[x["coin"]] = x
        print("\n[최근 김프]")
        for c in COINS:
            if c in last:
                x = last[c]
                print(f"  {c}: {x['day']} {x['prem']*100:+.2f}% · 3일 변화 {x['change']*100:+.2f}%p (기준 +{TH*100:.0f}%p)")
    print("\n[백테스트 기대치] 발견 구간 PF 1.73 · 블랙박스(2026) PF 1.40 · 4개 합산 연 30~50회 · 조용한 시기엔 한 달 2~3회")
    print("[판단] 김프 엣지는 '한국 열풍'에 따라 강해졌다 약해짐 — 조용한 시기의 성적만으로 판단하지 말 것")


if __name__ == "__main__":
    if "--stats" in sys.argv:
        stats()
    else:
        try:
            run()
        except Exception as e:
            log(f"[치명] {e}")
            sys.exit(1)
