#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[오더북 신호 점검 — 최근 N시간 동안 신호에 얼마나 가까웠나]   ※ 서버에서 ob_bot.py 옆에서 실행

봇과 같은 방식으로 5분봉마다 다시 계산:
  거래량 배수 = 그 봉 거래량 ÷ 직전 288봉 평균     (조건: ≥ 3)
  호가 z      = 그 봉 끝 ±1% 불균형의 30일 z점수   (조건: ≤ -2, ob_imb_5m.csv 이력 사용)
출력: 조건별 충족 봉 수, 둘 다 충족한 봉, 거래량이 가장 컸던 봉 10개와 그때 z
실행: venv/bin/python ob_check.py 48      (기본 24시간)
"""

import os, sys
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import numpy as np
import pandas as pd
import ob_bot as ob


def main():
    hours = float(sys.argv[1]) if len(sys.argv) > 1 else 24
    now = int(pd.Timestamp.now(tz="UTC").value // 10 ** 6)
    start = (now - int(hours * 3_600_000)) // ob.BAR_MS * ob.BAR_MS
    bars, s = [], start - (ob.VOL_WIN + 5) * ob.BAR_MS
    while s < now:
        b = ob.klines(s)
        if not b:
            break
        bars += b; s = b[-1]["ts"] + ob.BAR_MS
        if len(b) < 1000:
            break
    K = pd.DataFrame(bars).drop_duplicates("ts").set_index("ts").sort_index()
    K = K[K.index + ob.BAR_MS <= now]
    hist = ob.load_imb_history()
    H = pd.Series(hist).sort_index()
    rows = []
    for ts in K.index[K.index >= start]:
        end = ts + ob.BAR_MS
        prev = K.loc[(K.index < ts) & (K.index >= ts - ob.VOL_WIN * ob.BAR_MS), "volume"]
        vr = K.at[ts, "volume"] / prev.mean() if len(prev) >= 100 else np.nan
        w = H[(H.index > end - ob.Z_WIN * ob.BAR_MS) & (H.index <= end)].dropna()
        zv = (w.iloc[-1] - w.mean()) / w.std() if (end in H.index and len(w) >= ob.Z_MIN and w.std() > 0) else np.nan
        rows.append(dict(시각=pd.Timestamp(end, unit="ms", tz="Asia/Seoul").strftime("%m-%d %H:%M"),
                         가격=K.at[ts, "close"], 봉=("상승" if K.at[ts, "close"] > K.at[ts, "open"] else "하락"),
                         거래량배수=vr, 호가z=zv))
    R = pd.DataFrame(rows)
    pd.set_option("display.unicode.east_asian_width", True)
    print(f"최근 {hours:g}시간 오더북 신호 점검 — XRPUSDT (조건: 거래량 ≥ {ob.VOL_MULT}배 & 호가 z ≤ {ob.Z_THR})")
    print("=" * 80)
    print(f"  5분봉 {len(R)}개 · 호가 z 계산된 봉 {R['호가z'].notna().sum()}개 (없는 봉 = 그 시각 실시간 호가 기록이 없음)")
    print(f"  거래량 3배 이상: {int((R['거래량배수'] >= ob.VOL_MULT).sum())}봉 · 호가 z -2 이하: {int((R['호가z'] <= ob.Z_THR).sum())}봉 · "
          f"가장 낮은 z: {R['호가z'].min():.2f}")
    both = R[(R["거래량배수"] >= ob.VOL_MULT) & (R["호가z"] <= ob.Z_THR)]
    print(f"  ▶ 둘 다 충족: {len(both)}봉" + ("" if len(both) == 0 else " ← 봇 로그에 ★ 신호가 있어야 함"))
    if len(both):
        print(both.to_string(index=False))
    print("\n  거래량이 가장 컸던 봉 10개 (그때 호가 z)")
    top = R.sort_values("거래량배수", ascending=False).head(10).copy()
    top["거래량배수"] = top["거래량배수"].map(lambda v: f"{v:.1f}배"); top["호가z"] = top["호가z"].map(lambda v: f"{v:.2f}" if v == v else "-")
    print(top.to_string(index=False))
    print("\n  [읽는 법] 거래량은 터졌는데 z 가 -2 근처도 안 가면 → 매도 호가가 두껍지 않았던 것(매수 호가도 버텼음) = 설계상 안 산 것")


if __name__ == "__main__":
    main()
