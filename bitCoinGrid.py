#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
[KRW-SOL Grid v4.2 - RaceFix]

v4.1 대비 변경점:
 ★ (I) 매도 체결/정합 경합 수정: reconcile 이 매도 항목을 삭제하기 전에 주문 상세를
       조회해, 체결(done)된 매도면 손익 정산(trade_success_reconciled) 후 삭제.
       + check_fill 을 reconcile 앞으로 이동(체결 정산 우선) → 체결이 정산 없이
       증발하고 그 자리에 매수가 깔리던 경합 창 제거.
 ★ (J) 재고 상한 재확인: 매수 배치 루프 '매 회차'마다 상한 체크. 한 사이클에
       여러 레이어를 깔 때 상한을 초과하던 구멍 봉쇄.

v4.0 대비 변경점:
 ★ (G) 재고 상한을 '시가 평가액' → '투입 원가' 기준으로 변경.
       - 판정식: 매도벽의 매수원가 합(buy_price×vol) + 미체결 매수 예약액 >= MAX_INVENTORY_KRW
         → 신규 매수 중단. 매도는 계속.
       - 가격이 빠져도 한도가 늘어나지 않음(순환역행 차단). API 호출 없이 수첩만으로 계산.
       - "30만원 × N개 = 총투입 상한" 직관과 동일하되, BUY_AMOUNT 변경·부분체결에도 정확.
 ★ (H) 그리드 상향 트레일링 시 최하단 매수 취소 로그 추가(prune_lowest).

v3.3.2 대비 변경점 (재설계):
 ★ (A) DRY_RUN 모드 신설: 공개 시세(웹소켓) + 가상 체결/가상 잔고. 키 불필요, 로컬 실행용.
       - 상태 파일 분리(dry_*.json) → 라이브 state 절대 안 건드림.
 ★ (B) 예산 분리: 주문 '총개수' 상한(구 MAX_OPEN_ORDERS=70) 폐지.
       - 매수: MAX_LAYERS(5) + KRW 잔고 + 게이트 + 모드로만 제어.
       - 재고(매도벽): MAX_INVENTORY_KRW(명시적 KRW 상한)로 제어.
       → 하락으로 매도벽이 쌓여도 '매수 엔진'이 질식하지 않음(낮은 자리 횡보에서 계속 회전).
 ★ (C) MAX_INVENTORY_KRW: 죽어있던 MAX_INVENTORY를 실제 재고 상한(KRW 평가액)으로 실체화.
       - 총 SOL 보유(주문 잠김 포함) × 현재가 >= 상한 → 신규 매수 중단. 매도는 계속.
 ★ (D) 추세 게이트: 가격 vs 30일 이평 ±1% 히스테리시스.
       - 이평 +1% 위 돌파 → OPEN(매수 허용) / -1% 아래 이탈 → CLOSED(신규 매수만 중단).
       - 기존 미체결 매수·매도벽은 건드리지 않음. 모드 시스템과 AND 결합(더 보수적인 쪽 우선).
       - 게이트 상태는 state에 저장(재시작 시 유지).
 ★ (E) 버그 수정:
       - 웹소켓 좀비: 연속 에러 5회 시 WebSocketManager 재생성.
       - orphan 탐지: get_balance(가용)이 아닌 총보유(가용+주문잠김) 기준으로 수정.
       - reconcile 의 bare `except: pass` → 로그 남김.
       - check_fill 의 "체결 시 mode=NORMAL 리셋" 삭제(decide_mode가 4초마다 덮어써 무의미
         + mode_change 오탐으로 reset_buy_grid 오발 유발).
 ★ (F) 킬스위치: 사용자 결정으로 미포함(손절 없음 유지). 재고 상한+게이트가 '증식 방지'만 담당.

[유지] 모드 시스템(24h MDD+체결강도), startup_purge, self-healing sync, orphan sniper(3%),
       근접가드, 원자적 상태저장, Daily Sales Log.

[실행]
 - 드라이런(로컬):  기본값. 그냥 실행. (SIM_START_KRW 가상잔고로 시작)
 - 라이브(EC2):     export DRY_RUN=false + UPBIT_ACCESS_KEY/SECRET_KEY 필요.
 ※ MAX_INVENTORY_KRW 는 반드시 본인 예산에 맞게 조정할 것.
"""

import os
import time
import json
import threading
import traceback
from decimal import Decimal, getcontext
from datetime import datetime
from collections import deque
import pyupbit
from pyupbit import WebSocketManager

getcontext().prec = 28

# ==========================================
# [사용자 설정 영역] - Config
# ==========================================
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() not in ("false", "0", "no")

# ⚠️ 키를 코드에 직접 넣지 마세요. 환경변수로만 주입하세요. (드라이런은 키 불필요)
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "")

SYMBOL           = "KRW-SOL"
COIN             = SYMBOL.split("-")[1]
BUY_AMOUNT_KRW   = Decimal("300000")
BASE_GRID_GAP    = Decimal("0.004")
PROFIT_PCT       = Decimal("0.005")

PROXIMITY_KRW    = Decimal("350")

# ★ (C)(G) 재고 상한 [투입 원가 기준]: 재고 매수원가 + 미체결 매수 예약액이
#      이 값 이상이면 신규 매수 중단. 매도는 계속.
#      기본값 2,100만 = 구버전(70개×30만)의 암묵적 상한과 동일.
#      ※ 이 숫자는 예산 재검토(총 코인배분 → 그리드 몫) 때 반드시 다시 정할 것.
MAX_INVENTORY_KRW = Decimal("21000000")

# ★ (D) 추세 게이트
GATE_MA_DAYS     = 30                  # 일봉 이평 기간
GATE_BAND        = Decimal("0.01")     # 히스테리시스 ±1%
GATE_REFRESH_SEC = 1800                # 이평 갱신 주기(30분)

VP_WINDOW_SEC      = 1800
MIN_TOTAL_VOL_KRW  = 1000000000

CAUTION_MDD      = Decimal("-0.05")
CAUTION_VP       = Decimal("40.0")
DEFENSIVE_MDD    = Decimal("-0.10")
DEFENSIVE_VP     = Decimal("25.0")
FREEZE_MDD       = Decimal("-0.20")
FREEZE_VP        = Decimal("10.0")

MAX_LAYERS       = 5

# 드라이런 가상 시작 잔고
SIM_START_KRW    = Decimal("3000000")

STATE_FILE       = "dry_grid_state.json" if DRY_RUN else "grid_state.json"
SIM_FILE         = "dry_sim_exchange.json"

WS_ERR_RESTART   = 5                   # ★ (E) 연속 에러 N회 → 웹소켓 재생성
FEE              = Decimal("0.0005")   # 업비트 0.05%
# ==========================================

def logj(ev, **kwargs):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(json.dumps({"ts": now, "ev": ev, **kwargs}, ensure_ascii=False), flush=True)

def adjust_price(price):
    p = Decimal(str(price))
    if p >= 100000: tick = Decimal("100")
    elif p >= 10000: tick = Decimal("10")
    else: tick = Decimal("1")
    return (p / tick).to_integral_value(rounding='ROUND_FLOOR') * tick


# ==========================================
# ★ (A) 가상 거래소: pyupbit.Upbit 와 동일 시그니처의 서브셋 구현
#   - 지정가 매수: 현재가 <= 주문가 → 체결 / 지정가 매도: 현재가 >= 주문가 → 체결
#   - 수수료: 매수 시 KRW 5bp 추가 차감, 매도 시 수익금 5bp 차감(라이브 손익식과 동일)
# ==========================================
class SimUpbit:
    def __init__(self):
        self.krw_avail = SIM_START_KRW
        self.krw_locked = Decimal("0")
        self.coin_avail = Decimal("0")
        self.coin_locked = Decimal("0")
        self.orders = {}                 # uuid -> {state:'wait'|'done'|'cancel', ...}
        self.done_recent = deque(maxlen=200)
        self._seq = 0
        self._load()

    # ---- 영속화 ----
    def _load(self):
        if not os.path.exists(SIM_FILE): return
        try:
            with open(SIM_FILE, encoding="utf-8") as f: d = json.load(f)
            self.krw_avail = Decimal(d["krw_avail"]); self.krw_locked = Decimal(d["krw_locked"])
            self.coin_avail = Decimal(d["coin_avail"]); self.coin_locked = Decimal(d["coin_locked"])
            self.orders = {u: o for u, o in d.get("orders", {}).items()}
            self._seq = d.get("seq", 0)
            self.done_recent = deque(d.get("done_recent", []), maxlen=200)
        except Exception:
            logj("err_sim_load", trace=traceback.format_exc())

    def save(self):
        try:
            d = {"krw_avail": str(self.krw_avail), "krw_locked": str(self.krw_locked),
                 "coin_avail": str(self.coin_avail), "coin_locked": str(self.coin_locked),
                 "orders": self.orders, "seq": self._seq,
                 "done_recent": list(self.done_recent)}
            tmp = SIM_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f: json.dump(d, f, indent=2)
            os.replace(tmp, SIM_FILE)
        except Exception:
            logj("err_sim_save", trace=traceback.format_exc())

    def _uuid(self):
        self._seq += 1
        return f"sim-{self._seq:08d}"

    # ---- 체결 엔진: 매 틱 호출 ----
    def on_price(self, price):
        price = Decimal(str(price))
        for uid, o in list(self.orders.items()):
            if o["state"] != "wait": continue
            p = Decimal(o["price"]); v = Decimal(o["remaining_volume"])
            if o["side"] == "bid" and price <= p:
                lock = p * v * (Decimal("1") + FEE)
                self.krw_locked -= lock
                self.coin_avail += v
                o.update(state="done", executed_volume=str(v), remaining_volume="0")
                self.done_recent.append(uid)
                logj("SIM_FILL", side="bid", price=str(p), vol=str(v))
            elif o["side"] == "ask" and price >= p:
                self.coin_locked -= v
                self.krw_avail += p * v * (Decimal("1") - FEE)
                o.update(state="done", executed_volume=str(v), remaining_volume="0")
                self.done_recent.append(uid)
                logj("SIM_FILL", side="ask", price=str(p), vol=str(v))

    # ---- pyupbit 호환 API ----
    def buy_limit_order(self, sym, price, volume):
        p = Decimal(str(price)); v = Decimal(str(volume))
        lock = p * v * (Decimal("1") + FEE)
        if self.krw_avail < lock:
            logj("SIM_REJECT", reason="krw_insufficient"); return None
        self.krw_avail -= lock; self.krw_locked += lock
        uid = self._uuid()
        self.orders[uid] = {"uuid": uid, "market": sym, "side": "bid", "state": "wait",
                            "price": str(p), "volume": str(v),
                            "remaining_volume": str(v), "executed_volume": "0"}
        return {"uuid": uid}

    def sell_limit_order(self, sym, price, volume):
        p = Decimal(str(price)); v = Decimal(str(volume))
        if self.coin_avail < v:
            logj("SIM_REJECT", reason="coin_insufficient"); return None
        self.coin_avail -= v; self.coin_locked += v
        uid = self._uuid()
        self.orders[uid] = {"uuid": uid, "market": sym, "side": "ask", "state": "wait",
                            "price": str(p), "volume": str(v),
                            "remaining_volume": str(v), "executed_volume": "0"}
        return {"uuid": uid}

    def cancel_order(self, uid):
        o = self.orders.get(uid)
        if not o or o["state"] != "wait": return None
        v = Decimal(o["remaining_volume"]); p = Decimal(o["price"])
        if o["side"] == "bid":
            lock = p * v * (Decimal("1") + FEE)
            self.krw_locked -= lock; self.krw_avail += lock
        else:
            self.coin_locked -= v; self.coin_avail += v
        o["state"] = "cancel"
        return {"uuid": uid}

    def get_order(self, ticker_or_uuid, state=None):
        if isinstance(ticker_or_uuid, str) and ticker_or_uuid.startswith("KRW-"):
            if state == "wait":
                return [o for o in self.orders.values() if o["state"] == "wait"]
            if state == "done":
                return [self.orders[u] for u in self.done_recent if u in self.orders]
            return []
        return self.orders.get(ticker_or_uuid, {"executed_volume": "0"})

    def get_balance(self, ticker):
        if ticker == "KRW": return float(self.krw_avail)
        return float(self.coin_avail)

    def get_balances(self):
        return [{"currency": "KRW", "balance": str(self.krw_avail), "locked": str(self.krw_locked)},
                {"currency": COIN, "balance": str(self.coin_avail), "locked": str(self.coin_locked)}]


# ==========================================
# 봇 본체
# ==========================================
class GridBotV4:
    def __init__(self):
        if DRY_RUN:
            self.upbit = SimUpbit()
            logj("mode", run="DRY_RUN", sim_krw=str(SIM_START_KRW))
        else:
            if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
                raise SystemExit("환경변수 UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY 필요 (라이브)")
            self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
            logj("mode", run="LIVE")
        self.lock = threading.RLock()
        self.grid_map = {}
        self.current_price = Decimal("0")

        self.current_mode = "NORMAL"
        self.last_mode = "NORMAL"
        self.rolling_24h_high = Decimal("0")
        self.last_high_update = 0
        self.last_reconcile_time = 0
        self.trade_history = deque()
        self.cumulative_net_profit = Decimal("0")

        self.daily_sale_count = 0
        self.last_sale_date = datetime.now().strftime("%Y-%m-%d")

        self.orphan_baseline = None

        # ★ (D) 게이트 상태
        self.gate_open = False           # 최초 기동은 닫힘 → 첫 이평 계산 후 판정
        self.gate_ma = Decimal("0")
        self.last_gate_update = 0
        self.last_gate_log = 0

        self.load_state()
        self.startup_purge()

    # ---------- 총보유(가용+잠김) : ★ (E) orphan/재고 계산용 ----------
    def get_total_coin(self):
        try:
            for b in (self.upbit.get_balances() or []):
                if b.get("currency") == COIN:
                    return Decimal(str(b.get("balance", "0"))) + Decimal(str(b.get("locked", "0")))
        except Exception:
            logj("err_total_coin", trace=traceback.format_exc())
        return Decimal("0")

    def inventory_cost_krw(self):
        """★ (G) 재고 원가(매도벽의 매수원가 합) + 미체결 매수 예약액.
        가격 변동에 불변 → 하락 시 한도가 '늘어나는' 순환역행 없음. API 호출 불필요."""
        with self.lock:
            ask_cost = sum(i['buy_price'] * i['volume']
                           for i in self.grid_map.values() if i['side'] == 'ask')
            bid_committed = sum(i['buy_price'] * i['volume']
                                for i in self.grid_map.values() if i['side'] == 'bid')
        return ask_cost + bid_committed

    # ---------- ★ (D) 추세 게이트 ----------
    def update_gate(self):
        if time.time() - self.last_gate_update < GATE_REFRESH_SEC and self.gate_ma > 0:
            return
        try:
            df = pyupbit.get_ohlcv(SYMBOL, interval="day", count=GATE_MA_DAYS + 2)
            if df is None or len(df) < GATE_MA_DAYS + 1: return
            closed = df['close'].iloc[:-1].tail(GATE_MA_DAYS)   # 완성된 일봉만
            self.gate_ma = Decimal(str(closed.mean()))
            self.last_gate_update = time.time()
        except Exception:
            logj("err_gate_ma", trace=traceback.format_exc())
            return
        if self.gate_ma <= 0 or self.current_price <= 0: return
        up = self.gate_ma * (Decimal("1") + GATE_BAND)
        dn = self.gate_ma * (Decimal("1") - GATE_BAND)
        prev = self.gate_open
        if self.current_price > up:   self.gate_open = True
        elif self.current_price < dn: self.gate_open = False
        # 밴드 내: 이전 상태 유지(히스테리시스)
        if prev != self.gate_open:
            logj("gate_change", gate="OPEN" if self.gate_open else "CLOSED",
                 price=str(self.current_price), ma=str(round(self.gate_ma, 0)))
            self.save_state()

    # ---------- 기동 시 유령 주문 정리 ----------
    def startup_purge(self):
        try:
            actual = self.upbit.get_order(SYMBOL, state='wait') or []
            actual_uuids = {o['uuid'] for o in actual}
            with self.lock:
                ghosts = [uid for uid in list(self.grid_map.keys()) if uid not in actual_uuids]
                for uid in ghosts:
                    del self.grid_map[uid]
            logj("startup_purge", removed=len(ghosts), kept=len(self.grid_map),
                 actual_open=len(actual_uuids))
            self.save_state()
        except Exception:
            logj("err_startup_purge", trace=traceback.format_exc())

    # ---------- 상태 저장/복원 (v4: {"orders":…, "meta":…} 스키마, 구버전 자동 마이그레이션) ----------
    def load_state(self):
        if not os.path.exists(STATE_FILE): return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            raw_map = data.get("orders", data)          # 구버전(맵 단독) 호환
            meta = data.get("meta", {})
            for uid, info in raw_map.items():
                vol = Decimal(info.get('volume', '0'))
                if vol <= 0: continue
                self.grid_map[uid] = {
                    'buy_price': Decimal(info['buy_price']),
                    'sell_price': Decimal(info['sell_price']),
                    'side': info['side'],
                    'volume': vol,
                    'timestamp': info.get('timestamp', time.time())
                }
            self.gate_open = bool(meta.get("gate_open", False))
            self.cumulative_net_profit = Decimal(str(meta.get("cum_profit", "0")))
            logj("state_loaded", count=len(self.grid_map), gate="OPEN" if self.gate_open else "CLOSED")
        except Exception:
            logj("err_load", trace=traceback.format_exc())

    def save_state(self):
        try:
            orders = {}
            for uid, info in self.grid_map.items():
                orders[uid] = {
                    'buy_price': str(info['buy_price']),
                    'sell_price': str(info['sell_price']),
                    'side': info['side'],
                    'volume': str(info['volume']),
                    'timestamp': info.get('timestamp', time.time())
                }
            data = {"orders": orders,
                    "meta": {"gate_open": self.gate_open,
                             "cum_profit": str(self.cumulative_net_profit)}}
            tmp_file = STATE_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            os.replace(tmp_file, STATE_FILE)
            if DRY_RUN: self.upbit.save()
        except Exception:
            logj("err_save", trace=traceback.format_exc())

    # ---------- 정합성 ----------
    def reconcile_orders(self):
        if time.time() - self.last_reconcile_time < 60: return
        try:
            actual_orders = self.upbit.get_order(SYMBOL, state='wait') or []
            actual_uuids = [o['uuid'] for o in actual_orders]
            now = time.time()

            with self.lock:
                to_delete = []
                for uid, info in self.grid_map.items():
                    if uid not in actual_uuids and (now - info.get('timestamp', 0) > 20):
                        if info['side'] == 'bid': continue
                        if info['side'] == 'ask':
                            # ★ (I) 삭제 전 체결 여부 확인: 체결된 매도면 정산 후 삭제
                            #   (check_fill 이 채가기 전에 reconcile 이 먼저 도는 경합 창 방어)
                            try:
                                detail = self.upbit.get_order(uid) or {}
                                exec_vol = Decimal(str(detail.get('executed_volume', '0')))
                                d_state = detail.get('state', '')
                            except Exception:
                                exec_vol, d_state = Decimal('0'), ''
                            if d_state == 'done' and exec_vol > 0:
                                net = (Decimal(str(detail.get('price', info['sell_price']))) * exec_vol * Decimal("0.9995")) \
                                      - (info['buy_price'] * exec_vol * Decimal("1.0005"))
                                self.cumulative_net_profit += net
                                self.daily_sale_count += 1
                                logj("trade_success_reconciled", profit=str(round(net, 2)),
                                     total=str(round(self.cumulative_net_profit, 2)),
                                     daily_sales=self.daily_sale_count)
                            to_delete.append(uid)
                for uid in to_delete: del self.grid_map[uid]

                for o in actual_orders:
                    if o['uuid'] not in self.grid_map:
                        p = Decimal(str(o['price']))
                        v = Decimal(str(o['remaining_volume']))
                        side = 'bid' if o['side'] == 'bid' else 'ask'
                        buy_p = p if side == 'bid' else adjust_price(p / (Decimal("1") + PROFIT_PCT))
                        self.grid_map[o['uuid']] = {
                            'buy_price': buy_p,
                            'sell_price': p if side == 'ask' else adjust_price(p * (Decimal("1") + PROFIT_PCT)),
                            'side': side, 'volume': v, 'timestamp': now
                        }
                        logj("self_healing_sync", price=str(p), side=side)

                buy_infos = sorted([i for i in self.grid_map.values() if i['side'] == 'bid'],
                                   key=lambda x: x['buy_price'], reverse=True)
                if buy_infos and self.gate_open:            # ★ 게이트 닫힘 시 재정렬(신규 배치 유발)도 보류
                    highest_bid_p = buy_infos[0]['buy_price']
                    mode_m = {"NORMAL": 1.0, "CAUTION": 2.0, "DEFENSIVE": 3.3, "FREEZE": 999.0}
                    target_1st = adjust_price(self.current_price * (Decimal("1") - BASE_GRID_GAP * Decimal(str(mode_m.get(self.current_mode, 1.0)))))
                    if target_1st > highest_bid_p and (target_1st - highest_bid_p) > PROXIMITY_KRW:
                        logj("periodic_realign_up", current=str(highest_bid_p), target=str(target_1st))
                        self.reset_buy_grid()

                # ★ (E) orphan: 총보유(가용+잠김) 기준
                total_coin = self.get_total_coin()
                tracked_sell_vol = sum(info['volume'] for info in self.grid_map.values() if info['side'] == 'ask')
                orphan_vol = total_coin - tracked_sell_vol

                if orphan_vol > Decimal("0.02"):
                    if self.orphan_baseline is None:
                        self.orphan_baseline = self.current_price
                        logj("orphan_found_anchor", vol=str(orphan_vol), anchor_price=str(self.orphan_baseline))
                    target_3pct_p = adjust_price(self.orphan_baseline * Decimal("1.03"))
                    if self.current_price >= target_3pct_p:
                        logj("orphan_sniper_execute", vol=str(orphan_vol), exit_price=str(self.current_price))
                        res = self.upbit.sell_limit_order(SYMBOL, float(self.current_price), float(orphan_vol))
                        if res:
                            self.grid_map[res['uuid']] = {
                                'buy_price': self.orphan_baseline, 'sell_price': self.current_price,
                                'side': 'ask', 'volume': orphan_vol, 'timestamp': time.time()
                            }
                            self.orphan_baseline = None
                    else:
                        logj("orphan_monitoring", vol=str(orphan_vol), anchor=str(self.orphan_baseline), target=str(target_3pct_p))
                else:
                    self.orphan_baseline = None
            self.last_reconcile_time = now
            self.save_state()
        except Exception:
            logj("err_reconcile", trace=traceback.format_exc())   # ★ (E) 침묵 실패 제거

    def reset_buy_grid(self):
        with self.lock:
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    uuid = o['uuid']
                    self.upbit.cancel_order(uuid)
                    time.sleep(0.2)
                    detail = self.upbit.get_order(uuid)
                    exec_vol = Decimal(str(detail.get('executed_volume', '0')))
                    if exec_vol > 0 and uuid in self.grid_map:
                        info = self.grid_map[uuid]
                        s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(exec_vol))
                        if s_res:
                            self.grid_map[s_res['uuid']] = {
                                'buy_price': info['buy_price'], 'sell_price': info['sell_price'],
                                'side': 'ask', 'volume': exec_vol, 'timestamp': time.time()
                            }
                    if uuid in self.grid_map: del self.grid_map[uuid]
            time.sleep(0.3)
            self.save_state()

    # ---------- 그리드 유지 ----------
    def maintain_grid(self):
        try:
            mode_m = {"NORMAL": 1.0, "CAUTION": 2.0, "DEFENSIVE": 3.3, "FREEZE": 999.0}
            current_gap = BASE_GRID_GAP * Decimal(str(mode_m.get(self.current_mode, 1.0)))
            with self.lock:
                if self.current_mode == "FREEZE": return

                buy_infos = sorted([i for i in self.grid_map.items() if i[1]['side'] == 'bid'],
                                   key=lambda x: x[1]['buy_price'])
                if len(buy_infos) >= MAX_LAYERS:
                    highest_buy = buy_infos[-1][1]['buy_price']
                    if (self.current_price - highest_buy) / self.current_price > (current_gap + Decimal("0.002")):
                        lowest_uid = buy_infos[0][0]
                        logj("prune_lowest", price=str(buy_infos[0][1]['buy_price']))   # ★ (H)
                        detail = self.upbit.get_order(lowest_uid)
                        exec_vol = Decimal(str(detail.get('executed_volume', '0')))
                        self.upbit.cancel_order(lowest_uid)
                        time.sleep(0.2)
                        if exec_vol > 0 and lowest_uid in self.grid_map:
                            info = self.grid_map[lowest_uid]
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(exec_vol))
                            if s_res:
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 'sell_price': info['sell_price'],
                                    'side': 'ask', 'volume': exec_vol, 'timestamp': time.time()
                                }
                        if lowest_uid in self.grid_map: del self.grid_map[lowest_uid]

                # ===== ★ 신규 매수 배치 게이트 (AND 결합) =====
                # 1) 추세 게이트 닫힘 → 신규 매수 없음 (기존 주문은 유지)
                if not self.gate_open:
                    if time.time() - self.last_gate_log > 600:
                        logj("buy_blocked_gate", price=str(self.current_price),
                             ma=str(round(self.gate_ma, 0)) if self.gate_ma > 0 else "N/A")
                        self.last_gate_log = time.time()
                    return
                # 2) 재고 상한(원가 기준) → 신규 매수 없음 (매도는 계속)
                inv = self.inventory_cost_krw()
                if inv >= MAX_INVENTORY_KRW:
                    if time.time() - self.last_gate_log > 600:
                        logj("buy_blocked_inventory", inventory_cost_krw=str(round(inv, 0)),
                             cap=str(MAX_INVENTORY_KRW))
                        self.last_gate_log = time.time()
                    return
                # 이번 매수(30만)까지 더하면 상한을 넘는 경우도 차단
                if inv + BUY_AMOUNT_KRW > MAX_INVENTORY_KRW:
                    if time.time() - self.last_gate_log > 600:
                        logj("buy_blocked_inventory_next", inventory_cost_krw=str(round(inv, 0)),
                             cap=str(MAX_INVENTORY_KRW))
                        self.last_gate_log = time.time()
                    return

                occupied_prices = [i['buy_price'] for i in self.grid_map.values()]
                current_bid_count = len([i for i in self.grid_map.values() if i['side'] == 'bid'])
                for i in range(1, MAX_LAYERS + 1):
                    target_p = adjust_price(self.current_price * (Decimal("1") - current_gap * i))
                    if any(abs(p - target_p) <= PROXIMITY_KRW for p in occupied_prices): continue
                    if current_bid_count < MAX_LAYERS:
                        # ★ (J) 매 배치 직전 상한 재확인: 한 사이클에 여러 개 깔 때도 초과 불가
                        if self.inventory_cost_krw() + BUY_AMOUNT_KRW > MAX_INVENTORY_KRW:
                            logj("buy_blocked_inventory_next",
                                 inventory_cost_krw=str(round(self.inventory_cost_krw(), 0)),
                                 cap=str(MAX_INVENTORY_KRW))
                            break
                        balance = Decimal(str(self.upbit.get_balance("KRW")))
                        if balance < (BUY_AMOUNT_KRW * Decimal("1.0005")): break
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            self.grid_map[res['uuid']] = {
                                'buy_price': target_p, 'sell_price': adjust_price(target_p * (Decimal("1") + PROFIT_PCT)),
                                'side': 'bid', 'volume': vol, 'timestamp': time.time()
                            }
                            logj("place_buy", price=str(target_p))
                            current_bid_count += 1
                            occupied_prices.append(target_p)
                            self.save_state()
                        time.sleep(0.3 if not DRY_RUN else 0.01)
        except Exception:
            logj("err_maintain", trace=traceback.format_exc())

    # ---------- 체결 처리 ----------
    def check_fill(self):
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            if today != self.last_sale_date:
                self.daily_sale_count = 0
                self.last_sale_date = today

            dones = self.upbit.get_order(SYMBOL, state='done') or []
            changed = False
            with self.lock:
                for o in dones:
                    uid = o['uuid']
                    if uid in self.grid_map:
                        info = self.grid_map[uid]
                        vol = Decimal(str(o['executed_volume']))
                        if info['side'] == 'bid':
                            s_res = self.upbit.sell_limit_order(SYMBOL, float(info['sell_price']), float(vol))
                            if s_res:
                                del self.grid_map[uid]
                                self.grid_map[s_res['uuid']] = {
                                    'buy_price': info['buy_price'], 'sell_price': info['sell_price'],
                                    'side': 'ask', 'volume': vol, 'timestamp': time.time()
                                }
                                logj("place_sell", price=str(info['sell_price']))
                                changed = True
                        elif info['side'] == 'ask':
                            del self.grid_map[uid]
                            self.daily_sale_count += 1
                            net = (Decimal(str(o['price'])) * vol * Decimal("0.9995")) - (info['buy_price'] * vol * Decimal("1.0005"))
                            self.cumulative_net_profit += net
                            logj("trade_success", profit=str(round(net, 2)),
                                 total=str(round(self.cumulative_net_profit, 2)),
                                 daily_sales=self.daily_sale_count)
                            # ★ (E) 구버전의 mode=NORMAL 강제 리셋 삭제
                            changed = True
                if changed: self.save_state()
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    # ---------- 시장 지표 ----------
    def update_24h_high(self):
        if time.time() - self.last_high_update < 300: return
        try:
            df = pyupbit.get_ohlcv(SYMBOL, interval="minute60", count=24)
            if df is not None:
                self.rolling_24h_high = Decimal(str(df['high'].max()))
                self.last_high_update = time.time()
        except Exception:
            logj("err_24h_high", trace=traceback.format_exc())

    def get_volume_power(self):
        now = time.time()
        while self.trade_history and now - self.trade_history[0][0] > VP_WINDOW_SEC:
            self.trade_history.popleft()
        buy_vol = sum(vol for ts, vol, side in self.trade_history if side == 'BID')
        sell_vol = sum(vol for ts, vol, side in self.trade_history if side == 'ASK')
        if self.current_price == 0: return Decimal("100.0")
        total_vol_krw = (Decimal(str(buy_vol)) + Decimal(str(sell_vol))) * self.current_price
        if total_vol_krw < MIN_TOTAL_VOL_KRW: return Decimal("100.0")
        if sell_vol == 0: return Decimal("100.0")
        return (Decimal(str(buy_vol)) / Decimal(str(sell_vol))) * 100

    def decide_mode(self):
        if self.rolling_24h_high == 0: return "NORMAL"
        mdd = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high
        vp = self.get_volume_power()
        if mdd <= FREEZE_MDD or vp < FREEZE_VP: return "FREEZE"
        if mdd <= DEFENSIVE_MDD or vp < DEFENSIVE_VP: return "DEFENSIVE"
        if mdd <= CAUTION_MDD or vp < CAUTION_VP: return "CAUTION"
        return "NORMAL"

    # ---------- 메인 루프 ----------
    def run(self):
        logj("bot_start", v="4.2.RaceFix", dry=DRY_RUN,
             inventory_cap=str(MAX_INVENTORY_KRW), gate_ma_days=GATE_MA_DAYS)
        wm = WebSocketManager("ticker", [SYMBOL])
        err_streak = 0
        last_loop_time = 0
        while True:
            try:
                data = wm.get()
                if not data:
                    raise ValueError("ws_empty")
                err_streak = 0
                self.current_price = Decimal(str(data['trade_price']))
                self.trade_history.append((time.time(), Decimal(str(data['trade_volume'])), data['ask_bid']))

                if DRY_RUN:
                    self.upbit.on_price(self.current_price)   # ★ 가상 체결 엔진

                if time.time() - last_loop_time > 4:
                    self.update_24h_high()
                    self.update_gate()                         # ★ (D)
                    self.check_fill()                          # ★ (I) reconcile 보다 먼저: 체결 정산 우선
                    self.reconcile_orders()
                    self.current_mode = self.decide_mode()
                    if self.current_mode != self.last_mode:
                        logj("mode_change_reset", prev=self.last_mode, curr=self.current_mode)
                        self.reset_buy_grid()
                        self.last_mode = self.current_mode
                    self.maintain_grid()
                    mdd_val = (self.current_price - self.rolling_24h_high) / self.rolling_24h_high if self.rolling_24h_high > 0 else 0
                    logj("status", mode=self.current_mode,
                         gate="OPEN" if self.gate_open else "CLOSED",
                         price=format(int(self.current_price), ','),
                         mdd=f"{round(mdd_val*100, 2)}%",
                         v_power=f"{round(self.get_volume_power(), 1)}%",
                         inventory_cost_krw=str(round(self.inventory_cost_krw(), 0)),
                         daily_sales=self.daily_sale_count,
                         cum_profit=str(round(self.cumulative_net_profit, 2)))
                    last_loop_time = time.time()
            except Exception:
                err_streak += 1
                logj("err_loop", streak=err_streak, trace=traceback.format_exc())
                time.sleep(2)
                if err_streak >= WS_ERR_RESTART:               # ★ (E) 웹소켓 좀비 방지
                    logj("ws_restart")
                    try: wm.terminate()
                    except Exception: pass
                    wm = WebSocketManager("ticker", [SYMBOL])
                    err_streak = 0

if __name__ == "__main__":
    GridBotV4().run()