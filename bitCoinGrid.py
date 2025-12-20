"""
[전략 요약: 업비트 솔라나(SOL) 밀착 추적 그리드 봇]

1. 기본 환경
   - 대상: KRW-SOL (솔라나)
   - 방식: 촘촘한 스캘핑 (Grid Scalping)
   - 시드: 그리드당 10,000원 (설정에 따라 변경 가능)

2. 매수 및 추적 전략 (Crawling Grid)
   - 진입: 현재가 기준 아래로 0.3% 간격, 총 5단계 매수 그물망 상시 유지
   - 하락 시: 매수 체결 시 즉시 익절 주문을 걸고, 맨 아래에 새로운 그물 보충
   - 상승 시: 가격이 올라 그물이 멀어지면, 최하단 주문을 취소하여 현재가 밑으로 '전진 배치'
   - 특징: 가격을 자석처럼 밀착 추적하여 횡보 및 완만한 상승장에서 수익 극대화

3. 청산 및 관리 전략
   - 익절(TP): 각 매수 건별 +0.5% 지정가 매도 (수수료 제외 약 0.4% 순수익)
   - 꼬임 방지: 매수-매도 주문을 1:1로 매칭(Order Pairing)하여 잔고 섞임 원천 차단
   - 부하 관리: 3~5초 단위 상태 체크로 업비트 API 호출 제한(Rate Limit) 준수
"""

import os
import time
import json
import threading
import traceback
from decimal import Decimal, getcontext
from datetime import datetime
import pyupbit
from pyupbit import WebSocketManager

getcontext().prec = 28

# ==========================================
# [사용자 설정 영역]
# ==========================================
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY", "po04aXLppNilEDtmtkMVGMcL2VaaQTSU4aIy8xLy")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY", "6Yi02ssfxbXYzpOFlazpEjinLa6AVq3960lpxEzJ")

SYMBOL           = "KRW-SOL"
BUY_AMOUNT_KRW   = Decimal("10000")  # 각 그리드당 1만원
GRID_GAP_PCT     = Decimal("0.003")  # 0.3% 간격
PROFIT_PCT       = Decimal("0.005")  # 0.5% 익절
MAX_LAYERS       = 5                 # 아래로 유지할 그물 개수
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

class CrawlingGridBot:
    def __init__(self):
        self.upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
        self.lock = threading.Lock()
        self.grid_map = {} 
        self.current_price = Decimal("0")

    def init_clear_and_seed(self):
        """기존 주문 정리 후 현재가에 즉시 첫 번째 매수 주문 실행"""
        try:
            logj("init_clear", msg="Cleaning old orders...")
            orders = self.upbit.get_order(SYMBOL, state='wait') or []
            for o in orders:
                if o['side'] == 'bid':
                    self.upbit.cancel_order(o['uuid'])
                    time.sleep(0.1)

            # [핵심] 시작 시 현재가로 즉시 매수 주문 (Seed Buy)
            # 웹소켓 연결 전이므로 현재가 한 번 조회
            curr = Decimal(str(pyupbit.get_current_price(SYMBOL)))
            seed_p = adjust_price(curr)
            vol = BUY_AMOUNT_KRW / seed_p
            
            logj("seed_buy_try", price=str(seed_p))
            res = self.upbit.buy_limit_order(SYMBOL, float(seed_p), float(vol))
            
            if res and 'uuid' in res:
                sell_p = adjust_price(seed_p * (Decimal("1") + PROFIT_PCT))
                self.grid_map[res['uuid']] = {"sell_price": sell_p}
                logj("seed_buy_placed", uuid=res['uuid'], target_sell=str(sell_p))
            
        except Exception:
            logj("err_init", trace=traceback.format_exc())

    def maintain_grid(self):
        try:
            with self.lock:
                orders = self.upbit.get_order(SYMBOL, state='wait') or []
                buy_orders = sorted([o for o in orders if o['side'] == 'bid'], 
                                    key=lambda x: Decimal(str(x['price'])), reverse=True)

                # 1. 가격 상승 추적 (Shift Up)
                if buy_orders:
                    highest_buy = Decimal(str(buy_orders[0]['price']))
                    # 현재가가 1번 그리드보다 0.4% 이상 높아지면 추적 시작
                    if (self.current_price - highest_buy) / self.current_price > (GRID_GAP_PCT + Decimal("0.001")):
                        lowest_order = buy_orders[-1]
                        self.upbit.cancel_order(lowest_order['uuid'])
                        logj("shift_up", cancelled_price=str(lowest_order['price']))
                        buy_orders.pop()

                # 2. 그물망 유지 (현재가 아래로 0.3%씩 MAX_LAYERS개)
                for i in range(1, MAX_LAYERS + 1):
                    target_p = adjust_price(self.current_price * (Decimal("1") - GRID_GAP_PCT * i))
                    
                    # 겹침 방지 (간격의 절반인 0.15% 이내 주문 스킵)
                    min_dist = target_p * Decimal("0.0015")
                    if any(abs(Decimal(str(o['price'])) - target_p) < min_dist for o in buy_orders):
                        continue

                    if len(buy_orders) < MAX_LAYERS:
                        vol = BUY_AMOUNT_KRW / target_p
                        res = self.upbit.buy_limit_order(SYMBOL, float(target_p), float(vol))
                        if res and 'uuid' in res:
                            sell_p = adjust_price(target_p * (Decimal("1") + PROFIT_PCT))
                            self.grid_map[res['uuid']] = {"sell_price": sell_p}
                            logj("place_buy", price=str(target_p), sell_at=str(sell_p))
                            buy_orders.append({'price': target_p})
                        time.sleep(0.1)

        except Exception:
            logj("err_maintain", trace=traceback.format_exc())

    def check_fill(self):
        try:
            dones = self.upbit.get_order(SYMBOL, state='done') or []
            with self.lock:
                for o in dones:
                    uid = o['uuid']
                    if uid in self.grid_map:
                        sell_p = self.grid_map[uid]['sell_price']
                        vol = o['executed_volume']
                        s_res = self.upbit.sell_limit_order(SYMBOL, float(sell_p), float(vol))
                        if s_res:
                            logj("place_sell", price=str(sell_p), buy_uuid=uid)
                            del self.grid_map[uid]
        except Exception:
            logj("err_check", trace=traceback.format_exc())

    def run(self):
        # 시작 시 정리 및 첫 매수 실행
        self.init_clear_and_seed()
        
        logj("bot_start", msg="Crawling Grid v2.2 Active with Seed Buy")
        wm = WebSocketManager("ticker", [SYMBOL])
        last_t = 0
        while True:
            data = wm.get()
            if not data: continue
            self.current_price = Decimal(str(data['trade_price']))
            
            if time.time() - last_t > 4: # 4초 주기로 유지보수
                self.maintain_grid()
                self.check_fill()
                last_t = time.time()

if __name__ == "__main__":
    CrawlingGridBot().run()