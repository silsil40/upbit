import time
import pyupbit
import math

access = "rIGYfaNMhpNrKgUhxAEsSNONjTx6Br6nblxQu0nE"
secret = "xopBplK0MWlPuTaf9V197Xgtb1hC02LnPAvkHmvo"

def get_balance(ticker):
    """잔고 조회"""
    balances = upbit.get_balances()
    for b in balances:
        if b['currency'] == ticker:
            if b['balance'] is not None:
                return float(b['balance'])
            else:
                return 0
    return 0

def get_current_price(ticker):
    """현재가 조회"""
    return pyupbit.get_orderbook(ticker=ticker)["orderbook_units"][0]["ask_price"]

#로그인
upbit = pyupbit.Upbit(access, secret)
print("autotrade start")

#자동매매 시작
try:

    min_price = 11.0
    max_price = 20.0
    set_balance = 5100
    de_price = 0.3
    my_cash = get_balance("KRW") 
    default_price = 0.0

    bound_trans = list() # 거래 범위
    bound_buy = list() # 매수 리스트
    bound_sel = list() # 매도 리스트

    # 거래 범위 설정
    default_price = min_price
    bound_trans.append(round(default_price,2))
    while default_price <= max_price:
        default_price = default_price+de_price
        bound_trans.append(round(default_price,2))
    
    print("!!!Bound_trans : {}".format(bound_trans))

    # loop 시작
    while True :
        current_price = get_current_price("KRW-CRE")
        print("!!!Current_price : {}".format(current_price))

        bound_buy.clear()
        bound_sel.clear()

        # 실시간 호가 확인 후 매도, 매수 리스트 적재
        for i in bound_trans:
            if current_price > i:
                bound_buy.append(i)
            else:
                bound_sel.append(i)

        #print("!!!Bound_buy List!!! : {}".format(bound_buy))
        #print("!!!Bound_sel List!!! : {}".format(bound_sel))

        # 미체결 거래내역 체크해서 sel,buy list에 있으면 삭제(아래에서 리스트에 있는 대상 전부 매수,매도 예정)
        ord_list = upbit.get_order("KRW-CRE")
        bound_size = len(bound_trans)
        while bound_size > 0:
            try:
                trans_type = list(ord_list)[bound_size-1].get('side')
                trans_cash = list(ord_list)[bound_size-1].get('price')

                if trans_type == 'ask': # 매도 체크
                    if float(trans_cash) in bound_sel:
                        bound_sel.remove(float(trans_cash))
                elif trans_type == 'bid': # 매수 체크
                    if float(trans_cash) in bound_buy:
                        bound_buy.remove(float(trans_cash))
                        
            except IndexError as e:
                #print("Index Error : ".format(e))
                pass
            bound_size-=1
        
        #print("!!!Confirm Bound_buy List!!! : {}".format(bound_buy))
        #print("!!!Confirm Bound_sel List!!! : {}".format(bound_sel))

        # Sel list 매도 예약
        for i in bound_sel:
            my_cre = get_balance("CRE")
            sell_coin = set_balance/i
            if my_cre > sell_coin:
                upbit.sell_limit_order("KRW-CRE", round(i,2), sell_coin) # 매도 예약
                print("Order Sell Coin!!!! : {}".format(round(i,2)))
                time.sleep(1)
        
        # Buy list 매수 예약
        for i in bound_buy:
            my_cash = get_balance("KRW")
            cnt_maesoo = math.trunc(my_cash/set_balance)
            ord_buy_coin = set_balance/i
            if cnt_maesoo > 0:
                upbit.buy_limit_order("KRW-CRE", round(i,2), ord_buy_coin) # 매수 예약
                print("Add Order Buy Coin!!!! {}".format(round(i,2)))
                time.sleep(1)
    
except Exception as e:
    print(e)
    time.sleep(1)