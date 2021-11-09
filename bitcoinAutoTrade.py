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
    current_price = get_current_price("KRW-CRE")
    min_price = 12.0
    max_price = 17.0
    set_balance = 10000
    de_price = 0.1
    my_cash = get_balance("KRW") 
    last_done = pyupbit.get_current_price("KRW-CRE")

    if my_cash > set_balance : 
        #현재가 매수
        buy_coin = set_balance/current_price
        upbit.buy_limit_order("KRW-CRE", current_price, buy_coin)
        print("Buy Coin!!!!")
        time.sleep(10)

        get_orderd = upbit.get_order("KRW-CRE", state="done")
        frt_uuid = list(get_orderd)[0].get('uuid')
        frt_price = upbit.get_order(frt_uuid).get('price')
        print("!!!!frt_uuid : {}".format(frt_uuid))

        my_cash = get_balance("KRW")
        cnt = my_cash/set_balance
        #최초 매수가 아래로 매수 주문
        while cnt >= 0 and float(frt_price) >= min_price:
            frt_price=float(frt_price)-de_price # 매수할 호가 지정
            cnt-=1
            buy_coin = set_balance/float(frt_price)
            upbit.buy_limit_order("KRW-CRE", round(frt_price,2), buy_coin)
            print("Order Buy Coin!!!! : {}".format(round(frt_price,2)))
            time.sleep(1)

        #보유 코인 체크해서 최초 매수가 위로 매도 주문, 현재 코인 가격 재측정
        my_cre = get_balance("CRE")
        frt_price = upbit.get_order(frt_uuid).get('price')

        while my_cre > (set_balance/(float(frt_price)+de_price)) and float(frt_price) <= max_price:
            frt_price=float(frt_price)+de_price # 매도할 호가 지정
            sell_coin = set_balance/float(frt_price)
            if my_cre > sell_coin:
                upbit.sell_limit_order("KRW-CRE", round(frt_price,2), sell_coin)
                my_cre = get_balance("CRE")
                print("Order Sell Coin!!!! : {}".format(round(frt_price,2)))
            time.sleep(1)

        #마지막 체결된 거래 내용 확인해서 매수/매도 셋팅, bid 매수, ask 매도
        while True:
            num = 0
            get_orderd = upbit.get_order("KRW-CRE", state="done")
            dne_ordcnt = len(list(get_orderd))
            last_uuid = 0
            first_in = 0
            confirm_dt = 0
            after_dt= 0
            #마지막 체결된 시간으로 uuid 가져오기.
            while dne_ordcnt > 0:
                pre_uuid = list(get_orderd)[dne_ordcnt-1].get('uuid')
                if first_in == 0:
                    confirm_dt = list(upbit.get_order(pre_uuid).get('trades'))[0].get('created_at')
                    first_in+=1
                else:
                    after_dt = list(upbit.get_order(pre_uuid).get('trades'))[0].get('created_at')

                #큰값으로 교체
                #print("이전값 : {}".format(confirm_dt))
                #print("비교값 : {}".format(after_dt))
                if str(after_dt) > confirm_dt:
                    confirm_dt = after_dt
                    last_uuid = pre_uuid

                dne_ordcnt-=1  

            str_side = upbit.get_order(last_uuid).get('side')
            fl_price = float(upbit.get_order(last_uuid).get('price'))
            aft_uuid = last_uuid

            #최초 uuid와 최근 체결된 uuid 비교
            if frt_uuid != aft_uuid: #최초 체결된 거래는 제외.
                if str_side == 'bid': #매수 (매도 예약 필요)
                    fl_price+=de_price
                    my_cre = get_balance("CRE")
                    ord_sel_coin = set_balance/fl_price
                    if my_cre > ord_sel_coin:
                        upbit.sell_limit_order("KRW-CRE", round(fl_price,2), ord_sel_coin)
                        print("Add Order Sell Coin!!!! {}".format(round(fl_price,2)))
                        print("!!!!Sell check uuid : {}".format(aft_uuid))
                        frt_uuid = aft_uuid
                elif str_side == 'ask': #매도 (매수 예약 필요.)
                    fl_price-=de_price
                    my_cash = get_balance("KRW")
                    cnt_maesoo = math.trunc(my_cash/set_balance)
                    ord_buy_coin = set_balance/fl_price
                    if cnt_maesoo > 0:
                        upbit.buy_limit_order("KRW-CRE", round(fl_price,2), ord_buy_coin)
                        print("Add Order Buy Coin!!!! {}".format(round(fl_price,2)))
                        print("!!!!Buy check uuid : {}".format(aft_uuid))
                        frt_uuid = aft_uuid

                        #호가창 상단에 매도 작업 추가
                        maedo_list = upbit.get_order("KRW-CRE")

                        #매도 예약된 최대 금액 구하기
                        firstin = 0
                        tt = 0
                        ch_price = 0
                        pri_size = len(list(maedo_list))
                        while pri_size > 0:
                            
                            trans_type = list(maedo_list)[pri_size-1].get('side')
                            if trans_type == 'ask':
                                if firstin == 0:
                                    tt = list(maedo_list)[pri_size-1].get('price') 
                                    firstin+=1
                                else:
                                    ch_price = list(maedo_list)[pri_size-1].get('price')

                                #큰값으로 교체
                                #print("이전값 : {}".format(float(tt)))
                                #print("비교값 : {}".format(float(ch_price)))
                                if float(ch_price) > float(tt):
                                    tt = ch_price

                            pri_size-=1    
                                
                        print("!!!!max buy orderd price!!!! {}".format(tt))
                        #최대금액+ 기준호가 금액으로 매도 추가
                        my_cre = get_balance("CRE")
                        while my_cre > (set_balance/(float(tt)+de_price)):
                            tt=float(tt)+de_price # 매도할 호가 지정
                            sell_coin = set_balance/float(tt)
                            if my_cre > sell_coin:
                                upbit.sell_limit_order("KRW-CRE", round(tt,2), sell_coin)
                                my_cre = get_balance("CRE")
                                print("Order Sell Coin!!!!")
                                print("Order Sell Coin!!!! : {}".format(round(tt,2)))
                            time.sleep(1)
                        
except Exception as e:
    print(e)
    time.sleep(1)