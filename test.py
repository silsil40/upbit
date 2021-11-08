import pyupbit

def get_current_price(ticker):
    """현재가 조회"""
    return pyupbit.get_orderbook(ticker=ticker)["orderbook_units"][0]["ask_price"]

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

access = "rIGYfaNMhpNrKgUhxAEsSNONjTx6Br6nblxQu0nE"          # 본인 값으로 변경
secret = "xopBplK0MWlPuTaf9V197Xgtb1hC02LnPAvkHmvo"          # 본인 값으로 변경
upbit = pyupbit.Upbit(access, secret)

print(upbit.get_balance("KRW-CRE"))     # KRW-CRE 조회
print(upbit.get_balance("KRW"))         # 보유 현금 조회

current_price = get_current_price("KRW-CRE")
#min_price = 9.5
#max_price = 19.4
set_balance = 5100
my_krw = upbit.get_balance("KRW")
my_cre = upbit.get_balance("KRW-CRE")
#print(set_balance/current_price)

"""
while min_price <= max_price :
    min_price+=1
    print(min_price)
    print(30)
"""
#max_price+=1
#print(max_price)
#print(40)

#print(upbit.get_order("KRW-CRE"))
test = upbit.get_order("KRW-CRE")
#print(list(test)[1].get('price'))
#print(test.get('price'))

print(pyupbit.get_current_price("KRW-CRE"))
#print(upbit.get_order("KRW-CRE"))

#for ticker, price in test.items():
#    print(ticker, price)

print(len(list(test)))
print(test)


firstin = 0
tt = 0
ch_price = 0
pri_size = len(list(test))
while pri_size > 0:
    if firstin == 0:
        tt = list(test)[pri_size-1].get('price') 
        firstin+=1
    else:
        ch_price = list(test)[pri_size-1].get('price')

    #큰값으로 교체
    print("이전값 : {}".format(float(tt)))
    print("비교값 : {}".format(float(ch_price)))
    if float(ch_price) > float(tt):
        tt = ch_price

    pri_size-=1    
    print("tt!!!! {}".format(tt))
print("!!!!max buy orderd price!!!! {}".format(float(tt)+0.1))



#print(upbit.get_order("KRW-CRE", state="done"))
get_orderd = upbit.get_order("KRW-CRE", state="done")
print(list(get_orderd)[0].get('side'))
print(list(get_orderd)[0].get('price'))
print(list(get_orderd)[0].get('uuid'))
frt_uuid = list(get_orderd)[0].get('uuid')
snd_uuid = list(get_orderd)[1].get('uuid')
#print(float(list(get_orderd)[0].get('price'))+0.1)

print(frt_uuid)
print(snd_uuid)

print("!!!!frt_uuid : {}".format(frt_uuid))

#print(float(upbit.get_order(frt_uuid).get('price'))+0.1)
frt_price = upbit.get_order(frt_uuid).get('price')
#print(float(frt_price)+0.1)
#frt_price=float(frt_price) + 0.8



#sell_coin = set_balance/float(frt_price)
#print("selcoin : {}".format(sell_coin))
#upbit.sell_limit_order("KRW-CRE", frt_price, sell_coin)

#if frt_uuid == snd_uuid:
#    print(1)
#else:
#    print(2)



'''
my_cre = 433
current_price = 13
max_price = 19.5
price_list = 0.1

while my_cre > (set_balance/(current_price+price_list)) and current_price <= max_price:
    current_price+=price_list # 매도할 호가 지정
    sell_coin = set_balance/current_price
    if my_cre > sell_coin:
        print("Order Sell Coin!!!!")
'''

