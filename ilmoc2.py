import pyupbit
import pandas as pd
import talib as ta
import time
import numpy as np
import requests
import datetime
import yaml
from decimal import Decimal

with open('config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

# Set API keys
access_key = _cfg['access_key']
secret_key = _cfg['secret_key']
upbit = pyupbit.Upbit(access_key, secret_key)
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']

# Set trading parameters
tickers = pyupbit.get_tickers("KRW")
interval_daily = 'day'
interval_minutes = 'minute5'
min_order_amount = 6000
max_order_amount = 50000
trailing_start_percentage = 0.04
trailing_stop_percentage = 0.027 
stop_loss_percentage = 0.04

def send_message(msg):

    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print(message)

# Get owned stocks
def get_owned_stocks():
    owned_stocks = []
    balances = upbit.get_balances()
    for balance in balances:
        if balance['currency'] != 'KRW':
            owned_stocks.append('KRW-' + balance['currency'])
    return owned_stocks

def ichimoku(df):
    high_prices = df['high']
    low_prices = df['low']
    close_prices = df['close']
    dates = df.index

    # Tenkan-sen (Conversion Line): (9-period high + 9-period low)/2))
    nine_period_high = high_prices.rolling(window=9).max()
    nine_period_low = low_prices.rolling(window=9).min()
    tenkan_sen = (nine_period_high + nine_period_low) / 2

    # Kijun-sen (Base Line): (26-period high + 26-period low)/2))
    period26_high = high_prices.rolling(window=26).max()
    period26_low = low_prices.rolling(window=26).min()
    kijun_sen = (period26_high + period26_low) / 2

    # Senkou Span A (Leading Span A): (Conversion Line + Base Line)/2))
    senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(26)

    # Senkou Span B (Leading Span B): (52-period high + 52-period low)/2))
    period52_high = high_prices.rolling(window=52).max()
    period52_low = low_prices.rolling(window=52).min()
    senkou_span_b = ((period52_high + period52_low) / 2).shift(26)

    return tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b

def stock_selection(ticker):
    df = pyupbit.get_ohlcv(ticker, interval=interval_daily)

    if df is None:
        return False

    tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b = ichimoku(df)

    # 종목선정 조건 1: 일목균형선들 중 두개 이상이 양봉 캔들을 우상향하며 관통한다.
    bullish_candle = (df['close'] > df['open']) & (tenkan_sen.shift(-1) > tenkan_sen) & (kijun_sen.shift(-1) > kijun_sen)
    condition1 = bullish_candle & (tenkan_sen > df['open']) & (kijun_sen > df['open'])

    # 종목선정 조건 2: 그 양봉 캔들 윗쪽에는 일목균형선이 있으면 안된다.
    condition2 = (tenkan_sen.shift(-1) < df['close']) & (kijun_sen.shift(-1) < df['close'])

    # 종목선정 조건 3: 일목균형선들은 반드시 그 양봉 캔들을 관통하거나 캔들 아래쪽으로 지나가야 한다.
    condition3 = (tenkan_sen.shift(-1) < df['close']) | (kijun_sen.shift(-1) < df['close'])

    # 종목선정 조건 4: 그 양봉 캔들의 몸통 길이는 윗꼬리와 아랫꼬리를 합친 길이보다 길어야 한다.
    body_length = df['close'] - df['open']
    upper_tail = df['high'] - df['close']
    lower_tail = df['open'] - df['low']
    condition4 = body_length > (upper_tail + lower_tail)

    # 종목선정 조건 5: 그 양봉 캔들의 윗쪽에 구름이 형성된 종목은 제외한다.
    condition5 = (df['close'] > senkou_span_a) & (df['close'] > senkou_span_b)

    selected = (condition1 & condition2 & condition3 & condition4 & condition5).iat[-1]

    return selected

def buy_next_day(ticker):
    current_price = pyupbit.get_current_price(ticker)
    if current_price is None:
        send_message("Failed to get current price for {}".format(ticker))
        return
    buy_amount = min(max_order_amount, upbit.get_balance("KRW"))
    if buy_amount < min_order_amount:
        return
    upbit.buy_market_order(ticker, buy_amount)
    send_message("Buy {} at {} KRW, Amount: {}".format(ticker, current_price, buy_amount))

    # Monitor and sell the stock
    bought_price = current_price
    trailing_high_price = bought_price

    # send_message("업비트 자동매매 프로그램 시작")
    while True:
        current_price = pyupbit.get_current_price(ticker)
        if current_price is None:
            send_message("Failed to get current price for {}".format(ticker))
            # print(f"Failed to get current price for {ticker}")
            continue
        
        # Update trailing high price
        if current_price >= bought_price * (1 + trailing_start_percentage):
            trailing_high_price = max(trailing_high_price, current_price)
        
        # Check if the stock should be sold due to trailing stop
        if trailing_high_price * (1 - trailing_stop_percentage) >= current_price:
            units = upbit.get_balance(ticker)
            upbit.sell_market_order(ticker, units)
            print("Sell {} at {} KRW, Trailing Stop".format(ticker, current_price))
            # print(f"Sell {ticker} at {current_price} KRW, Trailing Stop")
            break

        # Check if the stock should be sold due to stop loss
        if bought_price * (1 - stop_loss_percentage) >= current_price:
            units = upbit.get_balance(ticker)
            upbit.sell_market_order(ticker, units)
            send_message("Sell {} at {} KRW, Stop Loss".format(ticker, current_price))
            # print(f"Sell {ticker} at {current_price} KRW, Stop Loss")
            break

        # Wait for 10 seconds before checking again
        time.sleep(10)

import threading

def asset_summary():
    threading.Timer(14400, asset_summary).start() 

    krw_balance = Decimal(upbit.get_balance("KRW"))
    balances = upbit.get_balances()
    total_assets = krw_balance

    message = "보유 종목:\n"

    for balance in balances:
        if balance['currency'] != 'KRW':
            ticker = 'KRW-' + balance['currency']
            
            if ticker not in tickers:
                continue

            amount = Decimal(balance['balance'])
            avg_buy_price = Decimal(balance['avg_buy_price'])

            # 주문 내역에서 미체결 매도 주문 수량 가져오기
            orders = upbit.get_order(ticker, state="wait")
            for order in orders:
                if order['side'] == 'ask':
                    amount += Decimal(order['remaining_volume'])

            total_value = avg_buy_price * amount
            total_assets += total_value

            message += f"{ticker}: 수량 {amount}, 총액(KRW) {total_value:.0f}\n"

    message += f"원화 보유액: {krw_balance:.0f} KRW\n"
    message += f"자산 총액: {total_assets:.0f} KRW"

    send_message(message)

asset_summary()

'''
selected_tickers = []
for ticker in tickers:
    try:
        if stock_selection(ticker):
            selected_tickers.append(ticker)
            send_message("Selected {}".format(ticker))
    except Exception as e:
        send_message("Error checking {}: {}".format(ticker, e))
'''
# send_message("선정된 종목: {}".format(', '.join(selected_tickers)))

# 다음 날의 시작가에 매수를 진행하기 위해 시간을 확인하고 대기합니다.

def main_loop():
    while True:
        now = datetime.datetime.now()
        selected_tickers = []

        for ticker in tickers:
            try:
                if stock_selection(ticker):
                    selected_tickers.append(ticker)
                    send_message("Selected {}".format(ticker))
            except Exception as e:
                send_message("Error checking {}: {}".format(ticker, e))

        for ticker in selected_tickers:
            try:
                # Wait for the next 5-minute candle
                wait_seconds = 60 - now.second + (4 - (now.minute % 5)) * 60
                time.sleep(wait_seconds)

                buy_next_day(ticker)
            except Exception as e:
                send_message("Error buying {}: {}".format(ticker, e))

        # Wait 1 minute before checking again
        time.sleep(60)

send_message("업비트 자동매매 프로그램 시작")
main_loop()
