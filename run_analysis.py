import boto3
import json
import pytz
import time
from tqdm import tqdm
import requests 
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime
import sys

def get_message():
    queue_url = "https://sqs.us-west-2.amazonaws.com/283282745763/dividend_analysis"
    sqs = boto3.client('sqs')
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds= 10)
    
    if 'Messages' not in response:
        print("No messages to process.")
        return
        
    message = response['Messages'][0]
    print("Got message:", message['Body'])
    return message
    
def clear_message(message):
    queue_url = "https://sqs.us-west-2.amazonaws.com/283282745763/dividend_analysis"
    sqs = boto3.client('sqs')
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
    print("Message deleted.")
    
def notify(plain_text):
    sns_client = boto3.client('sns', region_name='us-west-2')
    topic_arn = 'arn:aws:sns:us-west-2:283282745763:trading'
    sns_client.publish(
        TopicArn=topic_arn,
        Message=plain_text,
    )
    
def get_secret():
    client = boto3.client('secretsmanager', region_name='us-west-2')
    return json.loads(client.get_secret_value(SecretId="tokens")['SecretString'])

def parse_time(iso_string ):
    utc_time = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    portland_time = utc_time.astimezone(ZoneInfo("America/Los_Angeles"))
    return portland_time
    
def get_candles(ticker, start_date, end_date, timeframe = '30Min'):
    base_url = "https://data.alpaca.markets/v2/stocks"
    headers = {
        "APCA-API-KEY-ID": alpaca_key,
        "APCA-API-SECRET-KEY": alpaca_secret
    }
    
    # Parse the start and end dates
    start_time = pd.to_datetime(start_date)
    end_time = pd.to_datetime(end_date)
    today = datetime.today()  # Use UTC to match API expectations

    # Ensure the end date is not in the future
    if end_time > today:
        raise ValueError("end_date cannot be in the future.")

    params = {
        "start": start_time.isoformat() + "Z",  # Format as ISO 8601
        "end": end_time.isoformat() + "Z",     # Format as ISO 8601
        "timeframe": timeframe  # Adjust timeframe as needed (e.g., "1Min", "5Min", "1Day")
    }

    url = f"{base_url}/{ticker}/bars"
    r = requests.get(url, headers=headers, params=params)
    
    if r.status_code == 200:
        data = r.json().get('bars', [])
        for row in data:
            row['t'] = parse_time(row['t'])

        # Define trading hours in UTC
        trading_start = datetime.strptime("09:30", "%H:%M").replace(tzinfo=pytz.timezone("US/Eastern"))
        trading_end = datetime.strptime("16:00", "%H:%M").replace(tzinfo=pytz.timezone("US/Eastern"))

        # Filter for only regular trading hours
        filtered_data = []
        for row in data:
            candle_time = row['t'].astimezone(pytz.timezone("US/Eastern"))
            if trading_start.time() <= candle_time.time() <= trading_end.time():
                filtered_data.append(row)

        return filtered_data
    else:
        r.raise_for_status()

def get_dividend_dates(ticker):
    print('Getting Dividend Dates')
    url = f"https://api.polygon.io/v3/reference/dividends?ticker={ticker}&apiKey={polygon_key}"
    results = []
    i = 0
    while url:
        if 'apiKey' not in url:
            url = url + f'&apiKey={polygon_key}'
        
            
        r = requests.get(url)
        
        if r.status_code != 200:
            raise Exception(f"Failed to fetch data: {r.status_code} - {r.text}")
        
        data = r.json()
        results.extend(data.get('results', []))
        
        url = data.get('next_url')
        if i > 0: # Can't hit this Url more than 5 times a minute
            time.sleep(12) 
        i = i + 1

    
    return results



def analyze(row):
    ex_div_date = pd.to_datetime(row['ex_dividend_date'])
    candles = pd.DataFrame(row['candles'])
    candles['t'] = pd.to_datetime(candles['t'])
    candles['date'] = candles['t'].dt.date
    candles = candles.groupby('date')[['vw']].mean()
    candles = candles.reset_index()
    candles['div_date'] = pd.to_datetime(candles['date']) == pd.to_datetime(ex_div_date)
    candles['time'] = candles.index - candles['div_date'].idxmax()
    candles = candles.set_index('time')
    return candles
    
    
def process():
    message = get_message()
    if not message: 
        return False
    message_body = json.loads(message['Body'])
    ticker = message_body['ticker']

    divs = get_dividend_dates(ticker)

    final = []
    for row in tqdm(divs, 'Calculating Historical Performance '): 
        ex_div_date = row['ex_dividend_date']
        record_date = row['record_date']

        begin = pd.to_datetime(record_date) - pd.offsets.BDay(4)
        end = pd.to_datetime(record_date) + pd.offsets.BDay(4)
        try:
            candles = get_candles(ticker, begin, end, '1Hour')
            row['candles'] = candles
            final.append(row)
        except Exception as e:
            
            pass

    results = []
    for thing in final: 
        try:
            ans = analyze(thing)
            if -1 in ans.index:
                base_price = ans.loc[-1]['vw']
                ans['vw'] = ans['vw']/base_price
                ans['date'] = thing['record_date']
                results.append(ans)
        except Exception as e: 
            e

    if len(results) > 0:
        x = pd.concat(results).reset_index()    
        stats = x.groupby('time')['vw'].describe().loc[0]
        drop = min(stats.loc['75%'] , stats.loc['25%'])
        drop = round(1 - float(drop),4)
        yeild = round(float(message_body['yeild']),4)

        if drop < yeild:
            text = f"""
            Potential Trade: EXPECTED PROFIT: {round(100 * (yeild- drop),4)}  %
            
            {json.dumps(message_body)}
            
            EXPECTED DROP: {100 * drop} %
            
            EXPECTED YIELD: {100 * yeild} %
            
            
            
            STATS
            {stats}
            """
            notify(text)
            print("Profit predicted emailing")
        
        if drop > yeild:
            print("NEGATIVE PROFIT PREDICTED")
    if len(results) == 0:
        print("Not enough history found")
            
    clear_message(message)
    return True
    
secret = get_secret()
alpaca_key = secret['alpaca_key']
alpaca_secret = secret['alpaca_secret']
polygon_key = secret['polygon_key']

while True: 
    ans = process()
    if not ans: 
        print("Exiting")
        sys.exit(0)
    for num in tqdm(range(0,12), desc = 'Cooling off'):
        time.sleep(1)
