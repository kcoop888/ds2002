import json
import requests
from datetime import date

stock = input()
# stock = "GOOG"
# print(stock)

total = {}

header_var = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit\
    /537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}

url = 'https://query1.finance.yahoo.com/v7/finance/quote'
queryStr = {"symbols": stock}
response = requests.request("GET", url, headers=header_var, params=queryStr)
stock_json = response.json()

if stock_json['quoteResponse']['error'] is None:
    total["Name Ticker"] = stock_json['quoteResponse']['result'][0]['symbol']
    try:
        total["Full Stock Name"] = stock_json['quoteResponse']['result'][0]['longName']
        total["Current Price"] = stock_json['quoteResponse']['result'][0]['regularMarketPrice']
    except KeyError:
        print("No longName or regularMarketPrice")

url2 = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
queryStr2 = {"symbol": stock, "modules": "financialData"}
response2 = requests.request("GET", url2, headers=header_var, params=queryStr2)
stock_json_fd = response2.json()

if stock_json_fd['quoteSummary']['error'] is None:
    total["Target Mean Price"] = \
        stock_json_fd['quoteSummary']['result'][0]['financialData']['targetMeanPrice']['fmt']
    total["Cash on Hand"] = stock_json_fd['quoteSummary']['result'][0]['financialData']['totalCash']["longFmt"]

url3 = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
queryStr3 = {"symbol": stock, "modules": "defaultKeyStatistics"}
response3 = requests.request("GET", url3, headers=header_var, params=queryStr3)
stock_json_sum = response3.json()

if stock_json_sum['quoteSummary']['error'] is None:
    total["Profit Margins"] = \
        stock_json_sum['quoteSummary']['result'][0]['defaultKeyStatistics']['profitMargins']['fmt']

today = date.today()
total["date"] = today.isoformat()

total_json = json.dumps(total)

print(total_json)
