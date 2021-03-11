import datetime
import time
import pandas as pd
import requests
from collections import namedtuple
from twstock.proxy import get_proxies

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

WANTGOO_BASE_URL = 'https://www.wantgoo.com/'
DATATUPLE = namedtuple('Data', ['date', 'volume', 'open', 'high', 'low', 'close'])

class BaseFetcher(object):
    def fetch(self, year, month, sid, retry):
        pass

    def _make_datatuple(self, data):
        pass

    def purify(self, original_data):
        pass

class WantgooFetcher(BaseFetcher):
    REPORT_URL = WANTGOO_BASE_URL
    HEADERS = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'}

    def fetch_info(self, sid: str, retry: int=5):
        for retry_i in range(retry):
            company_profile_response = requests.get(
                self.REPORT_URL + 'stock/' + sid + '/company-profile-data',
                headers = self.HEADERS,
                proxies=get_proxies())
                
            eps_response = requests.get(
                self.REPORT_URL + 'stock/' + sid + '/financial-statements/eps-data',
                headers = self.HEADERS,
                proxies=get_proxies())

            try:
                company_profile = company_profile_response.json()
                outstanding_shares = company_profile['outstandingShares'] if type(company_profile) is dict else 1.0
                eps = eps_response.json()
            except JSONDecodeError:
                continue
            else:
                break
        else:
            # Fail in all retries
            print(sid + 'info fail')

        eps = pd.Series({str(e['year'])+'/'+str(e['season'])+'Q': e['beps'] for e in eps})
        self.info = {
            'id': sid,
            'capital': 1.0,
            'outstanding_shares': outstanding_shares,
            'PER': sum(eps[:4]) if len(eps) >= 4 else None
        }
        self.info.update(eps)
        self.info = pd.Series(self.info)
        return self.info

    def fetch_daily(self, sid: str, num: int, total_stock: int, retry: int=5):
        self.total_stock = total_stock
        params = {'before': int(time.mktime(datetime.datetime.now().timetuple()))*1000, 'top': num}
        for retry_i in range(retry):
            candlesticks_response = requests.get(
                self.REPORT_URL + 'investrue/' + sid + '/historical-daily-candlesticks',
                params = params,
                headers = self.HEADERS,
                proxies=get_proxies())

            institutional_investors_response = requests.get(
                self.REPORT_URL + 'stock/' + sid + '/institutional-investors/trend-data?topdays=20',
                headers = self.HEADERS,
                proxies=get_proxies())

            major_investors_response = requests.get(
                self.REPORT_URL + 'stock/' + sid + '/major-investors/main-trend-data',
                headers = self.HEADERS,
                proxies=get_proxies())

            lending_response = requests.get(
                self.REPORT_URL + 'stock/' + sid + '/margin-trading/historical-lending-balance',
                headers = self.HEADERS,
                proxies=get_proxies())

            borrowing_response = requests.get(
                self.REPORT_URL + 'stock/' + sid + '/margin-trading/historical-borrowing-balance',
                headers = self.HEADERS,
                proxies=get_proxies())

            try:
                candlesticks = candlesticks_response.json()[::-1]
                institutional_investors = institutional_investors_response.json()[::-1]
                major_investors = major_investors_response.json()[::-1]
                lending = lending_response.json()[::-1]
                borrowing = borrowing_response.json()[::-1]
            except JSONDecodeError:
                continue
            else:
                break
        else:
            # Fail in all retries
            print(sid + 'daily fail')
            candlesticks = []
            institutional_investors = []
            major_investors = []
            lending = []
            borrowing = []

        return self.purify(candlesticks, major_investors, institutional_investors, lending, borrowing)

    def purify(self, candlesticks, major_investors, institutional_investors, lending, borrowing):
        candlesticks_data = pd.DataFrame(candlesticks, columns=['volume', 'open', 'close', 'high', 'low'])
        candlesticks_data['date'] = [datetime.datetime.fromtimestamp(d['tradeDate']/1000) for d in candlesticks]

        institutional_investors_data = pd.DataFrame(institutional_investors, columns=[
            'date',
            'sumForeignNoDealer', 'sumForeignWithDealer', 'sumING', 'sumDealerBySelf', 'sumDealerHedging',
            'sumHoldingRate', 'foreignHoldingRate', 'ingHolding']).rename(columns={
                'sumING': 'investment_trust', 
                'sumHoldingRate': 'sum_holding_rate',
                'foreignHoldingRate': 'foreign_holding_rate'})
        institutional_investors_data['date'] = [datetime.datetime.strptime(d['date'], '%Y-%m-%dT%H:%M:%S') for d in institutional_investors]

        major_investors_data = (pd.DataFrame(major_investors, columns=['date', 'stockAgentMainPower', 'stockAgentDiff', 'skp5', 'skp20'])
            .rename(columns={"stockAgentMainPower": "major_investors", "stockAgentDiff": "agent_diff"}))
        major_investors_data['date'] = [datetime.datetime.strptime(d['date'], '%Y-%m-%dT%H:%M:%S') for d in major_investors]

        lending_data = (pd.DataFrame(lending, columns=['date', 'lendingBalance', 'limit'])
            .rename(columns={"lendingBalance": "lending_balance", "limit": "balance_limit"}))
        lending_data['date'] = [datetime.datetime.fromtimestamp(d['date']/1000) for d in lending]

        borrowing_data = (pd.DataFrame(borrowing, columns=['date', 'borrowingBalance'])
            .rename(columns={"borrowingBalance": "borrowing_balance"}))
        borrowing_data['date'] = [datetime.datetime.fromtimestamp(d['date']/1000) for d in borrowing]

        data = pd.merge(candlesticks_data, institutional_investors_data, how='left', on=['date'])
        data = pd.merge(data, major_investors_data, how='left', on=['date'])
        data = pd.merge(data, lending_data, how='left', on=['date'])
        data = pd.merge(data, borrowing_data, how='left', on=['date'])

        data = data.assign(investment_trust_holding_rate=round(data['ingHolding'].astype(float) / self.total_stock * 100, 2))
        data = (data.assign(dealer_holding_rate=round(data['sum_holding_rate'].astype(float) - data['foreign_holding_rate'].astype(float) - data['investment_trust_holding_rate'].astype(float), 2),
            foreign=round((data['sumForeignNoDealer'] + data['sumForeignWithDealer']).astype(float) / self.total_stock  * 100, 2),
            investment_trust=round(data['investment_trust'].astype(float) / self.total_stock  * 100, 2),
            dealer=round((data['sumDealerBySelf'] + data['sumDealerHedging']).astype(float) / self.total_stock * 100, 2)
        ))

        data = data.fillna(0.0)

        return data[[
            'date', 'volume', 'open', 'close', 'high', 'low',
            'foreign', 'investment_trust', 'dealer', 'sum_holding_rate', 'foreign_holding_rate', 'investment_trust_holding_rate', 'dealer_holding_rate',
            'major_investors', 'agent_diff', 'skp5', 'skp20',
            'lending_balance', 'borrowing_balance', 'balance_limit'
        ]]

    def get_all_stock_list(self, retry: int=5):
        for retry_i in range(retry):
            r = requests.get(self.REPORT_URL + 'investrue/all-alive',
                headers = self.HEADERS,
                proxies=get_proxies()
                )
            try:
                data = r.json()
            except JSONDecodeError:
                print('error')
                continue
            else:
                break
        else:
            # Fail in all retries
            print('fail')
            data = []

        filtered = filter(lambda l: l['type'] in ['Stock', 'ETF'], data)
        return list(filtered)