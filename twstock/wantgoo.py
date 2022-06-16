import asyncio
import datetime
import time
from collections import namedtuple
from logging import exception

import httpx
import pandas as pd

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
    HEADERS = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Safari/537.36',
        'cookie': '_smt_uid=629c5867.5fc65746; BID=894FB88F-DE0F-42C3-8C69-2E6CB780E6EC; BrowserMode=Web; _gcl_au=1.1.654098664.1654413417; hblid=1RZJCacNhvYF9fuz3h7B70HDBzFAaoKr; _okdetect=%7B%22token%22%3A%2216544134175230%22%2C%22proto%22%3A%22about%3A%22%2C%22host%22%3A%22%22%7D; olfsk=olfsk31832645327155573; _ok=8391-691-10-7433; _hjSessionUser_827061=eyJpZCI6IjQ2ZDJlYWI2LWE5NTEtNWFkNS05ZDZhLWM5YmNhNzNkZWI5OSIsImNyZWF0ZWQiOjE2NTQ0MTM0MTY3NzMsImV4aXN0aW5nIjp0cnVlfQ==; client_fingerprint=8762d32f0389049e186a5b228fbb2aeaf2a771d1f23d5feba70da91b3ed81a90; _fbp=fb.1.1654877865921.899127606; _gid=GA1.2.1899882238.1655399587; _gat_gtag_UA_6993262_2=1; __cf_bm=EGMLxJL08zraQuGQ7SB0pF5C84Duv9292VDUhGaKLZ0-1655399587-0-AZhqUucWGit8Okn9acq3EBBZKEkmcr7Qw+D1gNYbafWIhzA35T6uEl3EbGpoUSOQfC7M2nBNAm76SqvWJRlwcBDHyhifEuk5B0Jv5VLfQU3nKYuRgwgj8wmuF6x1yiJXmg==; wcsid=bollsFkWekqs4SxW3h7B70Hba6rAF0AB; _okbk=cd4%3Dtrue%2Cvi5%3D0%2Cvi4%3D1655399588067%2Cvi3%3Dactive%2Cvi2%3Dfalse%2Cvi1%3Dfalse%2Ccd8%3Dchat%2Ccd6%3D0%2Ccd5%3Daway%2Ccd3%3Dfalse%2Ccd2%3D0%2Ccd1%3D0%2C; _hjIncludedInSessionSample=0; _hjSession_827061=eyJpZCI6Ijc4ZjA2ZTQ2LTJlMzItNDZmOS1iYjM1LTU2ZmM4OWQ5OWY5NyIsImNyZWF0ZWQiOjE2NTUzOTk1ODk2NjIsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; _oklv=1655399593620%2CbollsFkWekqs4SxW3h7B70Hba6rAF0AB; _ga_FCVGHSWXEQ=GS1.1.1655399586.8.1.1655399602.0; _ga=GA1.2.2072470772.1654413417',
        'x-client-signature': '4e5aebf8960f928932466dc17a59e590df1c9e5efc0645060778c16b1889ec04',
        'referer': 'https://www.wantgoo.com/stock/6505'
    }

    async def fetch_url(self, url: str, params: dict = None, retry: int=5):
        async with httpx.AsyncClient(http2=True) as client:
            for retry_i in range(retry):
                response = await client.get(
                    url,
                    params = params,
                    headers = self.HEADERS
                )
                try:
                    response = response.json()
    
                except JSONDecodeError:
                    await asyncio.sleep(0.1)
                    continue
                else:
                    break
            else:
                print(url + ' fail')
                raise Exception(url + ' fail')

        return response
        
    async def fetch_info(self, sid: str):
        company_profile = self.fetch_url(self.REPORT_URL + 'stock/' + sid + '/company-profile-data')
        eps = self.fetch_url(self.REPORT_URL + 'stock/' + sid + '/financial-statements/eps-data')
        dividend = self.fetch_url(self.REPORT_URL + 'stock/' + sid + '/dividend-policy/ex-dividend-data')
        company_profile, eps, dividend = await asyncio.gather(company_profile, eps, dividend)

        outstanding_shares = company_profile['outstandingShares'] if type(company_profile) is dict else 1.0
        eps = pd.Series({str(e['year'])+'/'+str(e['season'])+'Q': e['beps'] for e in eps})

        cash_dividend = 0
        stock_dividend = 0
        if len(dividend) > 0:
            if dividend[0]['period'] is not None and str(datetime.datetime.now().year - 2011) in dividend[0]['period']:
                cash_dividend = round(dividend[0]['cashDividend'], 2)
                stock_dividend = round(dividend[0]['stockDividend'], 2)

        self.info = {
            'id': sid,
            'capital': 1.0,
            'outstanding_shares': outstanding_shares,
            'PER': eps[0] if len(eps) >= 1 else None,
            'cash_dividend': cash_dividend,
            'stock_dividend': stock_dividend
        }
        self.info.update(eps)
        self.info = pd.Series(self.info)
        return self.info

    async def fetch_daily(self, sid: str, num: int, total_stock: int):
        self.total_stock = total_stock
        params = {'before': int(time.mktime(datetime.datetime.now().timetuple()))*1000, 'top': num}

        candlesticks = self.fetch_url(
            self.REPORT_URL + 'investrue/' + sid + '/historical-daily-candlesticks',
            params
        )

        institutional_investors = self.fetch_url(
            self.REPORT_URL + 'stock/' + sid + '/institutional-investors/trend-data?topdays=20'
        )
        major_investors = self.fetch_url(self.REPORT_URL + 'stock/' + sid + '/major-investors/main-trend-data')

        lending = self.fetch_url(
            self.REPORT_URL + 'stock/' + sid + '/margin-trading/historical-lending-balance'
        )

        borrowing = self.fetch_url(
            self.REPORT_URL + 'stock/' + sid + '/margin-trading/historical-borrowing-balance',
        )
        candlesticks, major_investors, institutional_investors, lending, borrowing = await asyncio.gather(candlesticks, major_investors, institutional_investors, lending, borrowing)

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
        major_investors_data['date'] = [datetime.datetime.fromtimestamp(d['date']/1000) for d in major_investors]

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

    async def get_all_stock_list(self, retry: int=5):
        data = await self.fetch_url(self.REPORT_URL + 'investrue/all-alive')

        filtered = filter(lambda l: l['type'] in ['Index', 'Stock', 'ETF'], data)
        return list(filtered)
