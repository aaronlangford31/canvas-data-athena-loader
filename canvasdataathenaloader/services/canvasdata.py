import base64
from datetime import datetime
import hmac
import requests


class CanvasData():
    def __init__(self, config):
        self.apiurl = config['apiurl']
        self.apikey = config['apikey']
        self.apisecret = config['apisecret']

    def get_latest_files(self):
        return self.make_request('/api/account/self/file/latest')

    def get_lastest_schema(self):
        return self.make_request('/api/schema/latest')

    def make_request(self, route):
        date = datetime\
            .utcnow()\
            .strftime('%a, %d %b %Y %X GMT')

        signature = self.sign_request(route, date)
        headers = {
            'Date': date,
            'Authorization': signature,
            'host': 'api.inshosteddata.com',
            'accept': 'application/json'
        }
        return requests.get(self.apiurl + route, headers=headers).json()

    def sign_request(self, route, date):
        message = """GET
api.inshosteddata.com
{contenttype}
{contentmd5}
{route}
{queryparams}
{date}
{secret}""".format(contenttype='',
                   contentmd5='',
                   route=route,
                   queryparams='',
                   date=date,
                   secret=self.apisecret
                   )
        mod = hmac.new(self.apisecret.encode(), digestmod='sha256')
        mod.update(message.encode())
        digest = base64.b64encode(mod.digest()).decode()

        return "HMACAuth {key}:{signature}".format(key=self.apikey, signature=digest)
