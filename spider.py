import requests
import time
import re
import json
from bs4 import BeautifulSoup
from DBTools import log

__all__ = ['TTMJSpider']

class Spider(object):
    def __init__(self, url, depth, payload):
        self.url = url
        self.depth = depth
        self.payload = payload
        self.html = None

    def post_requests(self):
        return None

    def requests(self, method='get', url=None, data=None, cookie=None):
        if not url:
            url = self.url
        if method == 'get':
            return requests.get(url, headers=self.payload.get('headers', None), verify=False,cookies=cookie)
        elif method == 'post':
            return requests.post(url, headers=self.payload.get('headers', None), verify=False, data=data,cookies=cookie)

    def downloader(self,method='get',payload={}):
        while self.payload['retry_times'] > 0:
            try:
                response = self.requests(method=method, url=payload.get('url',None), data=payload.get('data',None),cookie=payload.get('cookie',None))
            except Exception as err:
                print(err)
                log.logger.info('trying to retry...')
                self.payload['retry_times'] -= 1
                time.sleep(self.payload['retry_time'])
            else:
                self.html = response.text
                return response.text

    def get_links(self):
        html = self.downloader(payload={'cookie':TTMJSpider.cookie,})
        links = []
        def is_link(link):
            check_link = lambda x:re.match(
                """(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]""", x)
            if check_link(link):
                illegal_words = ['java',]
                for word in illegal_words:
                    if word in link:
                        return False
                return True
            else: return False
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            self.soup = soup
        else:
            log.logger.error('cannot get the page')
            return None
        tags = soup.find_all('a')
        for tag in tags:
            link = tag.get('href')
            if not link:
                return None
            if 'http' not in link:
                link = "https://www.ttmeiju.com" + link
            if is_link(link):
                try:
                    index = link.index('#')
                    link = link[:index]
                except ValueError: pass
                if 'www.ttmeiju.com' in link:
                    links.append(link)
            else:
                pass
        if self.depth > 0:
            return links
    def html_parser(self):
        return None

class TTMJSpider(Spider):
    session = None
    cookie = None
    def __init__(self, url, depth, payload):
        Spider.__init__(self, url, depth, payload)

    def html_parser(self):
        if not self.html:
            return None
        if 'meijuid' in self.html:
            try:
                index = int(self.html.index('meijuid')) + 7
                mid = re.findall('[\d]*;',self.html[index:index + 10])[0][:-1]
                sid = len(self.soup.select(".seasonitem")[0].find_all('h3'))
            except Exception:
                return None
            if not id:
                return None
            for i in range(0, sid + 1):
                payload = {
                    'url': "http://www.ttmeiju.com/index.php/meiju/get_episodies.html",
                    'data': {'sid':str(i),'mid':str(mid)},
                    'cookie': TTMJSpider.cookie
                }
                respone = self.downloader(method='post',payload=payload)
                respone = json.loads(respone)
                soup = BeautifulSoup(respone['Html_Seedlist'],'html.parser')
                meiju = []
                for tag_tr in soup.find_all('tr'):
                    temp = dict()
                    tag_as = tag_tr.find_all('a')
                    for index in range(len(tag_as)):
                        if index == 0:
                            temp['mj_name'] = tag_as[index].text.strip()
                        else:
                            temp[tag_as[index].get('title')] = tag_as[index].get('href')
                        if temp:
                            meiju.append(temp)

                return meiju
        time.sleep(3)
    def get_session(self):
        data = {"PHPSESSID": "nn6q7h3vvfoh1p72850oi2lus2",
                "OWtE_973b_saltkey": "LnG91K99",
                "bpi_auth": "8fd9CR/5Xt2+wMgWe1lOUH9cxh1Yq7gRn+4kAcXf3iUDbrnsOgKMAgW/ZKdP4kNl7a970gPoMj4XcBD8q+QhGNGoWduROJ+nG7wdV694AzFwk3jCafMjrhRKF/sF2Qw+ig",
                "OWtE_973b_lastvisit": "1506866026",
                "OWtE_973b_sid": "FdzC3Z",
                "OWtE_973b_lastact": "1506913295	 uc.php",
                "OWtE_973b_auth": "5f43m6T1+LFGD14HWC16VmE1Lg9F6nkAanQy6FgrSbQCiBqMdYmVS+ktG9Xx8actTgxz6pVqB58y0sNhH2sYIuZk9N0",}
        respone = requests.get("http://www.ttmeiju.com/index.php/user/islogin.html", headers=self.payload.get('headers', None),
                               verify=False, cookies=data)

        if respone.text.strip():
            log.logger.info("测试Session成功")
            return data
        else:
            log.logger.error("测试Session无效")
            return None


