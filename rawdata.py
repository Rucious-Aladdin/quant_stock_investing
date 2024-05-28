import requests as rq
import re
import pandas as pd
import numpy as np
import os
from bs4 import BeautifulSoup
from io import BytesIO

class CrawilingModule:
    def __init__(self, url=None, down_url=None):
        if down_url is None:
            self.down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        else:
            self.down_url = down_url
        if url is None:
            self.url = url = 'https://finance.naver.com/sise/sise_deposit.nhn'
        else:
            self.url = url
        self.data = rq.get(url)
        self.data_html = BeautifulSoup(self.data.content)
        self.parse_day = self.data_html.select_one(
        'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text

        # 영업일 양식 변경 및 추출
        self.biz_day = re.findall('[0-9]+', self.parse_day)
        self.biz_day = ''.join(self.biz_day)
    
    
    def fit(self):
        self.gen_otp()
        self.post_otp()
        self.concat_sector_df()
        self.merge_df()
        return self.kor_ticker    
    
    def gen_otp(self):
        # OTP 생성
        self.gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        self.gen_otp_stk = {
            'mktId': 'STK',
            'trdDd': self.biz_day,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
        }
        self.gen_otp_ksq = {
        'mktId': 'KSQ',  # 코스닥 입력
        'trdDd': self.biz_day,
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
        }
        self.gen_otp_url_ind = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        self.gen_otp_data_ind = {
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': self.get_bizday(),
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
        }
        self.headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
        self.otp_stk = rq.post(self.gen_otp_url, self.gen_otp_stk, headers=self.headers).text
        self.otp_ksq = rq.post(self.gen_otp_url, self.gen_otp_ksq, headers=self.headers).text
        self.otp_ind = rq.post(self.gen_otp_url_ind, self.gen_otp_data_ind, headers=self.headers).text
        self.krx_ind = rq.post(self.down_url, {'code': self.otp_ind}, headers=self.headers)    
        self.krx_ind = pd.read_csv(BytesIO(self.krx_ind.content), encoding='EUC-KR')
        self.krx_ind['종목명'] = self.krx_ind['종목명'].str.strip()
        self.krx_ind['기준일'] = self.get_bizday()
    
    def post_otp(self):
        down_sector_stk = rq.post(self.down_url, {'code': self.otp_stk}, headers=self.headers)
        down_sector_ksq = rq.post(self.down_url, {'code': self.otp_ksq}, headers=self.headers)
        
        # 코스피 self.sector_stk.columns = ["종목코드", "종목명", "시장구분", "업종명", "종가", "대비", "등락률", "시가총액"]
        self.sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
        # 코스닥 self.sector_stk.columns = ["종목코드", "종목명", "시장구분", "업종명", "종가", "대비", "등락률", "시가총액"]
        self.sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding='EUC-KR ')
    

    def concat_sector_df(self):
        self.krx_sector = pd.concat([self.sector_stk, self.sector_ksq]).reset_index(drop=True)
        self.krx_sector['종목명'] = self.krx_sector['종목명'].str.strip()
        self.krx_sector['기준일'] = self.get_bizday()
    
    def merge_df(self):
        diff = list(set(self.krx_sector['종목명']).symmetric_difference(set(self.krx_ind['종목명'])))
        self.kor_ticker = pd.merge(self.krx_sector,
                      self.krx_ind,
                      on=self.krx_sector.columns.intersection(
                          self.krx_ind.columns).tolist(),
                      how='outer')
        self.kor_ticker['종목구분'] = np.where(self.kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                              np.where(self.kor_ticker['종목코드'].str[-1:] != '0', '우선주',
                                       np.where(self.kor_ticker['종목명'].str.endswith('리츠'), '리츠',
                                                np.where(self.kor_ticker['종목명'].isin(diff),  '기타',
                                                '보통주'))))
        self.kor_ticker = self.kor_ticker.reset_index(drop=True)
        self.kor_ticker.columns = self.kor_ticker.columns.str.replace(' ', '')
        self.kor_ticker = self.kor_ticker[['종목코드', '종목명', '시장구분', '종가',
                                '시가총액', '기준일', 'EPS', 'BPS', '주당배당금', '종목구분']]
        self.kor_ticker = self.kor_ticker.replace({np.nan: None})
        self.kor_ticker['기준일'] = pd.to_datetime(self.kor_ticker['기준일'])
    
    def save_df(self, filename="quant_kor_ticker.csv"):
        self.kor_ticker.to_csv(filename, encoding="cp949", index=False)
        print(f"{filename} 으로 저장 완료.")
        
    
    def get_kor_ticker(self):
        return self.kor_ticker
    
    def get_parseday(self):
        return self.parse_day

    def get_bizday(self):
        return self.biz_day
    
    def get_otp_code(self):
        return self.otp_stk
    

if __name__ == "__main__":
    cralwling = CrawilingModule()
    df = cralwling.make_df()
    print(df.head())
    print(df.shape) # -> (2680, 10)
    filepath = "./data/quant_kor_ticker.csv"
    cralwling.save_df(filepath)