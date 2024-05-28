import pandas as pd
import numpy as np
from tqdm import tqdm
import rawdata

class selectmodule:
    def __init__(self, df, args):
        self.args = args
        self.df = df
    
    def get_df(self):
        return self.df
    
    @classmethod
    def load_df(cls, filepath, args):
        df = pd.read_csv(filepath, sep=",", encoding="cp949")
        return cls(df, args)

    def fit(self):
        self.filter_columns()
        self.rename_columns()
    
    def filter_columns(self):
        columns = [
            "코드", "회사명", "시장구분", "주가(원)", "시가총액(억)",
            "GP/A (%)", "분기 GP/A (%)", "ROE (%)", "분기 ROE (%)", "ROA (%)", "분기 ROA (%)",
            "PSR", "분기 PSR", "PER", "분기 PER", "PBR",
            "PCR", "분기 PCR", "청산가치비율 (NCAV전략) (%)",
            "당좌 비율 (%)", "유동 비율 (%)", "부채 비율 (%)",
            "해외 본사 =1", "지주사 =1", "금융 =1", "스팩 =1", "리츠 =1", "관리종목 =1",
            "F스코어 순이익>0 여부", "F스코어 영업활동현금>0 여부", "F스코어 ROA증가 여부", 
            "F스코어 영현>순익 여부", "F스코어 부채비율감소 여부", "F스코어 유동비율증가 여부", 
            "F스코어 신주발행X 여부" , "F스코어 매출총이익률증가 여부" , "F스코어 총자산회전율증가 여부",
            ]
        self.df = self.df[columns]

    def rename_columns(self):
        renamed_columns = [
            "code", "company_name", "market_type", "price", "market_capitalization",
            "gpa", "gpa_quarter", "roe", "roe_quarter", "roa", "roa_quarter",
            "psr", "psr_quarter", "per", "per_quarter", "pbr",
            "pcr", "pcr_quarter", "ncav",
            "quick_ratio", "current_ratio", "debt_ratio",
            "isnotkor", "holding", "financial", "spac", "ritz", "admin",
            "F_net_income", "F_cashflowing", "F_inc_roa",
            "F_cash_bigger_income", "F_dec_debt", "F_inc_current",
            "F_new_stock", "F_inc_GPM", "F_inc_SA"
        ]
        self.df.columns = renamed_columns
    
    def save_df(self, filepath=None):
        if filepath is None:
            self.filepath = filepath
        self.df.to_csv(self.filepath, encoding=self.args.encoding, index=False)
    
class preprocessmodule(selectmodule):
    # crawaling raw data to preprocessded file
    def __init__(self, df, crawler, args):
        super().__init__(df, args)
        self.crawler = crawler        
        self.kor_ticker = self.crawler.get_kor_ticker()
        self.quant_data = None
        print(self.kor_ticker.head())
        
        self.error_list = []
        self.code_list = []
        self.company_list = []
        self.type_list = []
        self.price_list = []
        self.capital_list = []
        self.GPA_list = []
        self.GPA_q_list = []
        self.ROE_list = []
        self.ROE_q_list = []
        self.ROA_list = []
        self.ROA_q_list = []
        self.PSR_list = []
        self.PSR_q_list = []
        self.PER_list = []
        self.PER_q_list = []
        self.PBR_list = []
        self.PCR_list = []
        self.PCR_q_list = []
        self.NCAV_list = []
        self.current_ratio_list = []
        self.quick_ratio_list = []
        self.debt_ratio_list = []
        self.hae_list = []
        self.ji_list = []
        self.geum_list = []
        self.spec_list = []
        self.litz_list = []
        self.manage_list = []
        self.f1_list = []
        self.f2_list = []
        self.f3_list = []
        self.f4_list = []
        self.f5_list = []
        self.f6_list = []
        self.f7_list = []
        self.f8_list = []
        self.f9_list = []
        pass
        
    @classmethod
    def load_df(cls, filepath, crawler, args):
        df = pd.read_csv(filepath, sep=",")
        return cls(df, crawler, args)
    
    def fit(self, raw_path=None):
        if raw_path is None:
            quant_raw = pd.read_csv("./data/quant_raw.csv")
        else:
            quant_raw = pd.read_csv(raw_path)
             
        for i in tqdm(range(0, self.kor_ticker.shape[0])):
            try:
                code = 'A'+ self.kor_ticker.iloc[i]["종목코드"]

                if not (code in quant_raw["코드"].values):
                    raise Exception("종목코드가 raw파일에 없습니다.")

                company = self.kor_ticker.iloc[i]["종목명"]
                type = "코스피" if self.kor_ticker.iloc[i]["시장구분"] == "KOSPI" else "코스닥"
                price = self.kor_ticker.iloc[i]["종가"]
                captial = self.kor_ticker.iloc[i]["시가총액"] / (10 ** 8)

                ticker = self.kor_ticker.iloc[i]["종목코드"]
                url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}'
                data = pd.read_html(url, displayed_only=False)
                
                # 현재 시가총액 (억)
                market_capitalization = captial
                # 작년 매출총이익
                gross_fit_year = data[0][data[0]["IFRS(연결)"] == "매출총이익"].iloc[:,4].values[0]
                # 작년 총자산
                total_assets_year = data[2][data[2]["IFRS(연결)"] == "자산"].iloc[:,4].values[0]
                # 작년 당기순이익
                net_income_year = data[0][data[0]["IFRS(연결)"] == "당기순이익"].iloc[:,4].values[0]
                # 작년 총자본
                capital_year = data[2][data[2]["IFRS(연결)"] == "자본"].iloc[:,4].values[0]
                # 작년 매출액
                take_year = data[0][data[0]["IFRS(연결)"] == "매출액"].iloc[:,4].values[0]
                # 작년 영업활동현금흐름
                cash_flow = data[4][data[4]["IFRS(연결)"] == "영업활동으로인한현금흐름"].iloc[:,4].values[0]
                # 작년 부채 총계
                debt = data[2][data[2]["IFRS(연결)"] == "부채"].iloc[:,4].values[0]
                # 작년 유동자산
                current_assets = data[2][data[2]["IFRS(연결)"] == "유동자산계산에 참여한 계정 펼치기"].iloc[:,4].values[0]
                # 작년 당좌자산
                quick_assets_list = ["유동생물자산", "유동금융자산", "매출채권및기타유동채권", "당기법인세자산", "계약자산", "반품(환불)자산", "배출권", "기타유동자산", "현금및현금성자산", "매각예정비유동자산및처분자산집단"]
                quick_assets = 0
                for name in quick_assets_list:
                    quick_assets += data[2][data[2]["IFRS(연결)"] == name].fillna(0).iloc[:,4].values[0]
                # 작년 유동부채
                current_liabilities = data[2][data[2]["IFRS(연결)"] == "유동부채계산에 참여한 계정 펼치기"].iloc[:,4].values[0]

                # 직전 분기 매출총이익
                gross_fit_quarter = data[1][data[1]["IFRS(연결)"] == "매출총이익"].iloc[:,4].values[0]
                # 직전 분기 총자산
                total_assets_quarter = data[3][data[3]["IFRS(연결)"] == "자산"].iloc[:,4].values[0]
                # 직전 분기 당기순이익
                net_income_quarter = data[1][data[1]["IFRS(연결)"] == "당기순이익"].iloc[:,4].values[0]
                # 직전 분기 총자본
                capital_quarter = data[3][data[3]["IFRS(연결)"] == "자본"].iloc[:,4].values[0]
                # 직전 분기 매출액
                take_quarter = data[1][data[1]["IFRS(연결)"] == "매출액"].iloc[:,4].values[0]
                # 직전 분기 영업활동현금흐름
                cash_flow_quarter = data[5][data[5]["IFRS(연결)"] == "영업활동으로인한현금흐름"].iloc[:,4].values[0]

                # GP/A
                GP_A = gross_fit_year / total_assets_year * 100
                # ROE
                ROE = net_income_year / capital_year * 100
                # ROA
                ROA = net_income_year / total_assets_year * 100
                # PSR
                PSR = market_capitalization / take_year
                # PER
                PER = market_capitalization / net_income_year
                # PBR
                PBR = market_capitalization / capital_year
                # PCR
                PCR = market_capitalization / cash_flow
                # 청산가치비율 (NCAV전략) (%)
                NCAV = (current_assets - debt) / market_capitalization * 100
                # 당좌비율
                quick_ratio = quick_assets / current_liabilities * 100
                # 유동비율
                current_ratio = current_assets / current_liabilities * 100
                # 부채비율
                debt_ratio = debt / capital_year * 100

                # 분기 GP/A
                GP_A_quarter = gross_fit_quarter * 4 / total_assets_quarter * 100
                GP_A_quarter = GP_A if np.isnan(GP_A_quarter) else GP_A_quarter
                # 분기 ROE
                ROE_quarter = net_income_quarter * 4 / capital_quarter * 100
                ROE_quarter = ROE if np.isnan(ROE_quarter) else ROE_quarter
                # 분기 ROA
                ROA_quarter = net_income_quarter * 4 / total_assets_quarter * 100
                ROA_quarter = ROA if np.isnan(ROA_quarter) else ROA_quarter
                # 분기 PSR
                PSR_quarter = market_capitalization / (take_quarter * 4)
                PSR_quarter = PSR if np.isnan(PSR_quarter) else PSR_quarter
                # 분기 PER
                PER_quarter = market_capitalization / (net_income_quarter * 4)
                PER_quarter = PER if np.isnan(PER_quarter) else PER_quarter
                # 분기 PCR
                PCR_quarter = market_capitalization / (cash_flow_quarter * 4)
                PCR_quarter = PCR if np.isnan(PCR_quarter) else PCR_quarter
                
                self.code_list.append(code)
                self.company_list.append(company)
                self.type_list.append(type)
                self.price_list.append(price)
                self.capital_list.append(captial)
                self.GPA_list.append(GP_A)
                self.GPA_q_list.append(GP_A_quarter)
                self.ROE_list.append(ROE)
                self.ROE_q_list.append(ROE_quarter)
                self.ROA_list.append(ROA)
                self.ROA_q_list.append(ROA_quarter)
                self.PSR_list.append(PSR)
                self.PSR_q_list.append(PSR_quarter)
                self.PER_list.append(PER)
                self.PER_q_list.append(PER_quarter)
                self.PBR_list.append(PBR)
                self.PCR_list.append(PCR)
                self.PCR_q_list.append(PCR_quarter)
                self.NCAV_list.append(NCAV)
                self.current_ratio_list.append(current_ratio)
                self.quick_ratio_list.append(quick_ratio)
                self.debt_ratio_list.append(debt_ratio)
                
                self.hae_list.append(quant_raw[quant_raw["코드"] == code]["해외 본사 =1"].values[0])
                self.ji_list.append(quant_raw[quant_raw["코드"] == code]["지주사 =1"].values[0])
                self.geum_list.append(quant_raw[quant_raw["코드"] == code]["금융 =1"].values[0])
                self.spec_list.append(quant_raw[quant_raw["코드"] == code]["스팩 =1"].values[0])
                self.litz_list.append(quant_raw[quant_raw["코드"] == code]["리츠 =1"].values[0])
                self.manage_list.append(quant_raw[quant_raw["코드"] == code]["관리종목 =1"].values[0])
                self.f1_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 순이익>0 여부"].values[0])
                self.f2_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 영업활동현금>0 여부"].values[0])
                self.f3_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 ROA증가 여부"].values[0])
                self.f4_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 영현>순익 여부"].values[0])
                self.f5_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 부채비율감소 여부"].values[0])
                self.f6_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 유동비율증가 여부"].values[0])
                self.f7_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 신주발행X 여부"].values[0])
                self.f8_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 매출총이익률증가 여부"].values[0])
                self.f9_list.append(quant_raw[quant_raw["코드"] == code]["F스코어 총자산회전율증가 여부"].values[0])
            except:
                self.error_list.append(code)
        self.quant_data = pd.DataFrame({"코드" : self.code_list, "회사명" : self.company_list, "시장구분" : self.type_list,
                           "주가(원)" : self.price_list, "시가총액(억)" : self.capital_list, "GP/A (%)" : self.GPA_list, "분기 GP/A (%)" : self.GPA_q_list, "ROE (%)" : self.ROE_list,
                           "분기 ROE (%)" : self.ROE_q_list, "ROA (%)" : self.ROA_list, "분기 ROA (%)" : self.ROA_q_list, "PSR" : self.PSR_list, "분기 PSR" : self.PSR_q_list,
                           "PER" : self.PER_list, "분기 PER" : self.PER_q_list, "PBR" : self.PBR_list, "PCR" : self.PCR_list, "분기 PCR" : self.PCR_q_list, "청산가치비율 (NCAV전략) (%)" : self.NCAV_list,
                           "당좌 비율 (%)" : self.quick_ratio_list, "유동 비율 (%)" : self.current_ratio_list, "부채 비율 (%)" : self.debt_ratio_list, "해외 본사 =1" : self.hae_list,
                           "지주사 =1" : self.ji_list, "금융 =1" : self.geum_list, "스팩 =1" : self.spec_list, "리츠 =1" : self.litz_list, "관리종목 =1" : self.manage_list,
                           "F스코어 순이익>0 여부" : self.f1_list, "F스코어 영업활동현금>0 여부" : self.f2_list, "F스코어 ROA증가 여부" : self.f3_list, "F스코어 영현>순익 여부" : self.f4_list,
                           "F스코어 부채비율감소 여부" : self.f5_list, "F스코어 유동비율증가 여부" : self.f6_list, "F스코어 신주발행X 여부" : self.f7_list, "F스코어 매출총이익률증가 여부" : self.f8_list,
                           "F스코어 총자산회전율증가 여부" : self.f9_list})
        
    def save_df(self, filepath=None):
        if filepath is None:
            filepath = f"./data/extracted_data.csv"
        else:
            filepath = filepath
        self.quant_data.to_csv(filepath, index=False, encoding="cp949")
        
if __name__ == "__main__":
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date", dest="date", action="store")    
    args = parser.parse_args()

    
    
    filepath = "./data/"
    filename = "quant_raw.csv"
    final_path = os.path.join(filepath, filename)
    """
    preprocess = selectmodule.load_df(final_path)
    preprocess.filter_columns()
    preprocess.rename_columns()
    print(preprocess.get_df().head())
    """
    
    crawler = rawdata.CrawilingModule()
    crawler.fit()
    model=preprocessmodule.load_df(final_path, crawler, args)
    model.fit()
    model.save_df()