import pandas as pd

class selectmodule:
    def __init__(self, df, args):
        self.args = args
        self.df = df
    
    def get_df(self):
        return self.df
    
    @classmethod
    def load_df(cls, filepath, args):
        df = pd.read_csv(filepath, sep=",")
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

if __name__ == "__main__":
    import os
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date", dest="date", action="store")
    
    args = parser.parse_args()
    if args.date is None:
        print("날짜를 입력하세요.")
        print("예시: python main --date 20240415")
        sys.exit()
        
    date = args.date
    year = date[:4]
    day = date[4:]
    
    filepath = "C:\\Users\\Suseong Kim\\Documents\\Rstudy\\quant\\" + year + "_퀀트투자\\" + day
    filename = "quant_raw.csv"
    final_path = os.path.join(filepath, filename)
    preprocess = selectmodule.load_df(final_path)
    preprocess.filter_columns()
    preprocess.rename_columns()
    print(preprocess.get_df().head())
    
    