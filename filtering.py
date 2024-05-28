import pandas as pd

class valid_stock_filtering:
    def __init__(self, df, args):
        self.df = df
        print(self.df.describe())
        self.args = args

    def get_df(self):
        return self.df
    
    def fit(self):
        self.filter_by_company_type()
        self.kor_company_filter()

    def filter_by_company_type(self):
        # "holding", "financial", "spac", "ritz", "admin"
        cond = (self.df["holding"] == 0) & (self.df["spac"] == 0) & \
            (self.df["ritz"] == 0) & (self.df["financial"] == 0)
        self.df = self.df[cond]
        
        if not self.args.except_admin_filter:
            self.df = self.df[self.df["admin"] == 0]
        
    def kor_company_filter(self):
        self.df = self.df[self.df["isnotkor"] == 0]
    
    def save_df(self, filepath=None):
        if filepath is None:
            self.filepath = filepath
        self.df.to_csv(self.filepath, encoding=self.args.encoding, index=False)

class filter_by_quantile:
    def __init__(self, df, args):
        self.args = args
        self.df = df
        
        ## filtering 인수
        self.per_upper = 30
        self.pbr_lower = 0.2
        self.small_cap_index = None
        self.pcr_index = None
        self.psr_index = None
        self.per_index = None
        self.roe_index = None
        self.roa_index = None
        self.debt_rto = None
        self.ncav_low = None
        self.ncav_high = None
    
    def get_df(self):
        return self.df
    
    def fit(self):
        #refactoring 설정
        if self.args.refactor_perpbr:
            self.refactoring_fixed_value()
        
        #분위수 계산
        self.get_quantile()
        #필터 적용
        self.market_cap_filter()
        self.value_filter()
        self.financial_soundness_filter()
        self.roe_filter()
        self.fscore_filter()
        self.per_pbr_filter()
        self.ncav_filter()
    # na drop은 여기에.
    def get_quantile(self):
        self.per_upper = 30
        self.pbr_lower = 0.2
        #nan drop
        self.df = self.df.dropna(axis=0, how="any")
        
        #분위수 추출
        self.small_cap_q = .2;  self.pbr_q = .8; self.pbr_q_low = .05 ;self.psr_q = .8
        self.per_q = .8; self.pcr_q = .9; self.roe_q = .9; self.debt_rto_q = .9
        self.ncav_low_q = 0.35; self.ncav_high_q = 0.6
        
        self.small_cap_index = self.df["market_capitalization"].quantile(q=self.small_cap_q)
        self.pbr_index = self.df[self.df["pbr"] > 0]["pbr"].quantile(q=self.pbr_q)
        self.pbr_index_low = self.df[self.df["pbr"] > 0]["pbr"].quantile(q=self.pbr_q_low)
        self.psr_index = self.df[self.df["psr_quarter"] > 0]["psr_quarter"].quantile(q=self.psr_q)
        self.per_index = self.df[self.df["per_quarter"] > 0]["per_quarter"].quantile(q=self.per_q)
        self.pcr_index = self.df[self.df["pcr_quarter"] > 0]["pcr_quarter"].quantile(q=self.pcr_q)
        self.roe_index = self.df[self.df["roe_quarter"] > 0]["roe_quarter"].quantile(q=self.roe_q)
        self.debt_rto = self.df[self.df["debt_ratio"] > 0]["debt_ratio"].quantile(q=self.debt_rto_q)
        self.ncav_low = self.df["ncav"].quantile(q=self.ncav_low_q)
        self.ncav_high = self.df["ncav"].quantile(q=self.ncav_high_q)
        self.print_quantile()

    def market_cap_filter(self):
        self.df = self.df[self.df["market_capitalization"] <= self.small_cap_index]

    def value_filter(self):
        cond = (self.df["psr_quarter"] <= self.psr_index) & (self.df["pbr"] <= self.pbr_index) & \
            (self.df["per_quarter"] <= self.per_index) & (self.df["pcr_quarter"] <= self.pcr_index) & \
            (self.df["pbr"] >= self.pbr_index_low)
        self.df = self.df[cond]
        self.df = self.df[self.df["pcr_quarter"] > 0]

    def roe_filter(self):
        self.df = self.df[self.df["roe_quarter"] <= self.roe_index]
        self.df = self.df[self.df["roe_quarter"] > 0]

    def financial_soundness_filter(self):
        self.df = self.df[self.df["debt_ratio"] <= self.debt_rto]
        
    def fscore_filter(self):
        #"F_net_income", "F_cashflowing", "F_new_stock"
        cond = (self.df["F_net_income"] == 1) & (self.df["F_cashflowing"] == 1) & (self.df["F_new_stock"] == 1)
        self.df = self.df[cond]
    
    def per_pbr_filter(self):
        cond = (self.df["per_quarter"] <= self.per_upper) & (self.df["pbr"] >= self.pbr_lower)
        self.df = self.df[cond]
    
    def ncav_filter(self):
        if self.args.except_ncav:
            cond = (self.df["ncav"] > self.ncav_low) & (self.df["ncav"] < self.ncav_high)
            self.df = self.df[cond]
    
    def refactoring_fixed_value(self):
        self.per = float(self.args.per)
        self.pbr = float(self.args.pbr)
    
    def save_df(self, filepath=None):
        if filepath is None:
            self.filepath = filepath
        self.df.to_csv(self.filepath, encoding=self.args.encoding, index=False)

    def print_quantile(self):
        print("-"*15, end="")
        print("분위수 출력", end="")
        print("-"*15)
        x0 = "하위 " + str(int(101 * self.small_cap_q)) + "%"
        x1 = "상위 " + str(int(101 * (1-self.pbr_q))) + "% (자본잠식 제외)"
        x2 = "하위 " + str(int(101 * self.pbr_q_low)) + "% (적자제외)"
        x3 = "상위 " + str(int(101 * (1-self.psr_q))) + "% (적자제외)"
        x4 = "상위 " + str(int(101 * (1-self.per_q))) + "% (적자제외)"
        x5 = "상위 " + str(int(101 * (1-self.pcr_q))) + "% (적자제외)"
        x6 = "상위 " + str(int(101 * (1-self.roe_q))) + "% (적자제외)"
        x7 = "상위 " + str(int(101 * (1-self.debt_rto_q))) + "% (완전자본잠식제외)"
        print("%-20s %6d %-20s" % ("market_cap_index", self.small_cap_index, x0))
        print("%-20s %6.2f %-20s" % ("pbr_high", self.pbr_index, x1))
        print("%-20s %6.2f %-20s" % ("pbr_low", self.pbr_index_low, x2))
        print("%-20s %6.2f %-20s" % ("psr_index", self.psr_index, x3))
        print("%-20s %6.2f %-20s" % ("per_index", self.per_index, x4))
        print("%-20s %6.2f %-20s" % ("pcr_index", self.pcr_index, x5))
        print("%-20s %6.2f %-20s" % ("roe_index", self.roe_index, x6))
        print("%-20s %6.2f %-20s" % ("debt_index", self.debt_rto, x7))
        