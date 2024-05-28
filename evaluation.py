import pandas as pd

class evaluator():
    def __init__(self, df, args, rank_list, weight_list):
        self.df = df
        self.args = args
        self.rank_list = rank_list
        self.weight_list = weight_list
    
    def get_df(self):
        return self.df
    
    def fit(self):
        self.make_rank_columns()
        self.make_score_columnns()
        self.transfrom_order()
    
    def make_score_columnns(self):
        self.df["total_rank"] = 0
        for i, criterion in enumerate(self.rank_list):
            self.df["total_rank"] += self.df[criterion + "_score"] * self.weight_list[i]
    
    def make_rank_columns(self):
        for criterion in self.rank_list:
            ascending = True
            if ("gpa" in criterion) or ("roe" in criterion):
                ascending = False
            self.df[criterion + "_score"] = self.df[criterion].rank(ascending=ascending)
    
    def transfrom_order(self):
        columns = self.df.columns.tolist()
        ordered_columns = columns[:4] + [columns[-1]] + columns[4:-1]
        self.df = self.df[ordered_columns]
        self.df = self.df.sort_values(by='total_rank')
    
    def save_df(self, filepath=None):
        if filepath is None:
            self.filepath = filepath
        self.df.to_csv(self.filepath, encoding=self.args.encoding, index=False)