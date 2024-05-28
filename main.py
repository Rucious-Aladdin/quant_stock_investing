# 파일경로수정시 48번째 line을 참고하세요.
import sys
import os
import argparse

##자체모듈 로드
import preprocess
import filtering
import evaluation
import rawdata

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--except_ncav", dest="except_ncav", action="store_true")
    parser.add_argument("-a", "--except_admin_filter", dest="except_admin_filter", action="store_true")
    parser.add_argument("-r", "--refactor_perpbr", dest="refactor_perpbr", action="store_true")
    parser.add_argument("-rk", "--rank_select", dest="rank_select", action="store_true")
    parser.add_argument("-e", "--encoding", dest="encoding", action="store")
    
    args = parser.parse_args()

    if not os.path.exists("./data"):
        os.makedirs("./data")

    if args.refactor_perpbr:
        print("write refactored per, pbr:", end=" ")
        per, pbr = map(float, input().split())
        args.per = per
        args.pbr = pbr
    
    if args.rank_select:
        print("write your criteria:", end=" ")
        rank_list = list(input().split())
        print("write weight list: ", end="")
        weight_list = list(map(float, input().split()))
        if len(rank_list) != weight_list:
            print("같은수 만큼 입력해야 합니다.")
            exit()
    else: # gpa = 0.5, psr = 0.3, pcr = 0.2 select criterion as default evaluation method
        rank_list = ["gpa", "psr", "pcr"]
        weight_list = [0.5, 0.3, 0.2]

    if args.encoding is None:
        args.encoding = "cp949"
    
    cralwling = rawdata.CrawilingModule()
    df = cralwling.make_df()
    filepath = "./data/0_quant_kor_ticker.csv"
    cralwling.save_df(filepath)
    
    filepath = "./data/"
    filename = "1_quant_extracted_data.csv"
    final_path = os.path.join(filepath, filename)
    
    selecting = preprocess.selectmodule.load_df(final_path, args)
    selecting.fit()
    selected_df = selecting.get_df()
    
    valid_filter = filtering.valid_stock_filtering(selected_df, args)
    valid_filter.fit()
    valid_filter.save_df(os.path.join(filepath, "2_quant_valid.csv"))
    valid_df = valid_filter.get_df()
    
    quantile_filter = filtering.filter_by_quantile(valid_df, args)
    quantile_filter.fit()
    quantile_filter.save_df(os.path.join(filepath, "3_quant_filter.csv"))
    quantile_df = quantile_filter.get_df()
    
    evaluator = evaluation.evaluator(quantile_df, args, rank_list, weight_list)
    evaluator.fit()
    evaluator.save_df(os.path.join(filepath, "4_quant_final.csv"))

