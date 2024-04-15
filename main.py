# 파일경로수정시 48번째 line을 참고하세요.
import sys
import os
import argparse

##자체모듈 로드
import preprocess
import filtering
import evaluation

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date", dest="date", action="store")
    parser.add_argument("-n", "--except_ncav", dest="except_ncav", action="store_true")
    parser.add_argument("-a", "--except_admin_filter", dest="except_admin_filter", action="store_true")
    parser.add_argument("-r", "--refactor_perpbr", dest="refactor_perpbr", action="store_true")
    parser.add_argument("-rk", "--rank_select", dest="rank_select", action="store_true")
    args = parser.parse_args()

    if args.date is None:
        print("날짜를 입력하세요.")
        print("예시: python main.py --date 20240415")
        sys.exit()

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
    
    date = args.date
    year = date[:4]
    day = date[4:]

    #여기를 수정하세요! (저장하는 path와 호환됨.)
    filepath = "C:\\Users\\Suseong Kim\\Documents\\Rstudy\\quant\\" + year + "_퀀트투자\\" + day
    # ------------------------------------------------------------------------------------------
    
    filename = "quant_raw.csv"
    final_path = os.path.join(filepath, filename)
    
    selecting = preprocess.selectmodule.load_df(final_path, args)
    selecting.fit()
    df = selecting.get_df()
    
    #유효성 검증
    valid_filter = filtering.valid_stock_filtering(df, args)
    valid_filter.fit()
    df = valid_filter.get_df()
    filename = "quant_valid.csv"
    final_path = os.path.join(filepath, filename)
    df.to_csv(final_path, encoding="cp949", index=False)

    quantile_filter = filtering.filter_by_quantile(df, args)
    quantile_filter.fit()
    df = quantile_filter.get_df()
    filename = "quant_filter.csv"
    final_path = os.path.join(filepath, filename)
    df.to_csv(final_path, encoding="cp949", index=False)
    
    evaluator = evaluation.evaluator(df, args, rank_list, weight_list)
    evaluator.fit()
    df = evaluator.get_df()
    filename = "quant_final.csv"
    final_path = os.path.join(filepath, filename)
    df.to_csv(final_path, encoding="cp949", index=False)

