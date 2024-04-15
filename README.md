# 퀀트투자 종목선정기

- 파일경로는 main.py에서 수정가능합니다. filepath 변수를 수정해 사용하세용.
- pandas만 설치하시면 됩니다. ```$ pip install pandas```

- 위 경로에 ```quant_raw.csv``` 파일을 반드시 저장한 뒤에 실행해 주세요.
- (출처파일: 퀀트킹에서 다운로드)

### 가상환경 활성화
```sh
$ source quant\\Scripts\\activate
```
### 명령어 실행
```sh
$ python main.py --date 20240415
```
### 옵션 참고. date는 필수 인자

- ```-n``` 또는 ```--except_ncav```  -> ncav 필터 적용
- ```-a``` 또는 ```--except_admin_filter``` -> 관리종목 미제외
- ```-r``` 또는 ```--refactor_perpbr``` -> per, pbr 필터기준 재지정
- ```-rk``` 또는 ```--rank_select``` -> 지표, 가중치 재설정
- rank 평가의 default value는 gpa, pcr, psr 지표를 이용합니다. 각각 rank의 선형결합으로 최종 순위를 평가합니다. 
- example: ```total_rank = gpa_rank * 0.5 + pcr_rank * 0.3 + psr_rank * 0.2``` 

### 실행결과
- 결과파일은 수정한 경로의 ```quant_final.csv```파일을 참조하세요.