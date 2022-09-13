# bigquery_module

- [bigquery_module](#bigquery_module)
  - [1. 개요](#1-개요)
  - [2. 사용법](#2-사용법)
  - [3. 구현된 함수들](#3-구현된-함수들)
    - [1. bq_create_table(table_id, table_schema_list)](#1-bq_create_tabletable_id-table_schema_list)
    - [2. bq_get_query_result(target_query, print_affected_row=False)](#2-bq_get_query_resulttarget_query-print_affected_rowfalse)
    - [3. bq_df_upload_to_table(table_id, table_schema_list, target_df)](#3-bq_df_upload_to_tabletable_id-table_schema_list-target_df)
    - [4. bq_insert_json(table_id, listOfDict_row_to_insert)](#4-bq_insert_jsontable_id-listofdict_row_to_insert)

## 1. 개요
- Google cloud python bigquery 라이브러리에서 자주 쓰게 되는 기능들을 사용하게 편하게 모듈로 만든 결과물 
- 사용하기 위해선 아래 준비사항들이 필요하다.
  - https://github.com/googleapis/python-bigquery
  - https://developers.google.com/identity/protocols/oauth2#serviceaccount


## 2. 사용법
1. 아래 코드를 최상단에 추가
```Python
import sys
sys.path.append("모듈 파일이 있는 폴더 경로")
import bigquery # bigquery.py 모듈 불러옴
```
2. 클래스 인스턴스 생성 with 서비스 계정 json 파일
```Python
bq = bigquery.bigquery_module("서비스 계정 json 파일 경로")
```

3. 아래와 같이 원하는 함수를 불러와서 사용하자.
```Python
table_id = "원하는 테이블 ID"
# 스키마 예시
table_schema_list = [ 
    ('map_key', "INTEGER", "해당 던전의 map_key"),
    ('desc','STRING','설명'),
    ('battle_power','INTEGER','해당 던전의 적정 전투력'),
]
bq.bq_create_table(table_id, table_schema_list)
```

## 3. 구현된 함수들
### 1. bq_create_table(table_id, table_schema_list)
- 테이블 생성 함수
- Parameters
    1. table_id
        - "projectName.datasetName.tableName" 형식의 테이블 id
    2. table_schema_list
        - 테이블 스키마 정보는 최소 컬럼명, 컬럼 타입을 가지고 있어야 함
        - https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html
        ```Python
            table_schema =[
                bigquery.SchemaField('ios_product_id', "STRING")
                ,bigquery.SchemaField('android_product_id', "STRING")
                ,bigquery.SchemaField('name', "STRING")
                ,bigquery.SchemaField('company', "STRING")
                ,bigquery.SchemaField('ios_url', "STRING")
                ,bigquery.SchemaField('android_url', "STRING")
            ]
        ```    
        - 아래처럼 parameter를 넘겨주면, 함수 안에서 변환하여 적용함
        ```Python
            table_schema = [
            ('game_name', "STRING", '별도로 지정한 게임 이름')
            ,('build_description', "STRING", '어느 국가 빌드인지에 대한 설명')
            ,('appannie_appid_android', "STRING", '앱애니 안드로이드 appid')
            ,('appannie_appid_ios', "STRING", '앱애니 ios appid')
            ,('appannie_unified_game_name', "STRING", '앱애니 통합 관리 게임명')
            ,('android_category', "STRING", '앱애니 안드로이드 기준 게임 카테고리')
            ]
            # 컬럼명, 자료형, 컬럼설명 순서
        ```
- 생성이 완료되면, print로 어떤 테이블이 만들어졌는지 출력됨
    - `Created table ###.FOR_MONITERING.test_power`

### 2. bq_get_query_result(target_query, print_affected_row=False)
- SQL 쿼리를 문자열로 넣으면 실행해주는 함수
- Parameters
    1. target_query
        - 문자열 타입의 SQL 쿼리
        ```Python
        query = """
        DECLARE c_date DATE;
        SET c_date = current_date('Asia/Seoul');

        SELECT
        server_group_id,
        rank_contents_type,
        season_number,
        week_number,
        c_date,
        FORMAT_DATE("%Y%m%d", c_date) AS c_date_str
        FROM `###.$$$.fix_arena_season_info__20220825`
        WHERE rank_contents_type = 1
        """
        ```
    2. print_affected_row
        - Insert나 Update 사용 시 영향을 받은 행 수의 출력 여부를 결정하는 parameter
        - 기본 False이고, True로 하면 영향을 받은 행 수를 print로 출력한다.
        - 주 용도는 airflow에서 dag 실행 시 insert가 잘 되었는지 log 메뉴에서 확인하는 것이다.

### 3. bq_df_upload_to_table(table_id, table_schema_list, target_df)
- pandas dataframe을 그대로 insert 하고 싶을 때 사용
- parameters
    1. table_id
        - "projectName.datasetName.tableName" 형식의 테이블 id
    2. table_schema_list
        - [1. bq_create_table(table_id, table_schema_list)](#1-bq-create-table-table-id--table-schema-list-)의  table_schema_list와 같음
    3. target_df
        - insert 하고 싶은 dataframe
        - astype으로 반드시 자료형 bigquery의 테이블과 맞춰주어야 함
            - [astype 사용법](https://limyj0708.github.io/fastpages/python/pandas/2021/11/05/pandas_cheatsheet.html#6-3.-astype-:-%ED%83%80%EC%9E%85-%EB%B3%80%EA%B2%BD.-Bigquery%EC%97%90-df-%EC%97%85%EB%A1%9C%EB%93%9C-%EC%8B%9C-%EB%B0%98%EB%93%9C%EC%8B%9C-%EC%82%AC%EC%9A%A9)

### 4. bq_insert_json(table_id, listOfDict_row_to_insert)
- json을 insert 하고 싶을 때 사용
- parameters
    1. table_id
        - "projectName.datasetName.tableName" 형식의 테이블 id
    2. listOfDict_row_to_insert
        - json이라고는 하였지만, 결국 python에서는 dictionary를 원소로 가지는 list를 넣어주게 됨
        - 예를 들면...
        ```json
        rows_to_insert = [
            {"full_name": "Phred Phlyntstone", "age": 32},
            {"full_name": "Wylma Phlyntstone", "age": 29},
        ]
        ```
        - key는 컬럼 이름, value는 해당 row, 해당 컬럼의 값