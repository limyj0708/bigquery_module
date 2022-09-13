from google.cloud import bigquery
from google.oauth2 import service_account
import os

class bigquery_module:
    def __init__(self, credential_json):
        #self.credential_json = credential_json
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_json
    
    def bq_create_table(self, table_id, table_schema_list):
        """
        Parameters
        ----------
        1. table_id : str
            - "projectName.datasetName.tableName" 형식의 테이블 id
        2. table_schema_list : list(원소는 tuple)
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

        Returns
        -------
        반환값은 없음
        """
        table_schema = []
        client = bigquery.Client() # Construct a BigQuery client object.
        
        for each_element in table_schema_list: # table_schema_list를 bigquery library에 맞게 재구성
            table_schema.append(bigquery.SchemaField(each_element[0],each_element[1], description = each_element[2]))

        # TODO(developer): Set table_id to the ID of the table to create.
        table = bigquery.Table(table_id, schema=table_schema)
        table = client.create_table(table) # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    def bq_get_query_result(self, target_query, print_affected_row=False):
        """
        - SQL 쿼리를 문자열로 넣으면 실행해주는 함수
        
        Parameters
        ----------
        1. target_query : str
            - 문자열 타입의 SQL 쿼리
            ```Python
            query = '''
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
            '''
            ```
        2. print_affected_row : boolean
            - Insert나 Update 사용 시 영향을 받은 행 수의 출력 여부를 결정하는 parameter
            - 기본 False이고, True로 하면 영향을 받은 행 수를 print로 출력한다.
            - 주 용도는 airflow에서 dag 실행 시 insert가 잘 되었는지 log 메뉴에서 확인하는 것이다. 
        
        Returns
        -------
        - 쿼리 결과물인 iterable object가 반환된다.
            - google.cloud.bigquery.table.RowIterator
            - 이 Iterator에서 row 하나씩 추출하면 아래처럼 생긴 object가 나온다.
            - `Row((100000, 'dddddd', 2222), {'map_key': 0, 'description': 1, 'battle_power': 2})`
            -  row 클래스에 대한 설명 : [google.cloud.bigquery.table.Row](https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.Row#google_cloud_bigquery_table_Row_get)
            -  기본적으로는, object[0] 이렇게 튜플에서 각 원소에 접근하는 방식으로 행의 값을 참조할 수 있다.
            -  object.get("map_key")로 특정 컬럼을 지정해서 값을 참조할 수도 있다. 자세한 설명은 위의 링크 참조.
        - select 같이 반환 결과물이 있는 쿼리가 아니라면 (drop, insert 등) google.cloud.bigquery.table._EmptyRowIterator가 반환된다.
        """
        
        client = bigquery.Client()
        query_job = client.query(target_query)
        results = query_job.result()  # Waits for job to complete.
        
        if print_affected_row == False:
            pass
        else:
            check_child_exist = False # 기본적으로는 child job이 없다고 가정한다.
            affected_rows = 0 # 영향받은 열 수
            for child_job in client.list_jobs(parent_job=query_job.job_id): # child job이 없으면 그냥 넘어가게 됨
                check_child_exist = True # child job이 있으면, True로 바꿔줌
                if child_job.num_dml_affected_rows is not None: # child job의 DML에 의해 영향받은 열을 담고 있는 변수가 None이 아니라면
                    affected_rows += child_job.num_dml_affected_rows # 더해 준다.
            
            if check_child_exist == False:
                affected_rows = query_job.num_dml_affected_rows

            print(f"추가, 혹은 업데이트된 행의 수 : {affected_rows}")
        
        
        return results # iterable 오브젝트가 반환되는데, 반복적으로 호출은 불가능하다.
    

    def bq_df_upload_to_table(self, table_id, table_schema_list, target_df):
        """
        pandas dataframe을 insert하는 함수

        Parameters
        ----------
        1. table_id : str
            - "projectName.datasetName.tableName" 형식의 테이블 id
        2. table_schema_list : list(원소는 tuple)
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
        3. target_df : dataframe
            - insert 하고 싶은 dataframe
            - astype으로 반드시 자료형을 bigquery의 테이블과 맞춰주어야 함
            
        Returns
        -------
        반환값은 없음
        """

        table_schema = []
        client = bigquery.Client()
        
        for each_element in table_schema_list:
            table_schema.append(bigquery.SchemaField(each_element[0],each_element[1]))
                                                     
        job_config = bigquery.LoadJobConfig(schema=table_schema)

        job = client.load_table_from_dataframe(
            target_df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        #lg.info("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
        
        
    def bq_insert_json(self, table_id, listOfDict_row_to_insert):
        """
        Json 형태의 데이터를 insert하는 함수

        Parameters
        ----------
        1. table_id : list
            - "projectName.datasetName.tableName" 형식의 테이블 id
        2. listOfDict_row_to_insert : list(원소는 dict)
            - json이라고는 하였지만, 결국 python에서는 dictionary를 원소로 가지는 list를 넣어주게 됨
            - 예를 들면...
            ```Python
                rows_to_insert = [
                    {"full_name": "Phred Phlyntstone", "age": 32},
                    {"full_name": "Wylma Phlyntstone", "age": 29},
                ]
            ```
            - key는 컬럼 이름, value는 해당 row, 해당 컬럼의 값
        
        Returns
        -------
        반환값은 없음
        """
        
        # Construct a BigQuery client object.
        client = bigquery.Client()

        # TODO(developer): Set table_id to the ID of table to append to.
        # table_id = "your-project.your_dataset.your_table"

        #rows_to_insert = [
        #    {"full_name": "Phred Phlyntstone", "age": 32},
        #    {"full_name": "Wylma Phlyntstone", "age": 29},
        #]

        errors = client.insert_rows_json(table_id, listOfDict_row_to_insert)  # Make an API request.
        if errors == []:
            print(f"New {len(listOfDict_row_to_insert)} rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))