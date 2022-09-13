from google.cloud import bigquery
from google.oauth2 import service_account
import os

class bigquery_module:
    def __init__(self, credential_json):
        #self.credential_json = credential_json
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_json
    
        # 빅쿼리 테이블 생성 함수
    def bq_create_table(self, table_id, table_schema_list):
        table_schema = []
        """
        table_id = "projectName.datasetName.tableName" 형식의 테이블 정보
        
        - 테이블 스키마 정보는 아래와 같아야 함
        table_schema =[
                bigquery.SchemaField('ios_product_id', "STRING")
                ,bigquery.SchemaField('android_product_id', "STRING")
                ,bigquery.SchemaField('name', "STRING")
                ,bigquery.SchemaField('company', "STRING")
                ,bigquery.SchemaField('ios_url', "STRING")
                ,bigquery.SchemaField('android_url', "STRING")
            ]
            
        - 그래서 넘겨줘야 하는 table_schema_list의 형태는 아래와 같음
        table_schema = [
            ('game_name', "STRING", '별도로 지정한 게임 이름')
            ,('build_description', "STRING", '어느 국가 빌드인지에 대한 설명')
            ,('appannie_appid_android', "STRING", '앱애니 안드로이드 appid')
            ,('appannie_appid_ios', "STRING", '앱애니 ios appid')
            ,('appannie_unified_game_name', "STRING", '앱애니 통합 관리 게임명')
            ,('android_category', "STRING", '앱애니 안드로이드 기준 게임 카테고리')
            ]
         -- 컬럼명, 자료형, 컬럼설명 순서
        """
        client = bigquery.Client()
        
        for each_element in table_schema_list:
            table_schema.append(bigquery.SchemaField(each_element[0],each_element[1], description = each_element[2]))
        # Construct a BigQuery client object.

        # TODO(developer): Set table_id to the ID of the table to create.
        table = bigquery.Table(table_id, schema=table_schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    def bq_get_query_result(self, target_query, print_affected_row=False):
        """
        - 질의 쿼리 결과를 반환하는 함수
         -- target_query : SQL 쿼리를 문자열로 넣으면 된다.
         -- select의 결과로는 iterable 오브젝트가 반환되는데, 반복적으로 호출은 불가능하다.
          --- 이중 리스트로 구현된 행렬에서 원소를 참조한다고 생각하면 사용법이 간단하다. 아래처럼.
          --- for each_row in result:
                  print(each_row[0], each_row[1], ...)
        """
        
        
        client = bigquery.Client()
        query_job = client.query(target_query)
        results = query_job.result()  # Waits for job to complete.
        
        if print_affected_row == False:
            pass
        else:
            check_child_exist = False
            affected_rows = 0
            for child_job in client.list_jobs(parent_job=query_job.job_id):
                check_child_exist = True
                if child_job.num_dml_affected_rows is not None:
                    affected_rows += child_job.num_dml_affected_rows
            
            if check_child_exist == False:
                affected_rows = query_job.num_dml_affected_rows

            print(f"추가, 혹은 업데이트된 행의 수 : {affected_rows}")
        
        
        return results # iterable 오브젝트가 반환되는데, 반복적으로 호출은 불가능하다.
    

    def bq_df_upload_to_table(self, table_id, table_schema_list, target_df):
        """
        table_id = "projectName.datasetName.tableName" 형식의 테이블 정보
        
        - 테이블 스키마 정보는 아래와 같아야 함
        table_schema =[
                bigquery.SchemaField('ios_product_id', "STRING")
                ,bigquery.SchemaField('android_product_id', "STRING")
                ,bigquery.SchemaField('name', "STRING")
                ,bigquery.SchemaField('company', "STRING")
                ,bigquery.SchemaField('ios_url', "STRING")
                ,bigquery.SchemaField('android_url', "STRING")
            ]
            
        - 그래서 넘겨줘야 하는 table_schema_list의 형태는 아래와 같음
        table_schema = [
            ('game_name', "STRING", '별도로 지정한 게임 이름')
            ,('build_description', "STRING", '어느 국가 빌드인지에 대한 설명')
            ,('appannie_appid_android', "STRING", '앱애니 안드로이드 appid')
            ,('appannie_appid_ios', "STRING", '앱애니 ios appid')
            ,('appannie_unified_game_name', "STRING", '앱애니 통합 관리 게임명')
            ,('android_category', "STRING", '앱애니 안드로이드 기준 게임 카테고리')
            ]
         -- 컬럼명, 자료형, 컬럼설명 순서
        
        target_df = 업로드 할 dataframe
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