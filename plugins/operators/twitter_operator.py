from airflow.models import BaseOperator, DAG
# apply_defaults: em uma DAG pode haver parâmetros padrões que vai ser
# enviad para todos os operadores, esse decorator ajudar aplicar eles
# ---não é usado mais, já vem por padrão---
# from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook
from datetime import datetime
from pathlib import Path

import json

class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    def __init__(self, query, file_path, conn_id = None, start_time = None, end_time = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        #retorno caminho total do arquivo SEM o nome final do caminho
        # Path(self.file_path).parent
        Path(Path(self.file_path).parent).mkdir(parents = True, exist_ok = True) 

    def execute(self, context):
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time
        )

        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii = False)
                output_file.write("\n")

if __name__ == "__main__":
    with DAG(
        "TwitterTest",
        start_date = datetime.now()
    ) as dag:
        to = TwitterOperator(
            query = "AluraOnline",
            file_path = "AluraOnline_{{ ds_nodash }}.json",
            task_id = "twitter_test_run"
        )
        
        to.run()