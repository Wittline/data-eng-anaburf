from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
import logging as log
import pandas as pd
import io



class pgQuery(BaseOperator):
    
    
    @apply_defaults
    def __init__(self,
        postgres_conn_id,
        query,    
        *args,
        **kwargs):
                
        
        self.postgres_conn_id = postgres_conn_id
        self.query = query
        super().__init__(*args, **kwargs)

    
    def execute(self, context):

        source_hook = PostgresHook(self.postgres_conn_id)
        source_hook.run(self.query)
        print(" -- Query: ", self.query, " -- READY")