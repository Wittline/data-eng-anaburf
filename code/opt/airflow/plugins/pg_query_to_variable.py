from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
import logging as log
import pandas as pd
import io



class pgQueryToVariable(BaseOperator):
    
    
    @apply_defaults
    def __init__(self,
        postgres_conn_id = "",
        variable = "",
        column = "",
        query = "",
        *args,
        **kwargs):
        
        
        
        self.postgres_conn_id = postgres_conn_id
        self.variable = variable
        self.column = column
        self.query = query
        super().__init__(*args, **kwargs)

    
    def execute(self, context):

        target_hook = PostgresHook(self.postgres_conn_id)
        df = target_hook.get_pandas_df(sql= self.query)
        models = ','.join("'" + model + "'" for model in df[self.column].tolist())
        Variable.set(self.variable, models)
        print(models)