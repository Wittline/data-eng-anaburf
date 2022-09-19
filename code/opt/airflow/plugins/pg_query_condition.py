from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
import logging as log
import pandas as pd
import io


class pgQueryCondition(BaseOperator):

    @apply_defaults
    def __init__(self,
        postgres_conn_id,
        query,
        variable,   
        *args,
        **kwargs):
        
        self.postgres_conn_id = postgres_conn_id
        self.query = query
        self.variable = variable
        super().__init__(*args, **kwargs)

    
    def execute(self, context):

        source_hook = PostgresHook(self.postgres_conn_id)

        value = Variable.get(self.variable)

        if value == "True":
            source_hook.run(self.query)
            print(" -- Query: ", self.query, " -- READY")