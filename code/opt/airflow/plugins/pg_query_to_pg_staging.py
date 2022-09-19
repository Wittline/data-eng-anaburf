from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
import logging as log
import pandas as pd
import io


class pgQueryToPgStaging(BaseOperator):
    
    
    @apply_defaults
    def __init__(self,
        postgres_conn_source_id = "",
        postgres_conn_destination_id = "",
        query = "",        
        staging_table = "",
        *args,
        **kwargs):
                
        
        self.postgres_conn_source_id = postgres_conn_source_id
        self.postgres_conn_destination_id = postgres_conn_destination_id        
        self.query = query
        self.staging_table = staging_table
        super().__init__(*args, **kwargs)

    
    def execute(self, context):

        source_hook = PostgresHook(self.postgres_conn_source_id)
        target_hook = PostgresHook(self.postgres_conn_destination_id)
        df = source_hook.get_pandas_df(sql= self.query)

        col_str = ', '.join(df.columns.tolist())
        query_insert = f"INSERT INTO {self.staging_table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING;"
        
        rows = list(df.itertuples(index=False, name=None))        
        
        n_rows = []
        for i in range(0, len(rows)-1):            
            r = str(rows[i])
            r = r.replace('NaT', 'CURRENT_TIMESTAMP')
            r = r.replace('Timestamp(', '')
            r = r.replace(", tz='UTC')", '')
            n_rows.append(r)
        
        rows = None                
        target_hook.run(query_insert % ','.join(n_rows))