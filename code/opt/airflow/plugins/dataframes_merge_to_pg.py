from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
import logging as log
import pandas as pd
import io



class dataframesMergeToPg(BaseOperator):
    
    
    @apply_defaults
    def __init__(self,
            postgres_conn_id,   
            staging_table_1,
            staging_table_2,
            staging_final,
            left_on,
            right_on,
        *args,
        **kwargs):
                
        
        self.postgres_conn_id = postgres_conn_id
        self.staging_table_1 = staging_table_1        
        self.staging_table_2 = staging_table_2
        self.staging_final = staging_final
        self.left_on = left_on
        self.right_on = right_on
        super().__init__(*args, **kwargs)

    
    def execute(self, context):

        source_hook = PostgresHook(self.postgres_conn_id)        
        df1 = source_hook.get_pandas_df(sql= "SELECT * FROM {}".format(self.staging_table_1))
        df2 = source_hook.get_pandas_df(sql= "SELECT * FROM {}".format(self.staging_table_2))
        df3 = pd.merge(df1, df2, left_on=self.left_on, right_on=self.right_on)

        col_str = ', '.join(df3.columns.tolist())
        query_insert = f"INSERT INTO {self.staging_final} ({col_str}) VALUES %s ON CONFLICT DO NOTHING;"
        
        n_rows = []
        rows = list(df3.itertuples(index=False, name=None))
        for i in range(0, len(rows)-1):            
            r = str(rows[i])
            r = r.replace('NaT', 'CURRENT_TIMESTAMP')
            r = r.replace('Timestamp(', '')
            r = r.replace(", tz='UTC')", '')
            n_rows.append(r)
            
        rows = None                
        source_hook.run(query_insert % ','.join(n_rows))