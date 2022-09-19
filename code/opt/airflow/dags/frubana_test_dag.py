import logging
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.operators.dummy_operator import DummyOperator
from pg_query_to_variable import pgQueryToVariable
from pg_query_to_pg_staging import pgQueryToPgStaging
from dataframes_merge_to_pg import dataframesMergeToPg
from pg_query import pgQuery
from pg_query_condition import pgQueryCondition
import sql_statements



dag = DAG(
     dag_id = 'frubana_test_dag',
     description = 'Frubana demo',
     start_date = datetime.datetime.now(),
     schedule_interval= '@daily',
     tags=['FRUBANA']
)

start = DummyOperator(
    task_id = 'start'
)


pgQueryToVariable_1 = pgQueryToVariable(     
    postgres_conn_id = "postgres_conn_source_id",
    task_id = "pgQueryToVariable_1",
    variable = "most_used_aircraft",
    column = "aircraft_code",
    query = sql_statements.get_most_used_aicraft_model,
    dag=dag
)


create_staging_1 = pgQuery(
    task_id = "create_staging_1",
    postgres_conn_id = 'postgres_conn_destination_id',
    query = sql_statements.create_staging_1,
    dag = dag
)

create_staging_2 = pgQuery(
    task_id = "create_staging_2",
    postgres_conn_id = 'postgres_conn_destination_id',
    query = sql_statements.create_staging_2,
    dag = dag
)

create_staging_3 = pgQuery(
    task_id = "create_staging_3",
    postgres_conn_id = 'postgres_conn_destination_id',
    query = sql_statements.create_staging_3,
    dag = dag
)

create_staging_4 = pgQuery(
    task_id = "create_staging_4",
    postgres_conn_id = 'postgres_conn_destination_id',
    query = sql_statements.create_staging_4,
    dag = dag
)


pgQueryToPgStaging_1 = pgQueryToPgStaging(
    task_id = "pgQueryToPgStaging_1",
    postgres_conn_source_id = 'postgres_conn_source_id',
    postgres_conn_destination_id = 'postgres_conn_destination_id',
    query = sql_statements.get_flights_with_most_used_aicraft_model.format(str(Variable.get('most_used_aircraft'))),
    staging_table = "staging_1",
    dag = dag
)


pgQueryToPgStaging_2 = pgQueryToPgStaging(
    task_id = "pgQueryToPgStaging_2",
    postgres_conn_source_id = 'postgres_conn_source_id',
    postgres_conn_destination_id = 'postgres_conn_destination_id',
    query = sql_statements.get_tickets_booked_last_6_months,
    staging_table = "staging_2",
    dag=dag
)


dataframesMergeToPg_1 = dataframesMergeToPg(
    task_id = "dataframesMergeToPg_1",    
    postgres_conn_id = 'postgres_conn_destination_id',    
    staging_table_1 = "staging_1",
    staging_table_2 = "staging_2",
    staging_final = "staging_3",
    left_on = "flight_id",
    right_on = "flight_id_2",
    dag=dag
)


average_ticket_per_model = pgQuery(
    task_id = "average_ticket_per_model_1",
    postgres_conn_id = 'postgres_conn_destination_id',
    query = sql_statements.get_average_count_tickets,
    dag = dag
)

pgQuery_condition = pgQueryCondition(
    task_id = "drop_tables",
    postgres_conn_id = 'postgres_conn_destination_id',
    query = sql_statements.drop_staging,
    variable = "is_drop_tables",
    dag = dag
)


staging_ready = DummyOperator(
    task_id = 'staging_ready'
)

end = DummyOperator(
    task_id = 'end'
)

start >> [pgQueryToVariable_1, create_staging_1, create_staging_2, create_staging_3, create_staging_4]

[pgQueryToVariable_1, create_staging_1, create_staging_2, create_staging_3, create_staging_4] >> staging_ready

staging_ready >> [pgQueryToPgStaging_1, pgQueryToPgStaging_2]

[pgQueryToPgStaging_1, pgQueryToPgStaging_2] >> dataframesMergeToPg_1

dataframesMergeToPg_1 >> average_ticket_per_model 

average_ticket_per_model >> pgQuery_condition 
pgQuery_condition >> end