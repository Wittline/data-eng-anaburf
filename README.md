# data-eng-anaburf
R&D Data engineering challenge Frubana

<img width="2048" alt="Untitled (1)" src="https://user-images.githubusercontent.com/8701464/190950090-233a037f-deb9-4b00-a0a2-aa21e31dbda4.png">

# Language and libs
- Python
- Pandas
- Postgresql
- SQL
- Airflow

# Exercises

## 1. Install Postgresql, Airflow, Python, etc.

I will use a **docker-compose.yaml** file to install everything using docker containers, in total there will be 8 containers:

### **Apache Airflow Environment**
- Web server (Apache Airflow)
- Metadata (PostgreSQL)
- Scheduler (Apache Airflow)
- Queue (redis)
- Worker (Celery)
- Monitoring task and workers (Flower)

### **Database Environment**
- Destination DB: (PostgreSQL)
- Source Demo (PostgreSQL)


 ```yaml
version: '3.8'

x-airflow-common:
  &airflow-common  
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.1.1-python3.8}
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
    AIRFLOW__API__AUTH__BACKEND: 'airflow.api.auth.backend.basic_auth'
  volumes:
    - ./opt/airflow/dags:/opt/airflow/dags
    - ./opt/airflow/logs:/opt/airflow/logs
    - ./opt/airflow/plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-50000}"
  depends_on:
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  redis:
    image: redis:latest
    ports:
      - 6379:6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always

  airflow-worker:
    <<: *airflow-common
    command: celery worker
    restart: always

  airflow-init:
    <<: *airflow-common
    command: version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}

  flower:
    <<: *airflow-common
    command: celery flower
    ports:
      - 5555:5555
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always

  db_destination:
    container_name: pg_container_destination
    image: postgres
    restart: always
    environment:
        POSTGRES_USER: "postgres"
        POSTGRES_PASSWORD: "12345"
        POSTGRES_DB: "db"
        POSTGRES_HOST_AUTH_METHOD: trust
    ports:
        - "5961:5432"
  

  db_source:
    container_name: pg_container_source
    image: postgres:14.1-alpine
    restart: always
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    ports:
      - '5105:5432'
    volumes: 
      - db:/var/lib/postgresql/data
      - ./db/init.sql:/docker-entrypoint-initdb.d/demo-small-en-20170815.sql


volumes:
  postgres-db-volume:  
  db:
    driver: local
```

### **How to install everything**

- Install <a href="https://www.stanleyulili.com/git/how-to-install-git-bash-on-windows/">git-bash for windows</a>, once installed , open **git bash** and download this repository, this will download the **dags** folder and the **docker-compose.yaml** file, and other files needed.

``` 
ramse@DESKTOP-K6K6E5A MINGW64 /c
$ git clone https://github.com/Wittline/data-eng-anaburf.git
```

- Install <a href="https://docs.docker.com/docker-for-windows/install/">Docker Desktop on Windows</a>, it will install **docker compose** as well, docker compose will alow you to run multiple containers applications, Apache airflow has three main components: **metadata database**, **scheduler** and **webserver**, in this case we will use a celery executor too.

- Once all the files needed were downloaded from the repository , Let's run everything we will use the git bash tool again, go to the folder **data-eng-anaburf** we will run docker compose command.


```linux 
ramse@DESKTOP-K6K6E5A MINGW64 /c
$ cd data-eng-anaburf

ramse@DESKTOP-K6K6E5A MINGW64 /c/data-eng-anaburf
$ cd code
```

```linux 
ramse@DESKTOP-K6K6E5A MINGW64 /c/data-eng-anaburf/code
$ echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

```linux
ramse@DESKTOP-K6K6E5A MINGW64 /c/data-eng-anaburf/code
$ docker-compose up airflow-init
```

```linux 
$ docker-compose up
```


- Everything is up and running, the next thing to do is go to your browser and search <a href="http://localhost:8080/">http://localhost:8080</a>  this will call your webserver and would open your AIRFLOW GUI, for this example the <strong>user</strong> and <strong>password</strong> is "airflow", you can change it in your <strong>.yaml</strong> file.


**There is a couple of thing to configure to ensure the successful execution of your DAG:**

- Once inside your AIRFLOW GUI, In the header menu -> Admin -> Variables, Import the variables needed, they are located into the variables folder in the repository downloaded, import the **variables.json** file

![variables](https://user-images.githubusercontent.com/8701464/190950267-d2574ae1-7a96-43b7-902c-1e4b41e212eb.JPG)



- Now go to Admin -> Connections and put the Connections needed, in this case the two PostgreSQL db instances already created as a docker containers.

![conn](https://user-images.githubusercontent.com/8701464/190950246-fdbd69f8-12c2-4d68-b72a-f7ae3fb4ec0a.JPG)


### Running DAG

- Open a new git-bash console and put the below command, it will show you a list of containers with their ids 

```linux
$ docker ps
```

![dockerps](https://user-images.githubusercontent.com/8701464/190950377-69fbbf89-4683-4fb7-ae90-68be2ea9dd3c.JPG)


- The scheduler are putting the dags for running, so, choose the container id related with the scheduler and execute the command below, it will execute the dags


```linux
$ docker exec 59f776ba04ae airflow dags trigger frubana_test_dag
```

- The other way for execute dags is using the GUI, which is easier to understand and manage


![fru_dag](https://user-images.githubusercontent.com/8701464/190950505-709d9c38-ff39-496d-80b6-bb1bd10b09a2.JPG)



## 2. Install this database (might be the small version):
<a href="https://postgrespro.com/education/demodb">https://postgrespro.com/education/demodb</a>

![demodb-bookings-schema](https://user-images.githubusercontent.com/8701464/190950297-7c9cb1f5-174b-484a-97a9-fc838bcbb6aa.JPG)


- I downloaded the file: <a href="https://edu.postgrespro.com/demo-small-en.zip">https://edu.postgrespro.com/demo-small-en.zip</a> , and placed it into the "db" folder in this repository, once  the container start building the image, it will perform the execution of this whole script **docker-entrypoint-initdb.d/demo-small-en-20170815.sql** inside the container.

check the section below inside the **docker-compose.yaml** file:

 ```yaml
  db_source:
    container_name: pg_container_source
    image: postgres:14.1-alpine
    restart: always
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    ports:
      - '5105:5432'
    volumes: 
      - db:/var/lib/postgresql/data
      - ./db/init.sql:/docker-entrypoint-initdb.d/demo-small-en-20170815.sql
```

I did some modifications to the original file, I modified the line 17 with: **DROP DATABASE IF EXISTS demo;**, so REMEMBER add this file to the folder **db** and modify this line.


## 3. Setup a python environment (might be Anaconda or anything else)

**The python environment is already provided by the apache airflow ecosystem.**

### Using Airflow:

Once the questions were analyzed, I proceeded to design only 5 custom operators, this to avoid creating a lot of code and reusing functionality.

- **dataframes_merge_to_pg.py**: This will execute two queries, the results will be stored into two pandas dataframe, then it will apply a merge operation between both using two columns as keys. The final result will be stored in a PostgreSQL table. 
- **pg_query_condition.py**: This will execute a query inside a PostgreSQL db If and only if a specific variable allows it,
- **pg_query_to_pg_staging.py**: This operator executes a query in a source PostgreSQL database and the result will be stored in a table in a destination PostgreSQL database.
- **pg_query_to_variable.py**: This will execute a query inside a PostgreSQL database and the result for a specific  column will be stored in a variable.
- **pg_query.py**: This will only execute a query inside a PostgreSQL database.

The DAG is shown below:

![dag_img](https://user-images.githubusercontent.com/8701464/190950571-35567d2d-46eb-440e-a49c-f7d9a729edd5.JPG)


- **a. Create a query to retrieve flights related to planes having the most used aircraft model (tip: use CTE - Common Table Expressions), and load the data in a Pandas DataFrame.**


This part of the test will be separated in two tasks:

The execution of the task **pgQueryToVariable_1** will use the query **get_most_used_aicraft_model** this will store the most used aircraft models in a variable, then this variable will be used by the task **pgQueryToPgStaging_1** through the query **get_flights_with_most_used_aicraft_model**. the final result is moved to an staging table: **staging_1**

**Apache Airflow should not be used to store variables locally**


- **b. Create another query to retrieve tickets booked in the last 6 months, along with their amount. Put the data into a DataFrame.**

The execution of the task **pgQueryToPgStaging_2** will use the query **get_tickets_booked_last_6_months**, the result will be moved to an staging table: **staging_2**

- **c. Merge both datasets (4.a) and (4.b) above into a single one, to get all the flights and their tickets, using Pandas functionality. Save the result in a table.**

The task **dataframesMergeToPg_1** will use the result of the last two queries stored in an staging area and will perform the merge as dataframes, the result will be stored in another staging table: **staging_3**

 ```python
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
```

- **d. Find the average ticket count for each of the aircraft models that were most used, i.e. those in the result of 4.c.**

The task **average_ticket_per_model** is performing this last calculation through the query **get_average_count_tickets** and is storing the result in another staging table: **staging_4**


## 4. Write a python program to find all the cities that could be reached through sequences of flights when starting from an airport (e.g. KHV).

 ```python
 
def track_itinerary(dictionary, departure, airports):
    destination = dictionary.get(departure)
    if not destination:
        return
 
    print(airports[departure] + ' — —> ' + airports[destination])
    track_itinerary(dictionary, destination,airports)
 
 
def get_cities_from_flights(tickets, airports):
      
    destinations = {*tickets.values()}
 
    
    for departures, v in tickets.items():

        if departures not in destinations:        
            track_itinerary(tickets, departures, airports)
            return
 
 
if __name__ == '__main__':
 
    airports = {

        "AR1": "Argentina, BUENOS AIRES",
        "BA1": "Brazil, Sao paulo",
        "MA1": "Mexico, CDMX",
        "MA2": "Mexico, CANCUN",
        "US1": "USA, Houston",
        "US2": "USA, Seattle"
    }
    tickets = {
        'AR1': 'BA1',
        'MA2': 'MA1',
        'US1': 'MA2',
        'MA1': 'AR1'
    }
 
    get_cities_from_flights(tickets, airports)

# USA, Houston — —> Mexico, CANCUN
# Mexico, CANCUN — —> Mexico, CDMX
# Mexico, CDMX — —> Argentina, BUENOS AIRES
# Argentina, BUENOS AIRES — —> Brazil, Sao paulo
```


# Contributing and Feedback
Any ideas or feedback about this repository?. Help me to improve it.

# Authors
- Created by <a href="https://www.linkedin.com/in/ramsescoraspe"><strong>Ramses Alexander Coraspe Valdez</strong></a>
- Created on September 2022

## License
MIT License
