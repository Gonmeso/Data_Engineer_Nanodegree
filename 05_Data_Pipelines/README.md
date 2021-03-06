# Data Pipelines Using Apache Airflow

A Data Engineering project focused on learning the basics of Pipelines orchestration using Apache Airflow and AWS Redshift.

## Getting Started

The first step is to clone the repository `git clone https://github.com/Gonmeso/Data_Engineer_Nanodegree.git` and then cd into the project `cd 05_Data_Pipelines`


### Folder structure

The project is structured as follows:

```
.
├── README.md
├── config.json
├── dev_env
│   ├── airflow
│   │   ├── create_tables.sql
│   │   ├── dags
│   │   │   └── sparkify_dag.py
│   │   └── plugins
│   │       ├── __init__.py
│   │       ├── helpers
│   │       │   ├── __init__.py
│   │       │   └── sql_queries.py
│   │       └── operators
│   │           ├── __init__.py
│   │           ├── data_quality.py
│   │           ├── load_dimension.py
│   │           ├── load_fact.py
│   │           └── stage_redshift.py
│   ├── aws
│   │   ├── clean.py
│   │   ├── create_tables.py
│   │   ├── deploy.py
│   │   ├── sparkify_cloudformation.yaml
│   │   ├── sql_queries.py
│   │   ├── start.sh
│   │   ├── utils.py
│   │   └── utils.pyc
│   └── requirements.txt
└── docker-compose.yml
```

### Prerequisites

To make this project work you will need the following dependencies:

**Python**: as our main programming language
**virtualenv**: tool to create environments for python, isolating the dependencies for the project
**pip**: Python package installer, used to install the project dependencies
**AWS**: an Amazon Web Services Account in orther to use the Redshift services to build our data warehouse
**Docker**: for running Airflow locally using the `docker-compose.yml` file

#### Preparing our Amazon account

1. Creating an IAM user with admin privileges and programatic access
2. Storing the AWS_ACCESS_KEY and AWS_SECRET_KEY safely in order to run our commands usin `boto3`.
3. Get into de `aws` directory `cd dev_env/aws`
4. Run `start.sh` in order to start the creation of the Redshift cluster and create all the necessary tables, this will execute the `deploy.py` script to create everything that redshift needs and `create_tables.py` that connects to the cluster and creates the tables
5. Now our cluster is ready

#### Creating a Virtual Environment for local execution

 We have to make sure we are using python 3.6 or above as this projects uses some features not included below python 3.6 as `f-strings`. Run the following command in the `dev_env`folder to create an isolated python environment:

```bash
virtualenv --python python3.6 venv
```

Now the environment must be activated in order to install the dependencies:

```bash
source venv/bin/activate
```

And install our dependencies making use of `pip`:

```bash
pip install -r requirements.txt
```

Now we are ready to start the process.


#### Starting Airflow

In order to start Airflow the only thing we need is to get into the projects root dir `05_DataPipelines` and run `docker-compose up`. At the begining Airflow will find a couple of errors of non-defined varible `sparkify` so we need to add it. The `config.json` file is an example of the variable values.

```json
{
    "iam_role": "arn:aws:iam::591628149559:role/sparkify-dwh-role",
    "s3": {
        "log_data": "s3://udacity-dend/log_data",
        "events_data": "s3://udacity-dend/song_data/",
        "log_data_jsonpath": "s3://udacity-dend/log_json_path.json"
    },
    "redshift": {
        "table_staging_events": "staging_events",
        "table_staging_songs": "staging_songs",
        "table_songs": "songs",
        "table_artists": "artists",
        "table_songplays": "songplays",
        "table_users": "users",
        "table_time": "time"
    }
}
```

Also we need to define de AWS credentials (as stated in the project) and the postgres connection. The endpoint can be retrieve through aws console or you can get it when `create_tables.py` is executed as it will be printed out.

Once everything is done we can execute the DAG `Project_4_Dag`. Then the pipeline will execute and will make use of all the Operators code at `airflow/plugins/operators/`


## Authors

* **Gonzalo Mellizo-Soto Díaz**

## Acknowledgments

* Thanks to Udacity for the project!