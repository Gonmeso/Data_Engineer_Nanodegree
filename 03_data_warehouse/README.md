# Data Modeling With Apache Cassandra

A Data Engineering project focused on learning the basics of Data Modeling using cassandras as the main storage tool, performing ETL's using python.

## Getting Started

The first step is to clone the repository `git clone https://github.com/Gonmeso/Data_Engineer_Nanodegree.git` and then cd into the project `cd 03_data_warehouse`


### Folder structure

The project is structured as follows:

```
├── README.md
└── dev_env
    ├── create_tables.py
    ├── deploy.py
    ├── dwh.cfg
    ├── etl.py
    ├── requirements.txt
    ├── sparkify_cloudformation.yaml
    ├── sql_queries.py
    ├── start.sh
    └── utils.py
```

### Prerequisites

To make this project work you will need the following dependencies:

**Python**: as our main programming language
**virtualenv**: tool to create environments for python, isolating the dependencies for the project
**pip**: Python package installer, used to install the project dependencies
**AWS**: an Amazon Web Services Account in orther to use the Redshift services to build our data warehouse

#### Preparing our Amazon account

1. Creating an IAM user with admin privileges and programatic access
2. Storing the AWS_ACCESS_KEY and AWS_SECRET_KEY safely in order to run our commands usin `boto3`.
3. Create a new Security Group for the Redshift service to allow connections at port *5439*. To ease this process a new inbound rule can be added to the default security group, as I have done for this project.
4. Inlcude all the necessary information at `dwh.cfg. At this point the credentials and the region can be included (make sure you don't commit the aws credentials to git).

#### Creating a Virtual Environment

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


#### Filling the Data Warehouse Configuration File

The project uses a Cloudformation `yaml` template where the infrastructure is declared, for this case the Redshift cluster and the IAM role with S3 access are included. This template has parameters regarding the properties of the cluster and the name of the role. In order to retrieve this parameter programaticaly they have been added to `dwh.cfg`:

```cfg
[IAM]
ROLE_NAME=sparkify-dwh-role

[CLUSTER]
CLUSTER_ID=sparkify-cluster
CLUSTER_TYPE=single-node
NODE_TYPE=ds2.xlarge
NUMBER_OF_NODES=1
DB_NAME=sparkify
DB_USER=admin
DB_PASSWORD=1Q2w3e4r
```

Due to the small quantity of data and low need of resources a single node cluster has been configured. 


### Running the project

In order to create the Data Warehouse a convenience script has been added, so we only need to execute:

```bash
./start.sh
```

This script will execute `deploy.py` focused on the deployment of the cluster and role using the cloudformation template and `boto3` to retrieve the client. Then it will create the tables with `create_tables.py` and perform the ETL in `etl.py`. 


### Cleaning AWS

To clean all the resources we created `clean.py` could be executed, deleting the stack creted in cloudformation:

```bash
python clean.py
```

## Authors

* **Gonzalo Mellizo-Soto Díaz**

## Acknowledgments

* Thanks to Udacity for the project!