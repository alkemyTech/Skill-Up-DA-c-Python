import pandas as pd
from datetime import date, datetime, timedelta
import logging
import boto3

from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

# Default settings applied to all tasks

default_args = {
    "owner": "P3",
    # If a task fails, retry it once after waiting
    # at least 60 minutes
    "retries": 5,
    "retry_delay": timedelta(minutes=60),
}

# Instantiate DAG
with DAG(
    "P3-GroupI-Jujuy",
    start_date=datetime(2022, 11, 3),
    max_active_runs=5,
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    @task()
    def extract():
        logging.info('EXTRACT STARTED')
        with open('include/P3_UniJujuy.sql','r',encoding='utf-8') as f:
            sql_script = f.read()
        hook = PostgresHook(postgres_conn_id="alkemy_db")
        df = hook.get_pandas_df(sql=sql_script)
        df.to_csv("files/jujuy.csv")
        logging.info("CSV FILE CREATED")


    @task()
    def transform():
        logging.info('TRANSFORM STARTED')
        # Dict with Postal Codes
        with open('assets/codigos_postales.csv','r',encoding='utf-8') as f:
            cod_post_df = pd.read_csv(f)
            cod_post_df['localidad'] =  cod_post_df['localidad'].str.lower()
            cod_post_dict = dict(zip(cod_post_df.localidad, cod_post_df.codigo_postal))
        # Clean data
        with open("files/jujuy.csv") as f:
            df = pd.read_csv(f,index_col=[0])

        df = df.replace({'postal_code':cod_post_dict})
        df = df.replace({'gender': {'f':'female','m':'male'}})

        #Names
        df.names = df.names.astype(str)
        prefixs =[
            "mr. ",
            "mrs. ",
            "miss ",
            "dr. ",
        ]

        subfixs = [
            ' iv',
            ' dds',
            ' md',
            ' phd',
        ]

        for prefix in prefixs:
            df.names = df.names.str.removeprefix(prefix)
        for subfix in subfixs:
            df.names = df.names.str.removesuffix(subfix)

        df[['first_name','last_name']] = df.names.str.split(" ",n=1,expand=True)
        df = df.drop(['names'], axis=1)

        df.career = df.career.str.strip()

        def age(born):
            born = datetime.strptime(born, "%Y/%m/%d").date()
            today = date.today()
            age =  today.year - born.year - ((today.month, today.day) < (born.month, born.day))
            return age

        df['age'] = df['birth_date'].apply(age)
        df = df.drop(['birth_date'], axis=1)
        df.to_csv("files/jujuy.csv")
        df.to_csv("files/jujuy.txt",sep="\t",index=None)
        logging.info('TXT AND CSV FILES CREATED')

    @task()
    def load():
        logging.info('LOAD STARTED')
        ACCESS_KEY = "AKIASMO2Y7NKXVBNK7QN"
        SECRET_ACCESS_KEY = "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"
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )
        s3 = session.resource("s3")
        data = open("files/jujuy.txt", "rb")
        s3.Bucket("bucket-alk").put_object(
            Key="preprocess/jujuy.txt", Body=data
        )
        logging.info('LOAD SUCCESS')

    extract() >> transform() >> load()




with DAG(
    "P3-GroupI-Moron",
    start_date=datetime(2022, 11, 3),
    max_active_runs=5,
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    @task()
    def extract():
        with open('include/P3_UniMoron.sql','r',encoding='utf-8') as f:
            sql_script = f.read()

        hook = PostgresHook(postgres_conn_id="alkemy_db")
        df = hook.get_pandas_df(sql=sql_script)
        df.to_csv("files/moron.csv")

    @task()
    def transform():
        # Dict with Postal Codes
        with open('assets/codigos_postales.csv','r',encoding='utf-8') as f:
            cod_post_df = pd.read_csv(f)
            cod_post_df['localidad'] =  cod_post_df['localidad'].str.lower()
            cod_post_dict = dict(zip(cod_post_df.codigo_postal, cod_post_df.localidad))

        with open("files/moron.csv") as f:
            df = pd.read_csv(f,index_col=[0])

        df.location = df.location.astype(int)
        df = df.replace({'location': cod_post_dict})
        df = df.replace({'gender': {'F':'female','M':'male'}})

        #Age
        def age(born):
            born = datetime.strptime(born, "%d/%m/%Y").date()
            today = date.today()
            age =  today.year - born.year - ((today.month, today.day) < (born.month, born.day))
            return age

        df['age'] = df['birth_date'].apply(age)
        df = df.drop(['birth_date'], axis=1)

        #Name and LastName
        df.names = df.names.astype(str)
        prefixs =[
            "Mr. ",
            "Mrs. ",
            "Miss ",
            "Dr. ",
        ]

        subfixs = [
            ' iv',
            ' dds',
            ' md',
            ' phd',
        ]

        for prefix in prefixs:
            df.names = df.names.str.removeprefix(prefix)
        for subfix in subfixs:
            df.names = df.names.str.removesuffix(subfix)

        df[['first_name','last_name']] = df.names.str.split(" ",n=1,expand=True)
        df = df.drop(['names'], axis=1)


        #Columns to lower
        df['email'] = df['email'].str.lower()
        df['career'] = df['career'].str.lower()
        df['first_name'] = df['first_name'].str.lower()
        df['university'] = df['university'].str.lower()

        df.to_csv("files/moron.csv")
        df.to_csv("files/moron.txt",sep="\t",index=None)

    @task()
    def load():
        ACCESS_KEY = "AKIASMO2Y7NKXVBNK7QN"
        SECRET_ACCESS_KEY = "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"
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )
        s3 = session.resource("s3")
        data = open("files/moron.txt", "rb")
        s3.Bucket("bucket-alk").put_object(
            Key="preprocess/moron.txt", Body=data
        )

    extract() >> transform() >> load()
