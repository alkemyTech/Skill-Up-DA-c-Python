import logging
import logging.config
from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import (dag, task)

from plugins.mudule_Nz.extract import std_extract
from plugins.mudule_Nz.transform import std_transform

from pathlib import Path
def configure_logger():
            
    LOGGING_CONFIG = Path(__file__).parent.parent/"logger.cfg"
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger("GFUMoron_dag_etl")
    return logger


moron_sql = r'/usr/local/airflow/include/GFUMoron.sql'
moron_csv = r'/usr/local/airflow/include/GFUMoron.csv'
moron_txt = r'/usr/local/airflow/include/GFUMoron.txt'

pc_path = r'/usr/local/airflow/include/codigos_postales.csv'




@dag(
    schedule=timedelta(minutes=60),
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 5,
        'retry_delay': timedelta(minutes=1)
    },
    tags=['DataProcessing'])
def GFUMoron_dag_etl():
    
    @task()
    def extract():
        
        logger = configure_logger()
        logger.info('extract-init')
        
        
        return std_extract(moron_sql,moron_csv)
    @task()
    def transform(extract):
        logger = configure_logger()
        
        if extract:
            logger.info('preparing Transformation')
            std_kwargs = {
                'dfPath':moron_csv,
                'dateBornSchema': "%d/%m/%Y",
                'inscriptionDateSchema': "%d/%m/%Y",
                'minAge': 17,
                'maxAge': 82,
                'pathPostalCode': pc_path,
                'target_file': moron_txt
            }
            try:
                std_transform(**std_kwargs)
                logger.info('transformatrion completed')
                logger.info('------------------------------------------------------')
            except:
                logger.critical('transformation failure')
        return extract

    @task()
    def load(prev_task):
        print(prev_task)
        logger = configure_logger()
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        
    load(transform(extract()))

GFUMoron_dag_etl = GFUMoron_dag_etl()










"""
version: "3.1"
services:
  scheduler:
    volumes:
      - ./files:/usr/local/airflow/files:rw
      - ./dags/logs:/usr/local/airflow/mylogs:rw
"""