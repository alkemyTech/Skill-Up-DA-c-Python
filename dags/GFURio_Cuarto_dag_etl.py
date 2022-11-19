import logging
import logging.config
from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import (dag, task)
from pathlib import Path

from plugins.mudule_Nz.extract import std_extract
from plugins.mudule_Nz.transform import std_transform

rio_cuarto_sql = r'/usr/local/airflow/include/GFUNRioCuarto.sql'
rio_cuarto_csv = r'/usr/local/airflow/include/GFUNRioCuarto.csv'
rio_cuarto_txt = r'/usr/local/airflow/include/GFUNRioCuarto.txt'

pc_path = r'/usr/local/airflow/include/codigos_postales.csv'

def configure_logger():
            
    LOGGING_CONFIG = Path(__file__).parent.parent/"logger.cfg"
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger("GFURio_Cuarto_dag_etl")
    return logger


@dag(
    schedule=timedelta(minutes=60),
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 5,
        'retry_delay': timedelta(minutes=1)
    },
    tags=['DataProcessing'])
def GFURio_Cuarto_dag_etl():
    
    @task()
    def extract():
        return std_extract(rio_cuarto_sql,rio_cuarto_csv)
    @task()
    def transform(extract):
        logger = configure_logger()
        
        if extract:
            logger.info('preparing Transformation')
            std_kwargs = {
                'dfPath':rio_cuarto_csv,
                'dateBornSchema': "%Y/%b/%d",
                'inscriptionDateSchema': "%y/%b/%d",
                'minAge': 17,
                'maxAge': 82,
                'pathPostalCode': pc_path,
                'target_file': rio_cuarto_txt,
                'yyBornDate':True, 
            }
            try:
                std_transform(**std_kwargs)
                logger.info('transformatrion completed')
                logger.info('------------------------------------------------------')
            except:
                logger.critical('transformation failure')
        return extract
 
    @task()
    def load():
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        logging.info('PandaslOAD')
        
    transform(extract())
    load()

GFURio_Cuarto_dag_etl = GFURio_Cuarto_dag_etl()
