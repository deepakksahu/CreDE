import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator \
    import KubernetesPodOperator
from airflow.models import Variable

DEFAULT_ARGS = {
    'owner': 'de',
    'email': 'deepaksahu092@gmail.com',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 8, 31)
}

IMAGE_CONFIG = Variable.get('cred_images_config', deserialize_json=True)
CONFIG = Variable.get('cred_de_pipeline_conf',
                      deserialize_json=True)

with DAG(
        'data-pipeline',
        default_args=DEFAULT_ARGS,
        schedule_interval='0 * * * *' # Making it hourly job
) as dag:
    KubernetesPodOperator(
        namespace='Cred_Dataengineering',
        image=IMAGE_CONFIG['flat-data-ingestion'],
        cmds=["python", "main.py",
              "--config", json.dumps(CONFIG)],
        name="flat-data-ingestion",
        task_id="data-pipeline",
        in_cluster=True
    )

