from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id='test_db_connectivity',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['troubleshooting']
) as dag:

    test_network = BashOperator(
        task_id='check_host_visibility',
        bash_command='''
            echo "--- Connection Test ---"
            echo "1. Testing Localhost (Container internal):"
            nc -zv localhost 5432 || echo "RESULT: Localhost failed (Expected)"
            
            echo ""
            echo "2. Testing Docker Gateway (Windows Host):"
            nc -zv host.docker.internal 5432 || echo "RESULT: Host Gateway failed"
            
            echo ""
            echo "3. Testing IP Route:"
            ip route | grep default | awk '{print "Docker Gateway IP: " $3}'
        '''
    )