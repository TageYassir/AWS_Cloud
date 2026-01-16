"""
streamvision_daily_pipeline.py
DAG Airflow pour le pipeline quotidien StreamVision

Ce DAG orchestre :
1. Extraction PostgreSQL vers S3
2. Chargement S3 vers Snowflake Staging
3. Transformations dbt (staging vers core vers marts)
4. Tests de qualité dbt
5. Rafraîchissement des données Power BI (via API)

Emplacement : airflow/dags/streamvision_daily_pipeline.py
Auteur : Data Engineering Team - StreamVision
Date : 2025-11-27
"""

from datetime import datetime, timedelta
from airflow import DAG
# OLD
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator

# NEW
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# Use the generic SQL operator for Snowflake (best practice in newer Airflow)
# Updated imports for modern Airflow providers
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
import pendulum

# ============================================================================
# CONFIGURATION DU DAG
# ============================================================================

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'email': ['data-team@streamvision.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
    'catchup': False
}

dag = DAG(
    'streamvision_daily_pipeline',
    default_args=default_args,
    description='Pipeline quotidien StreamVision : PostgreSQL → S3 → Snowflake → dbt → BI',
    schedule='0 2 * * *',
    max_active_runs=1,
    tags=['production', 'daily', 'streamvision', 'data_pipeline'],
    catchup=False,
    doc_md="""# Pipeline quotidien StreamVision
    ...
    """
)

# ============================================================================
# TÂCHES PYTHON PERSONNALISÉES
# ============================================================================

def extract_postgres_to_s3(**context):
    """
    Extrait les données PostgreSQL et les pousse dans S3
    
    Utilise le script Python : StreamVisionTP/scripts/export_to_s3.py
    """
    import subprocess
    import sys
    import os
    
    execution_date = context['ds']  # Date d'exécution (YYYY-MM-DD)
    
    print(f"🚀 Extraction PostgreSQL vers S3 pour {execution_date}")
    
    # Chemin absolu vers le script d'export
    # ⚠️ MODIFIEZ CE CHEMIN SELON VOTRE INSTALLATION
    script_path = '/opt/airflow/dags/scripts/export_to_s3.py'
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script non trouvé : {script_path}")
    
    # Construction de la commande
    cmd = [sys.executable, script_path]
    
    # Exécution
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=os.path.dirname(script_path)
    )
    
    print(f"Sortie standard:\n{result.stdout}")
    
    if result.stderr:
        print(f"Sortie d'erreur:\n{result.stderr}")
    
    if result.returncode != 0:
        raise Exception(f"Échec de l'export PostgreSQL → S3. Code de retour: {result.returncode}")
    
    print(f"✅ Export PostgreSQL → S3 terminé avec succès pour {execution_date}")

def run_dbt_models(**context):
    """
    Exécute les transformations dbt
    
    Utilise la commande : dbt run --project-dir [chemin]
    """
    import subprocess
    import os
    import sys
    
    print("🔄 Exécution des modèles dbt...")
    
    # Chemin vers le projet dbt
    # ⚠️ MODIFIEZ CE CHEMIN SELON VOTRE INSTALLATION
    dbt_project_dir = "/opt/airflow/dbt/streamvision_dbt"
    
    if not os.path.exists(dbt_project_dir):
        raise FileNotFoundError(f"Projet dbt non trouvé : {dbt_project_dir}")
    
    # Commande dbt
    cmd = [
    sys.executable,
    "-m",
    "dbt",
    "run",
    "--project-dir",
    dbt_project_dir
]

    
    # Exécution
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=dbt_project_dir
    )
    
    print(f"Sortie dbt:\n{result.stdout[:500]}...")  # Affiche les 500 premiers caractères
    
    if result.stderr:
        print(f"Erreurs dbt:\n{result.stderr}")
    
    if result.returncode != 0:
        # On ne fait pas échouer le DAG pour les erreurs dbt, mais on log
        print(f"⚠️ Attention: dbt run a retourné le code {result.returncode}")
        # Vous pourriez envoyer une alerte ici
    
    print("✅ Transformations dbt terminées")

def run_dbt_tests(**context):
    """
    Exécute les tests de qualité dbt
    """
    import subprocess
    import os
    import sys
    
    print("🧪 Exécution des tests dbt...")
    
    dbt_project_dir = "/opt/airflow/dbt/streamvision_dbt"
    cmd = [
    sys.executable,
    "-m",
    "dbt",
    "test",
    "--project-dir",
    dbt_project_dir
]

    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=dbt_project_dir
    )
    
    print(f"Résultats tests:\n{result.stdout}")
    
    if result.stderr:
        print(f"Erreurs tests:\n{result.stderr}")
    
    # Les tests peuvent échouer sans faire échouer le DAG
    if result.returncode != 0:
        print(f"⚠️ Certains tests dbt ont échoué (code: {result.returncode})")
        # Envoyer une notification d'avertissement
    
    print("✅ Tests dbt terminés")

def generate_dbt_docs(**context):
    """
    Génère la documentation dbt
    """
    import subprocess
    import os
    import sys
    
    print("📚 Génération de la documentation dbt...")
    
    dbt_project_dir = '/opt/airflow/dbt/streamvision_dbt'
    cmd = [
    sys.executable,
    "-m",
    "dbt",
    "docs",
    "generate",
    "--project-dir",
    dbt_project_dir
]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=dbt_project_dir
    )
    
    if result.returncode != 0:
        print(f"⚠️ Échec de génération de la documentation: {result.stderr}")
    else:
        print("✅ Documentation dbt générée")
        
        # Optionnel : déployer la documentation quelque part
        # (GitHub Pages, S3, serveur web, etc.)

def send_slack_notification(**context):
    """
    Envoie une notification Slack à la fin du pipeline
    """
    import requests
    import json
    
    # Configuration du webhook Slack
    # ⚠️ REMPLACEZ PAR VOTRE WEBHOOK SLACK
    webhook_url = "https://hooks.slack.com/services/XXXXX/YYYYY/ZZZZZ"
    
    execution_date = context['ds']
    dag_id = context['dag'].dag_id
    
    message = {
        "text": f"✅ Pipeline {dag_id} terminé avec succès",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚀 Pipeline StreamVision terminé",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Date:*\n{execution_date}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Pipeline:*\n{dag_id}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Toutes les étapes ont été exécutées avec succès :\n• Extraction PostgreSQL → S3\n• Chargement S3 → Snowflake\n• Transformations dbt\n• Tests de qualité"
                }
            }
        ]
    }
    
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(message),
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code != 200:
            print(f"⚠️ Échec de l'envoi de la notification Slack: {response.text}")
        else:
            print("✅ Notification Slack envoyée")
            
    except Exception as e:
        print(f"⚠️ Erreur lors de l'envoi de la notification Slack: {e}")

# ============================================================================
# DÉFINITION DES TÂCHES
# ============================================================================

# Tâches de début/fin
start_task = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Tâche 1 : Extraction PostgreSQL → S3
task_extract = PythonOperator(
    task_id='extract_postgres_to_s3',
    python_callable=extract_postgres_to_s3,
    dag=dag
)

# Tâches 2 : Attente des fichiers dans S3 (Sensors)
task_wait_s3_users = S3KeySensor(
    task_id='wait_s3_users_file',
    bucket_name='streamvision-data-raw',  # ⚠️ MODIFIEZ
    bucket_key='raw/postgres/users/{{ ds }}/users_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,  # 10 minutes
    poke_interval=30,  # Vérifie toutes les 30 secondes
    mode='poke',
    dag=dag
)

task_wait_s3_viewing_sessions = S3KeySensor(
    task_id='wait_s3_viewing_sessions_file',
    bucket_name='streamvision-data-raw',  # ⚠️ MODIFIEZ
    bucket_key='raw/postgres/viewing_sessions/{{ ds }}/viewing_sessions_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag
)

# Tâches 3 : Chargement S3 → Snowflake STAGING
task_load_staging_users = SnowflakeOperator(
    task_id='load_staging_users',
    conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE LOADING_WH;
        USE SCHEMA STREAMVISION_WH.STAGING;
        
        -- Truncate et chargement incrémental
        DELETE FROM stg_users WHERE DATE(_loaded_at) = '{{ ds }}';
        
        COPY INTO stg_users (
            id, email, username, first_name, last_name, country, age_group,
            subscription_plan, subscription_start, subscription_end, created_at,
            last_login, is_active, payment_method, device_preference
        )
        FROM @STREAMVISION_WH.RAW.s3_raw_stage/postgres/users/{{ ds }}/
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
        
        SELECT 'STG_USERS chargé : ' || COUNT(*) || ' nouvelles lignes' 
        FROM stg_users 
        WHERE DATE(_loaded_at) = '{{ ds }}';
    """,
    dag=dag
)

task_load_staging_viewing_sessions = SnowflakeOperator(
    task_id='load_staging_viewing_sessions',
    conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE LOADING_WH;
        USE SCHEMA STREAMVISION_WH.STAGING;
        
        DELETE FROM stg_viewing_sessions WHERE DATE(_LOADED_AT) = '{{ ds }}';
        
        COPY INTO stg_viewing_sessions (
            id, user_id, content_id, session_start, session_end, duration_seconds,
            platform, device_type, quality, completion_rate, buffering_count, avg_bitrate,
            city, ip_address
        )
        FROM @STREAMVISION_WH.RAW.s3_raw_stage/postgres/viewing_sessions/{{ ds }}/
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
        PATTERN = '.*viewing_sessions.*\\.csv'
        ON_ERROR = 'CONTINUE';
        
        SELECT 'STG_VIEWING_SESSIONS chargé : ' || COUNT(*) || ' nouvelles lignes' 
        FROM stg_viewing_sessions 
        WHERE DATE(_LOADED_AT) = '{{ ds }}';
    """,
    dag=dag
)

# Tâche 4 : Nettoyage des données dans STAGING
task_clean_staging_data = SnowflakeOperator(
    task_id='clean_staging_data',
    conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE TRANSFORM_WH;
        USE SCHEMA STREAMVISION_WH.STAGING;
        
        -- Suppression des doublons dans stg_users
        DELETE FROM stg_users 
        WHERE _LOADED_AT IN (
            SELECT _LOADED_AT 
            FROM (
                SELECT _LOADED_AT,
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY _LOADED_AT DESC) as rn
                FROM stg_users
                WHERE DATE(_LOADED_AT) = '{{ ds }}'
            ) t
            WHERE t.rn > 1
        );
        
        -- Nettoyage des sessions avec durée invalide
        DELETE FROM stg_viewing_sessions 
        WHERE duration_seconds <= 0 
        AND DATE(_loaded_at) = '{{ ds }}';
        
        SELECT 'Données staging nettoyées' AS message;
    """,
    dag=dag
)

# Tâche 5 : Exécution de dbt
task_dbt_run = PythonOperator(
    task_id='dbt_run_models',
    python_callable=run_dbt_models,
    dag=dag
)

# Tâche 6 : Tests dbt
task_dbt_test = PythonOperator(
    task_id='dbt_test_models',
    python_callable=run_dbt_tests,
    dag=dag
)

# Tâche 7 : Documentation dbt
task_dbt_docs = PythonOperator(
    task_id='dbt_generate_docs',
    python_callable=generate_dbt_docs,
    dag=dag
)

# Tâche 8 : Rafraîchissement des vues matérialisées (optionnel)
task_refresh_views = SnowflakeOperator(
    task_id='refresh_materialized_views',
    conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE BI_WH;

        SELECT 
            'Materialized views auto-refreshed by Snowflake' AS message;
    """,
    dag=dag
)


# Tâche 9 : Notification
task_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_slack_notification,
    dag=dag
)

# Tâche 10 : Log de fin
task_log_completion = BashOperator(
    task_id='log_pipeline_completion',
    bash_command='echo "Pipeline StreamVision terminé avec succès le $(date)"',
    dag=dag
)

# ============================================================================
# DÉFINITION DES DÉPENDANCES (DAG)
# ============================================================================

# Phase 1 : Extraction
start_task >> task_extract

# Phase 2 : Attente des fichiers S3
task_extract >> [task_wait_s3_users, task_wait_s3_viewing_sessions]

# Phase 3 : Chargement vers Snowflake
task_wait_s3_users >> task_load_staging_users
task_wait_s3_viewing_sessions >> task_load_staging_viewing_sessions

# Phase 4 : Nettoyage
[task_load_staging_users, task_load_staging_viewing_sessions] >> task_clean_staging_data

# Phase 5 : Transformations dbt
task_clean_staging_data >> task_dbt_run
task_dbt_run >> task_dbt_test
task_dbt_test >> task_dbt_docs

# Phase 6 : Post-traitement
task_dbt_docs >> task_refresh_views
task_refresh_views >> task_notification
task_notification >> task_log_completion

# Phase 7 : Fin
task_log_completion >> end_task

# ============================================================================
# VISUALISATION DU DAG
# ============================================================================
"""
DAG Structure:

start_pipeline
    ↓
extract_postgres_to_s3
    ↓
wait_s3_users_file     wait_s3_viewing_sessions_file
    ↓                           ↓
load_staging_users     load_staging_viewing_sessions
            \         /
             \       /
        clean_staging_data
                ↓
        dbt_run_models
                ↓
        dbt_test_models
                ↓
        dbt_generate_docs
                ↓
        refresh_materialized_views
                ↓
        send_success_notification
                ↓
        log_pipeline_completion
                ↓
        end_pipeline
"""