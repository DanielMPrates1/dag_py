# %%
import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
sys.path.append('/opt/n/Drives compartilhados/22 - BI/6.Data_Engineering/1.Automacoes/data_orchestration')

from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python import PythonOperator

from scripts.extraction.crm import extrair_crm_contas, extrair_crm_contatos, extrair_crm_negocios
from scripts.transformation.crm import tratamento_crm_contas, tratamento_crm_contatos, tratamento_crm_negocios
from deploy.powerbi.dataflow import atualizar_dataproducts, atualizar_powerbi_vendas


# %%
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='pipeline_clientes',
    default_args=default_args,
    description='Pipeline do do CRM.',
    schedule_interval=None,#'0 3 * * *',  # Executa às 3:00 AM diariamente
    catchup=False,
    max_active_runs=3
) as dag1:
    
    extrair_dados_do_crm_contas_task = PythonOperator(
        task_id='extrair_dados_do_crm_contas',
        python_callable=extrair_crm_contas.run,
    )

    tratar_dados_do_crm_contas_task = PythonOperator(
        task_id='tratar_dados_do_crm_contas',
        python_callable=tratamento_crm_contas.transformar_dados,
    )

    
    
    extrair_dados_do_crm_contatos_task = PythonOperator(
        task_id='extrair_dados_do_crm_contatos',
        python_callable=extrair_crm_contatos.run,
    )

    tratar_dados_do_crm_contatos_task = PythonOperator(
        task_id='tratar_dados_do_crm_contatos',
        python_callable=tratamento_crm_contatos.transformar_dados,
    )

    
    
    extrair_dados_do_crm_negocios_task = PythonOperator(
        task_id='extrair_dados_do_crm_negocios',
        python_callable=extrair_crm_negocios.run,
    )

    tratar_dados_do_crm_negocios_task = PythonOperator(
        task_id='tratar_dados_do_crm_negocios',
        python_callable=tratamento_crm_negocios.transformar_dados,
    )

    

    atualizar_workspace_clientes_task = PythonOperator(
        task_id='atualizar_workspace_clientes',
        python_callable=atualizar_dataproducts.run,
        op_kwargs={'workspace_name': 'Data Products', 'fluxo_name': 'Clientes'},
        provide_context=True,   
    )


    # Achate a lista de listas em uma única lista de tarefas
    tasks = [
        extrair_dados_do_crm_contas_task >> tratar_dados_do_crm_contas_task,
        extrair_dados_do_crm_contatos_task >> tratar_dados_do_crm_contatos_task,
        extrair_dados_do_crm_negocios_task >> tratar_dados_do_crm_negocios_task
    ]

    # Defina a dependência entre todas as tarefas e a tarefa final
    tasks >> atualizar_workspace_clientes_task
