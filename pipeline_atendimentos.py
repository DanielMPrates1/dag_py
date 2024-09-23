# %%
import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
sys.path.append('/opt/n/Drives compartilhados/22 - BI/6.Data_Engineering/1.Automacoes/data_orchestration')

# %%
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, ExternalPythonOperator

from scripts.extraction.neppo import extrair_neppo_sla, extrair_pesquisa_satisfacao
from scripts.transformation.neppo import tratamento_atendimento_whatsapp

from deploy.powerbi.dataflow import atualizar_dataproducts

# from deploy.bigquery.deploy_dados import enviar_dados_crm_contas


# %%
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': False #timedelta(minutes=10),
}

with DAG(
    dag_id='pipeline_atendimentos',
    default_args=default_args,
    description='Uma DAG que executa desde a extração e a transformação das tabelas necessárias até a atualização do workspace de clientes.',
    schedule_interval=None,#'0 3 * * *',  # Executa às 3:00 AM diariamente, 
    catchup=False,
    max_active_runs=3,
    max_active_tasks=2
) as dag:
    
    @task_group(group_id='etl_neppo')
    def etl_neppo():
        # extração de dados
        extrair_neppo_sla_task = PythonOperator(
            task_id='extrair_neppo_sla',
            python_callable=extrair_neppo_sla.criar_df_sla_recente,
            provide_context=True,
        )

        extrair_pesquisa_satisfacao_task = PythonOperator(
            task_id='extrair_pesquisa_satisfacao',
            python_callable=extrair_pesquisa_satisfacao.run,
            provide_context=True,
        )

        # tratamento de dados
        tratar_atendimento_whats_neppo_task = PythonOperator(
            task_id='tratar_atendimento_whats_neppo',
            python_callable=tratamento_atendimento_whatsapp.run,
            provide_context=True,   
        )        

        # enviar_dados_do_crm_contas_bigquery_task = PythonOperator(
        #     task_id='enviar_dados_do_crm_contas_bigquery',
        #     python_callable=enviar_dados_crm_contas,
        #     provide_context=True,   
        # )

        # Definir a ordem de execução das tarefas
        [extrair_neppo_sla_task >> tratar_atendimento_whats_neppo_task]
        [extrair_pesquisa_satisfacao_task]
    
    
    # atualizar_workspace_clientes_task = PythonOperator(
    #     task_id='atualizar_workspace_clientes',
    #     python_callable=atualizar_dataproducts.run,
    #     op_kwargs={'workspace_name': 'Data Products', 'fluxo_name': 'Clientes'},
    #     provide_context=True,   
    # )
    
    etl_neppo() 
