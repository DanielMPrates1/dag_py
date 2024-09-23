#%%
import sys
sys.path.append('/opt/airflow')
sys.path.append('/opt/n/Drives compartilhados/22 - BI/6.Data_Engineering/1.Automacoes/data_orchestration')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, ExternalPythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.email import send_email

from scripts.extraction.sankhya.nfs_base.a_extrair_nfs_vendas_do_sankhya import extrair_nfs_vendas_do_sankhya
from scripts.extraction.sankhya.nfs_base.b_validar_tipos_de_dados import validar_tipos_de_dados
from scripts.extraction.sankhya.nfs_base.c_salvar_nfs_para_report_diario import salvar_nfs_para_report_diario
from scripts.extraction.sankhya.nfs_base.d_concatenar_df_extraido_com_base_de_vendas import concatenar_df_extraido_com_base_de_vendas
from scripts.extraction.sankhya.nfs_base.e_criar_arquivos_com_nfs_divergentes import criar_arquivos_com_nfs_divergentes

from deploy.powerbi.dataflow import atualizar_dataproducts, atualizar_powerbi_vendas
from scripts.transformation.powerbi.vendas import atualizacao_quebra_itens_filho_corr, atualizacao_report_diario
from scripts.transformation.powerbi.vendas import tratamento_cmv_real, tratamento_nfs_venda


# %%
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 26),
    'email_on_failure': True,  # Enviar e-mail em caso de falha
    'email_on_retry': False,
    'email': [],  # Lista de e-mails que receberão a notificação
    'retries': 5,
    'retry_delay': timedelta(minutes=1)#False #
}
with DAG(
    dag_id='pipeline_vendas',
    default_args=default_args,
    description='Uma DAG que executa desde a extração e a transformação das tabelas necessárias até a atualização do workspace de vendas.',
    schedule_interval=None,#'0 3 * * *',  # Executa às 3:00 AM diariamente,
    catchup=False,
    max_active_runs=1
    # on_failure_callback=notify_email  # Adiciona o callback para falhas
) as dag:
    
    @task_group(group_id='extrair_nfs_sankhya')
    def transformar_dados_base():
        extrair_nfs_vendas_do_sankhya_task = PythonOperator(
            task_id='extrair_nfs_vendas_do_sankhya',
            python_callable=extrair_nfs_vendas_do_sankhya,
            provide_context=True,
        )

        validar_tipos_de_dados_task = PythonOperator(
            task_id='validar_tipos_de_dados',
            python_callable=validar_tipos_de_dados,
            provide_context=True,
        )

        salvar_nfs_para_report_diario_task = PythonOperator(
            task_id='salvar_nfs_para_report_diario',
            python_callable=salvar_nfs_para_report_diario,
            provide_context=True,
        )

        concatenar_df_extraido_com_base_de_vendas_task = PythonOperator(
            task_id='concatenar_df_extraido_com_base_de_vendas',
            python_callable=concatenar_df_extraido_com_base_de_vendas,
            provide_context=True,
        )

        criar_arquivos_com_nfs_divergentes_task = PythonOperator(
            task_id='criar_arquivos_com_nfs_divergentes',
            python_callable=criar_arquivos_com_nfs_divergentes,
            provide_context=True,
        )

        [extrair_nfs_vendas_do_sankhya_task >> validar_tipos_de_dados_task >> salvar_nfs_para_report_diario_task >> concatenar_df_extraido_com_base_de_vendas_task >> criar_arquivos_com_nfs_divergentes_task]

    atualizacao_quebra_itens_filho_corr_task = PythonOperator(
        task_id='atualizacao_quebra_itens_filho_corr',
        python_callable=atualizacao_quebra_itens_filho_corr.run,
        provide_context=True,
    )

    atualizar_bi_vendas_task = PythonOperator(
        task_id='atualizar_bi_vendas',
        python_callable=atualizar_powerbi_vendas.run,
        op_kwargs={'workspace_name': 'Automacoes', 'fluxo_name': 'Vendas'},
        provide_context=True,
    )
    
    @task_group(group_id='transformar_dados')
    def transformar_dados():
        atualizacao_report_diario_task = PythonOperator(
            task_id='atualizacao_report_diario',
            python_callable=atualizacao_report_diario.run,
            provide_context=True,
            doc_md="""
            ### Task Documentation
            Cria um arquivo parquet na pasta 2.Dados Transformados a partir do power bi desktop publicado no power bi online.
            """,
        )

        transformar_nfs_venda_task = PythonOperator(
            task_id='transformar_nfs_venda',
            python_callable=tratamento_nfs_venda.run,
            provide_context=True,
        )

        transformar_cmv_real_task = PythonOperator(
            task_id='transformar_cmv_real',
            python_callable=tratamento_cmv_real.run,
            provide_context=True,
        )

        [atualizacao_report_diario_task, transformar_nfs_venda_task, transformar_cmv_real_task]

    atualizar_workspace_vendas_task = PythonOperator(
        task_id='atualizar_workspace_vendas',
        python_callable=atualizar_dataproducts.run,
        op_kwargs={'workspace_name': 'Data Products', 'fluxo_name': 'Vendas'},
        provide_context=True,   
    )

    # Definir a ordem de execução das tarefas
    transformar_dados_base() >> atualizacao_quebra_itens_filho_corr_task >> atualizar_bi_vendas_task >> transformar_dados() >> atualizar_workspace_vendas_task
