# Importação das bibliotecas necessárias
import sys
# Adiciona o caminho do Airflow ao sistema
sys.path.append('/opt/airflow')
# Adiciona o caminho do projeto de orquestração de dados ao sistema
sys.path.append('/opt/n/Drives compartilhados/22 - BI/6.Data_Engineering/1.Automacoes/data_orchestration')

# Importação de pacotes do Python e Airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Importação de scripts personalizados para extração e validação de dados
from scripts.extraction.sankhya.nfs_base.a_extrair_nfs_vendas_do_sankhya import extrair_nfs_vendas_do_sankhya
from scripts.extraction.sankhya.nfs_base.b_validar_tipos_de_dados import validar_tipos_de_dados
from scripts.extraction.sankhya.nfs_base.c_salvar_nfs_para_report_diario import salvar_nfs_para_report_diario
from scripts.extraction.sankhya.nfs_base.d_concatenar_df_extraido_com_base_de_vendas import concatenar_df_extraido_com_base_de_vendas
from scripts.extraction.sankhya.nfs_base.e_criar_arquivos_com_nfs_divergentes import criar_arquivos_com_nfs_divergentes

# Importação de scripts para integração com Power BI e tratamento de dados
from deploy.powerbi.dataflow import atualizar_dataproducts, atualizar_powerbi_vendas
from scripts.transformation.powerbi.vendas import atualizacao_quebra_itens_filho_corr, atualizacao_report_diario
from scripts.transformation.powerbi.vendas import tratamento_cmv_real, tratamento_nfs_venda

# Definição de argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',  # Dono do processo
    'depends_on_past': False,  # Não depende de execuções anteriores
    'start_date': datetime(2024, 8, 26),  # Data de início
    'email_on_failure': True,  # Envia e-mail em caso de falha
    'email_on_retry': False,  # Não envia e-mail em caso de tentativas
    'email': [],  # Lista de destinatários de e-mail (vazia por enquanto)
    'retries': 5,  # Número de tentativas de repetição em caso de falha
    'retry_delay': timedelta(minutes=1)  # Tempo de espera entre tentativas
}

# Definição da DAG (grafo de execução de tarefas)
with DAG(
    dag_id='pipeline_vendas',  # Nome da DAG
    default_args=default_args,  # Argumentos padrão definidos acima
    description='Executa extração, transformação e atualização do workspace de vendas.',  # Descrição
    schedule_interval=None,  # Sem agendamento automático
    catchup=False,  # Não executa tarefas anteriores automaticamente
    max_active_runs=1  # Limita a uma execução ativa por vez
) as dag:
    
    # Definição de um grupo de tarefas chamado 'extrair_nfs_sankhya'
    @task_group(group_id='extrair_nfs_sankhya')
    def transformar_dados_base():
        # Define a tarefa para extrair NFes de vendas do Sankhya
        extrair_nfs_vendas_do_sankhya_task = PythonOperator(
            task_id='extrair_nfs_vendas_do_sankhya',  # Nome da tarefa
            python_callable=extrair_nfs_vendas_do_sankhya,  # Função a ser chamada
            provide_context=True,  # Fornece contexto de execução
        )

        # Valida os tipos de dados extraídos
        validar_tipos_de_dados_task = PythonOperator(
            task_id='validar_tipos_de_dados',
            python_callable=validar_tipos_de_dados,
            provide_context=True,
        )

        # Salva os dados para o relatório diário
        salvar_nfs_para_report_diario_task = PythonOperator(
            task_id='salvar_nfs_para_report_diario',
            python_callable=salvar_nfs_para_report_diario,
            provide_context=True,
        )

        # Concatena os dados extraídos com a base de vendas existente
        concatenar_df_extraido_com_base_de_vendas_task = PythonOperator(
            task_id='concatenar_df_extraido_com_base_de_vendas',
            python_callable=concatenar_df_extraido_com_base_de_vendas,
            provide_context=True,
        )

        # Cria arquivos com divergências nas NFes
        criar_arquivos_com_nfs_divergentes_task = PythonOperator(
            task_id='criar_arquivos_com_nfs_divergentes',
            python_callable=criar_arquivos_com_nfs_divergentes,
            provide_context=True,
        )

        # Define a ordem de execução das tarefas dentro do grupo
        [extrair_nfs_vendas_do_sankhya_task >> validar_tipos_de_dados_task >> salvar_nfs_para_report_diario_task >> concatenar_df_extraido_com_base_de_vendas_task >> criar_arquivos_com_nfs_divergentes_task]

    # Atualiza a quebra de itens filhos no Power BI
    atualizacao_quebra_itens_filho_corr_task = PythonOperator(
        task_id='atualizacao_quebra_itens_filho_corr',
        python_callable=atualizacao_quebra_itens_filho_corr.run,
        provide_context=True,
    )

    # Atualiza os dados de vendas no Power BI
    atualizar_bi_vendas_task = PythonOperator(
        task_id='atualizar_bi_vendas',
        python_callable=atualizar_powerbi_vendas.run,
        op_kwargs={'workspace_name': 'Automacoes', 'fluxo_name': 'Vendas'},  # Argumentos extras
        provide_context=True,
    )
    
    # Define um grupo de tarefas chamado 'transformar_dados'
    @task_group(group_id='transformar_dados')
    def transformar_dados():
        # Atualiza o relatório diário
        atualizacao_report_diario_task = PythonOperator(
            task_id='atualizacao_report_diario',
            python_callable=atualizacao_report_diario.run,
            provide_context=True,
            doc_md="""
            ### Task Documentation
            Cria um arquivo parquet na pasta 2.Dados Transformados a partir do power bi desktop publicado no power bi online.
            """,  # Documentação da tarefa
        )

        # Aplica transformações nas NFes de venda
        transformar_nfs_venda_task = PythonOperator(
            task_id='transformar_nfs_venda',
            python_callable=tratamento_nfs_venda.run,
            provide_context=True,
        )

        # Aplica tratamento no CMV real
        transformar_cmv_real_task = PythonOperator(
            task_id='transformar_cmv_real',
            python_callable=tratamento_cmv_real.run,
            provide_context=True,
        )

        # Define a execução paralela das três tarefas dentro do grupo
        [atualizacao_report_diario_task, transformar_nfs_venda_task, transformar_cmv_real_task]

    # Atualiza o workspace "Data Products" no Power BI
    atualizar_workspace_vendas_task = PythonOperator(
        task_id='atualizar_workspace_vendas',
        python_callable=atualizar_dataproducts.run,
        op_kwargs={'workspace_name': 'Data Products', 'fluxo_name': 'Vendas'},
        provide_context=True,   
    )

    # Define a ordem de execução das tarefas principais
    transformar_dados_base() >> atualizacao_quebra_itens_filho_corr_task >> atualizar_bi_vendas_task >> transformar_dados() >> atualizar_workspace_vendas_task
