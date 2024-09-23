# %% Importação de bibliotecas padrão do Python
import os  # Para manipulação de diretórios e caminhos de arquivos
import sys  # Para modificar o caminho de execução
from datetime import datetime  # Para manipulação de datas

# Adiciona o diretório atual e o diretório do projeto de orquestração de dados ao `sys.path` para permitir importações
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
sys.path.append('/opt/n/Drives compartilhados/22 - BI/6.Data_Engineering/1.Automacoes/data_orchestration')

# %% Importação de módulos do Airflow e scripts personalizados
from datetime import datetime, timedelta
from airflow import DAG  # Classe para definir a DAG
from airflow.decorators import task_group  # Para agrupar tarefas
from airflow.operators.python import PythonOperator  # Operador para executar funções Python

# Scripts personalizados de extração e transformação de dados
from scripts.extraction.neppo import extrair_neppo_sla, extrair_pesquisa_satisfacao
from scripts.transformation.neppo import tratamento_atendimento_whatsapp

# Script para atualização de dataflows no Power BI
from deploy.powerbi.dataflow import atualizar_dataproducts

# %% Definição dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',  # Dono da DAG
    'depends_on_past': False,  # Não depende de execuções anteriores
    'start_date': datetime(2024, 8, 26),  # Data de início da DAG
    'email_on_failure': False,  # Não envia e-mails em caso de falha
    'email_on_retry': False,  # Não envia e-mails em caso de tentativa
    'retries': 0,  # Sem tentativas de repetição
    'retry_delay': False  # Sem atraso entre tentativas
}

# Definição da DAG, que contém o fluxo de execução
with DAG(
    dag_id='pipeline_atendimentos',  # Nome da DAG
    default_args=default_args,  # Argumentos padrão
    description='Executa a extração, transformação e atualização do workspace de clientes.',  # Descrição da DAG
    schedule_interval=None,  # Sem agendamento automático (pode ser agendado via interface)
    catchup=False,  # Não executa tarefas anteriores automaticamente
    max_active_runs=3,  # Número máximo de execuções simultâneas
    max_active_tasks=2  # Número máximo de tarefas ativas simultaneamente
) as dag:
    
    # Criação de um grupo de tarefas chamado 'etl_neppo'
    @task_group(group_id='etl_neppo')
    def etl_neppo():
        # Extração de dados do SLA do sistema Neppo
        extrair_neppo_sla_task = PythonOperator(
            task_id='extrair_neppo_sla',  # Nome da tarefa
            python_callable=extrair_neppo_sla.criar_df_sla_recente,  # Função a ser executada
            provide_context=True,  # Fornece o contexto da execução para a função
        )

        # Extração de dados da pesquisa de satisfação
        extrair_pesquisa_satisfacao_task = PythonOperator(
            task_id='extrair_pesquisa_satisfacao',
            python_callable=extrair_pesquisa_satisfacao.run,
            provide_context=True,
        )

        # Tratamento dos atendimentos via WhatsApp do sistema Neppo
        tratar_atendimento_whats_neppo_task = PythonOperator(
            task_id='tratar_atendimento_whats_neppo',
            python_callable=tratamento_atendimento_whatsapp.run,
            provide_context=True,   
        )

        # Define a ordem de execução das tarefas:
        # A tarefa de extrair o SLA deve ocorrer antes do tratamento do atendimento WhatsApp
        [extrair_neppo_sla_task >> tratar_atendimento_whats_neppo_task]
        # A tarefa de extrair a pesquisa de satisfação é executada separadamente
        [extrair_pesquisa_satisfacao_task]

    # Executa o grupo de tarefas 'etl_neppo'
    etl_neppo()
