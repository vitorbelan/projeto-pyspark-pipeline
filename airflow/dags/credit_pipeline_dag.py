from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.docker.operators.docker import DockerOperator
import os

# Configurações padrão do DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@empresa.com']  # Substitua pelo seu email
}

# Definir o DAG
dag = DAG(
    'credit_data_pipeline',
    default_args=default_args,
    description='Pipeline de processamento de dados de crédito com PySpark',
    schedule_interval=timedelta(hours=6),  # Executa a cada 6 horas
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['pyspark', 'data-quality', 'credit-analysis']
)

# Sensor para verificar se o arquivo de entrada existe
check_input_file = FileSensor(
    task_id='check_input_file',
    filepath='/data/df_credit_amostra.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag
)

# Task para executar o pipeline principal usando Docker
run_pipeline = DockerOperator(
    task_id='run_credit_pipeline',
    image='credit-data-pipeline:latest',
    container_name='credit_pipeline_{{ ts_nodash }}',
    api_version='auto',
    auto_remove=True,
    command=['python3', 'src/data_pipeline.py'],
    environment={
        'SPARK_DRIVER_MEMORY': '2g',
        'SPARK_EXECUTOR_MEMORY': '2g',
        'DATA_PATH': '/data/df_credit_amostra.csv',
        'RESULTS_PATH': '/results'
    },
    volumes=[
        '/opt/airflow/data:/data:ro',
        '/opt/airflow/results:/results',
        '/opt/airflow/logs:/logs'
    ],
    network_mode='bridge',
    dag=dag
)

def validate_pipeline_results():
    """Validar se os resultados foram gerados corretamente"""
    import json
    import os
    
    results_dir = '/opt/airflow/results'
    
    # Verificar se o relatório de qualidade foi gerado
    quality_report_path = os.path.join(results_dir, 'quality_report.json')
    if not os.path.exists(quality_report_path):
        raise FileNotFoundError("Relatório de qualidade não encontrado")
    
    # Ler e validar o relatório de qualidade
    with open(quality_report_path, 'r') as f:
        quality_report = json.load(f)
    
    # Verificar score de qualidade dos dados limpos
    if 'cleaned_data' in quality_report:
        quality_score = quality_report['cleaned_data'].get('quality_score', 0)
        if quality_score < 70:
            raise ValueError(f"Score de qualidade muito baixo: {quality_score}")
        print(f"✅ Score de qualidade: {quality_score}/100")
    
    # Verificar se os arquivos de resultado existem
    expected_results = [
        'location_risk_analysis',
        'top_sale_addresses'
    ]
    
    for result in expected_results:
        result_path = os.path.join(results_dir, result)
        if not os.path.exists(result_path):
            raise FileNotFoundError(f"Resultado não encontrado: {result}")
        print(f"✅ Resultado encontrado: {result}")
    
    print("🎉 Validação dos resultados concluída com sucesso!")

# Task para validar os resultados
validate_results = PythonOperator(
    task_id='validate_results',
    python_callable=validate_pipeline_results,
    dag=dag
)

def send_quality_alerts():
    """Enviar alertas baseados no relatório de qualidade"""
    import json
    import os
    
    quality_report_path = '/opt/airflow/results/quality_report.json'
    
    if os.path.exists(quality_report_path):
        with open(quality_report_path, 'r') as f:
            quality_report = json.load(f)
        
        # Verificar alertas para dados limpos
        if 'cleaned_data' in quality_report:
            report = quality_report['cleaned_data']
            
            alerts = []
            
            # Score de qualidade baixo
            quality_score = report.get('quality_score', 100)
            if quality_score < 80:
                alerts.append(f"🚨 Score de qualidade baixo: {quality_score}/100")
            
            # Verificar nulos
            null_columns = report.get('null_analysis', {}).get('columns_with_nulls', 0)
            if null_columns > 0:
                alerts.append(f"⚠️ {null_columns} colunas com valores nulos")
            
            # Verificar duplicatas
            duplicate_pct = report.get('duplicate_analysis', {}).get('duplicate_percentage', 0)
            if duplicate_pct > 5:
                alerts.append(f"🔄 {duplicate_pct:.1f}% de registros duplicados")
            
            # Verificar anomalias
            anomalies_count = len(report.get('anomalies', []))
            if anomalies_count > 0:
                alerts.append(f"🔍 {anomalies_count} tipos de anomalias detectadas")
            
            if alerts:
                print("\n🚨 ALERTAS DE QUALIDADE DETECTADOS:")
                for alert in alerts:
                    print(f"   {alert}")
                
                # Aqui você pode integrar com sistemas de alerta (Slack, email, etc.)
                # Por exemplo:
                # send_slack_notification(alerts)
                # send_email_notification(alerts)
            else:
                print("✅ Nenhum alerta de qualidade crítico!")
    else:
        print("⚠️ Relatório de qualidade não encontrado")

# Task para enviar alertas de qualidade
quality_alerts = PythonOperator(
    task_id='send_quality_alerts',
    python_callable=send_quality_alerts,
    dag=dag
)

# Task para cleanup de arquivos temporários (opcional)
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command="""
        # Limpar arquivos temporários mais antigos que 7 dias
        find /opt/airflow/temp -type f -mtime +7 -delete 2>/dev/null || true
        
        # Limpar logs de Spark mais antigos que 3 dias
        find /opt/airflow/spark-events -type f -mtime +3 -delete 2>/dev/null || true
        
        echo "🧹 Cleanup de arquivos temporários concluído"
    """,
    dag=dag
)

# Definir dependências das tasks
check_input_file >> run_pipeline >> validate_results >> quality_alerts >> cleanup_temp_files

# Task adicional para gerar relatório de execução
def generate_execution_report():
    """Gerar relatório de execução do pipeline"""
    from datetime import datetime
    import json
    import os
    
    execution_report = {
        "execution_date": datetime.now().isoformat(),
        "dag_id": "credit_data_pipeline",
        "status": "success",
        "tasks_completed": [
            "check_input_file",
            "run_credit_pipeline",
            "validate_results",
            "send_quality_alerts"
        ]
    }
    
    # Adicionar informações do relatório de qualidade se disponível
    quality_report_path = '/opt/airflow/results/quality_report.json'
    if os.path.exists(quality_report_path):
        with open(quality_report_path, 'r') as f:
            quality_data = json.load(f)
            execution_report["quality_summary"] = {
                "quality_score": quality_data.get('cleaned_data', {}).get('quality_score', 0),
                "total_records": quality_data.get('cleaned_data', {}).get('basic_metrics', {}).get('total_records', 0)
            }
    
    # Salvar relatório de execução
    report_path = f'/opt/airflow/results/execution_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(report_path, 'w') as f:
        json.dump(execution_report, f, indent=2)
    
    print(f"📋 Relatório de execução salvo: {report_path}")

generate_report = PythonOperator(
    task_id='generate_execution_report',
    python_callable=generate_execution_report,
    dag=dag
)

# Adicionar a task de relatório no final do pipeline
quality_alerts >> generate_report >> cleanup_temp_files
