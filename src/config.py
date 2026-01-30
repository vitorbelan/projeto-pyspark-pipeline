"""
Configurações para o pipeline de dados de crédito
"""

import os

# Configurações do Spark
SPARK_CONFIG = {
    "spark.app.name": "CreditDataPipeline",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
    "spark.sql.adaptive.skewJoin.enabled": "true"
}

# Caminhos de arquivos
DATA_PATH = os.getenv("DATA_PATH", "/data/df_credit_amostra.csv")
RESULTS_PATH = os.getenv("RESULTS_PATH", "/results")

# Configurações de qualidade de dados
QUALITY_THRESHOLDS = {
    "min_quality_score": 70.0,
    "max_null_percentage": 10.0,
    "max_duplicate_percentage": 5.0,
    "critical_columns": [
        "timestamp", 
        "receiving_address", 
        "amount", 
        "transaction_type", 
        "location_region", 
        "risk_score"
    ]
}

# Configurações de logging
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "/logs/pipeline.log"
}

# Configurações do pipeline
PIPELINE_CONFIG = {
    "enable_quality_checks": True,
    "save_intermediate_results": True,
    "cleanup_temp_files": True,
    "output_format": "parquet"  # ou "csv"
}

# Metadados das análises
ANALYSIS_METADATA = {
    "location_risk_analysis": {
        "description": "Análise de média de risk_score por location_region",
        "output_table": "location_risk_summary",
        "columns": ["location_region", "avg_risk_score"]
    },
    "top_sale_addresses_analysis": {
        "description": "Top 3 receiving addresses com maior amount nas últimas vendas",
        "output_table": "top_sale_addresses",
        "columns": ["receiving_address", "amount", "timestamp", "timestamp_readable"]
    }
}

# Configurações de container
CONTAINER_CONFIG = {
    "work_dir": "/app",
    "data_dir": "/data",
    "results_dir": "/results",
    "logs_dir": "/logs"
}
