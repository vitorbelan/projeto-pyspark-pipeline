# 🎯 Pipeline de Dados de Crédito com PySpark

Este projeto implementa uma pipeline completa de processamento de dados de transações de crédito usando **PySpark**, **Docker**, e **Apache Airflow** para orquestração, com monitoramento automático de qualidade de dados.

## 📋 Visão Geral

Aqui trouxe um projeto estruturadado onde a ideia e conseguir rodar usando docker mas que ele traga uma previa dos resultados no proprio terminal.

pontos basicos e rapidos
necessario colocar o arquivo de ingestao dentro da pasta data e ter docker rodando. depois so seguir a estruturacao do README.md
com o docker instalado rodar com comando ./run-complete.sh all

A pipeline processa dados de transações financeiras e realiza:
- ✅ **Importação** e limpeza de dados CSV
- 📊 **Análise de risco** por região geográfica  
- 💰 **Identificação** dos top 3 receiving addresses por valor
- 🔍 **Monitoramento automático** de qualidade de dados
- 📈 **Métricas** e alertas de conformidade

## 🏗️ Arquitetura

```
├── 🐳 Containerização (Docker + Docker Compose)
├── ⚡ Processamento (Apache Spark/PySpark)
├── ✈️ Orquestração (Apache Airflow)
├── 📊 Monitoramento (Spark UI + Quality Checks)
└── 🔍 Data Quality (Classe personalizada QualityCheck)
```

## 📁 Estrutura do Projeto

```
projeto-pyspark-pipeline/
├── 🐳 docker/
│   ├── Dockerfile                    # Imagem customizada com PySpark
│   └── docker-compose.yml            # Orquestração de containers
├── 📄 src/
│   ├── quality_check.py              # Classe para Data Quality
│   ├── data_pipeline.py              # Pipeline principal PySpark
│   └── config.py                     # Configurações
├── 📊 data/
│   └── df_credit_amostra.csv          # Dados de entrada
├── ✈️ airflow/
│   ├── dags/credit_pipeline_dag.py   # DAG do Airflow
│   └── docker-compose-airflow.yml    # Setup do Airflow
├── 🔧 requirements.txt               # Dependências Python
├── 🚀 run.sh                        # Script de execução
└── 📚 README.md                     # Esta documentação
```

## 🚀 Como Executar

### 1️⃣ **Preparação dos Dados**

```bash
# 1. Clone ou baixe o projeto
# 2. Coloque o arquivo CSV na pasta data/
cp df_credit_amostra.csv data/
```

### 2️⃣ **Execução Simples (Pipeline)**

```bash
# Dar permissão de execução ao script
chmod +x run.sh

# Executar apenas o pipeline principal
./run.sh pipeline
```

### 3️⃣ **Execução com Jupyter (Desenvolvimento)**

```bash
# Executar pipeline + Jupyter Lab
./run.sh jupyter

# Acessar: http://localhost:8888
```

### 4️⃣ **Execução com Monitoramento**

```bash
# Executar com Spark History Server
./run.sh monitoring

# Acessar Spark UI: http://localhost:18080
```

### 5️⃣ **Execução com Airflow (Orquestração)**

```bash
# Configurar e executar Airflow
./run.sh airflow

# Acessar: http://localhost:8080
# Usuário: admin | Senha: admin123
```

### 6️⃣ **Execução Completa**

```bash
# Executar tudo junto
./run.sh all

# Serviços disponíveis:
# - Pipeline: Execução única
# - Jupyter: http://localhost:8888  
# - Monitoring: http://localhost:18080
```

## 📊 Resultados Gerados

A pipeline gera os seguintes resultados na pasta `results/`:

### 1. **Análise de Risco por Região**
```
Location Regions por Média de Risk Score (ordem decrescente):
┌─────────────────┬────────────────┐
│ Location Region │ Avg Risk Score │
├─────────────────┼────────────────┤
│ Africa          │          62.50 │
│ Asia            │          31.25 │
│ South America   │          30.88 │
│ Europe          │          18.75 │
│ North America   │          12.50 │
└─────────────────┴────────────────┘
```

### 2. **Top 3 Receiving Addresses (Vendas)**
```
Top 3 Receiving Addresses (última transação de venda):
┌──────────────────────────────────────────┬─────────┬─────────────────────┐
│ Receiving Address                        │  Amount │ Timestamp           │
├──────────────────────────────────────────┼─────────┼─────────────────────┤
│ 0x6fdc047c2391615b3facd79b4588c7e9106e49f2 │ 66002.0 │ 2021-01-21 19:01:35 │
│ 0x4d220aa8bf8a866b1c8da34c900e8f783e5c98d7 │ 35623.0 │ 2020-04-19 10:15:22 │
│ 0x1f2f48e9c4b79d2e2a1a3d4e5f6a7b8c9d0e1f2a │  1500.0 │ 2021-11-15 08:30:45 │
└──────────────────────────────────────────┴─────────┴─────────────────────┘
```

### 3. **Relatório de Qualidade de Dados**
```json
{
  "cleaned_data": {
    "timestamp": "2024-12-08T15:30:42.123456",
    "basic_metrics": {
      "total_records": 17,
      "total_columns": 13
    },
    "quality_score": 95.5,
    "null_analysis": {
      "columns_with_nulls": 0
    },
    "anomalies": []
  }
}
```

## 🔍 Data Quality - Classe QualityCheck

A classe `QualityCheck` implementa monitoramento automático com:

### 📊 **Métricas Calculadas:**
- ✅ Total de registros e colunas
- 🔍 Análise de valores nulos (por coluna)
- 🔄 Detecção de registros duplicados
- 📈 Estatísticas descritivas (média, desvio, min/max)
- ⚠️ Detecção de anomalias automatizada

### 📈 **Score de Qualidade (0-100):**
- **100**: Dados perfeitos
- **80-99**: Qualidade alta
- **60-79**: Qualidade média (alertas)  
- **<60**: Qualidade baixa (ação necessária)

### 🚨 **Alertas Automáticos:**
- Valores negativos em `amount`
- Timestamps muito antigos/futuros
- Percentual alto de nulos
- Registros duplicados em excesso

## ✈️ Orquestração com Airflow

O DAG `credit_data_pipeline` implementa:

### 📋 **Tasks:**
1. `check_input_file` - Verificar arquivo de entrada
2. `run_credit_pipeline` - Executar pipeline PySpark
3. `validate_results` - Validar resultados gerados
4. `send_quality_alerts` - Enviar alertas de qualidade
5. `generate_execution_report` - Gerar relatório final
6. `cleanup_temp_files` - Limpeza de arquivos temporários

### ⏰ **Agendamento:**
- **Schedule:** A cada 6 horas
- **Retry:** 2 tentativas com 5 min de intervalo
- **Alertas:** Email em caso de falha

### 🔧 **Configuração:**
```python
# Editar em airflow/dags/credit_pipeline_dag.py
default_args = {
    'email': ['seu-email@empresa.com'],  # ← Configurar email
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
```

## 🐳 Comandos Docker

### **Build Manual**
```bash
docker build -t credit-data-pipeline:latest .
```

### **Executar Container Individual**
```bash
docker run --rm \
  -v $(pwd)/data:/data:ro \
  -v $(pwd)/results:/results \
  -v $(pwd)/src:/app/src \
  credit-data-pipeline:latest
```

### **Docker Compose - Pipeline**
```bash
docker-compose up --build pyspark-pipeline
```

### **Docker Compose - Com Jupyter**
```bash
docker-compose --profile jupyter up -d
```

## 📊 Monitoramento e Logs

### **Logs da Pipeline**
```bash
# Ver logs em tempo real
./run.sh logs

# Ou diretamente
docker-compose logs -f pyspark-pipeline
```

### **Spark UI (History Server)**
```bash
# Iniciar monitoramento
./run.sh monitoring

# Acessar: http://localhost:18080
```

### **Jupyter Lab (Desenvolvimento)**
```bash
# Análise interativa
./run.sh jupyter

# Acessar: http://localhost:8888
```

## 🔧 Configurações Avançadas

### **Ajustar Recursos Spark**
```yaml
# Em docker-compose.yml
environment:
  - SPARK_DRIVER_MEMORY=4g      # ← Ajustar conforme necessário
  - SPARK_EXECUTOR_MEMORY=4g    # ← Ajustar conforme necessário
  - SPARK_EXECUTOR_CORES=2      # ← Ajustar conforme necessário
```

### **Configurar Quality Thresholds**
```python
# Em src/config.py
QUALITY_THRESHOLDS = {
    "min_quality_score": 70.0,        # ← Score mínimo aceitável
    "max_null_percentage": 10.0,      # ← % máximo de nulos
    "max_duplicate_percentage": 5.0   # ← % máximo de duplicados
}
```

## 🚀 Scripts de Utilitário

### **Parar Todos os Serviços**
```bash
./run.sh stop
```

### **Limpar Ambiente Completo**
```bash
./run.sh clean
```

### **Ajuda Completa**
```bash
./run.sh help
```

## 📋 Dependências

### **Sistema:**
- 🐳 Docker 20.10+
- 🐙 Docker Compose 2.0+
- 💻 4GB+ RAM recomendado
- 💾 2GB+ espaço em disco

### **Python (instaladas automaticamente):**
- `pyspark==3.4.1`
- `pandas>=2.0.0`
- `tabulate>=0.9.0`
- Ver `requirements.txt` completo

## ❗ Resolução de Problemas

### **Problema: "Arquivo não encontrado"**
```bash
# Verificar se o arquivo está no local correto
ls -la data/df_credit_amostra.csv

# Deve retornar o arquivo
```

### **Problema: "Docker não está rodando"**
```bash
# Iniciar Docker Desktop
# Ou no Linux:
sudo systemctl start docker
```

### **Problema: "Porta já está em uso"**
```bash
# Ver o que está usando a porta
lsof -i :8080

# Parar serviços conflitantes
./run.sh stop
```

### **Problema: "Permissão negada no run.sh"**
```bash
chmod +x run.sh
```

### **Problema: "Memória insuficiente"**
```bash
# Reduzir recursos no docker-compose.yml
environment:
  - SPARK_DRIVER_MEMORY=1g
  - SPARK_EXECUTOR_MEMORY=1g
```

## 🔮 Próximos Passos

### **Possíveis Melhorias:**
- 🔔 Integração com Slack/Teams para alertas
- 📈 Dashboard em tempo real (Grafana)
- 🗃️ Integração com data warehouse (BigQuery/Snowflake)
- 🔄 CI/CD pipeline com GitHub Actions
- 🧪 Testes automatizados (pytest)
- 📊 Métricas avançadas de data drift

### **Escalabilidade:**
- ☸️ Migração para Kubernetes
- 📡 Streaming com Kafka + Spark Streaming
- 🌐 Deploy em cloud (AWS EMR, Databricks)

## 🤝 Contribuição

1. Fork do projeto
2. Criar branch para feature (`git checkout -b feature/AmazingFeature`)
3. Commit das mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para branch (`git push origin feature/AmazingFeature`)
5. Abrir Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 👨‍💻 Autor

Desenvolvido como solução completa de pipeline de dados com PySpark, Docker e Airflow.

---

**🎯 Ready to process some data? Execute `./run.sh pipeline` and let's go!**
