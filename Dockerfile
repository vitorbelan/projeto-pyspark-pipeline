# Use uma imagem base oficial do Python com Spark
FROM apache/spark-py

# Definir variáveis de ambiente
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Instalar dependências do sistema
USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos de requisitos
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Criar diretórios necessários
RUN mkdir -p /data /results /logs /app/src

# Copiar código fonte
COPY src/ ./src/
COPY data/ ./data/

# Criar usuário não-root para segurança
RUN groupadd -r sparkuser && useradd -r -g sparkuser sparkuser
RUN chown -R sparkuser:sparkuser /app /data /results /logs

# Mudar para o usuário não-root
USER sparkuser

# Definir o ponto de entrada
ENTRYPOINT ["python3", "src/data_pipeline.py"]

# Comando padrão (pode ser sobrescrito)
CMD ["--help"]
