#!/usr/bin/env python3

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, desc, asc, to_timestamp, from_unixtime
from pyspark.sql.types import DoubleType
from tabulate import tabulate
import json
from datetime import datetime

# Adicionar o diretório atual ao path para importar nossa classe
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from quality_check import QualityCheck


class CreditDataPipeline:
    """
    Pipeline de processamento de dados de transações de crédito
    """
    
    def __init__(self):
        self.spark = None
        self.quality_checker = None
        self.raw_df = None
        self.cleaned_df = None
        
    def initialize_spark(self):
        """Inicializar sessão Spark"""
        print("🚀 Inicializando Spark Session...")
        
        self.spark = SparkSession.builder \
            .appName("CreditDataPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Configurar nível de log
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Inicializar quality checker
        self.quality_checker = QualityCheck(self.spark)
        
        print("✅ Spark Session inicializada!")
        
    def debug_environment(self):
        """Debug do ambiente e volumes"""
        print("\n🔍 DEBUG DO AMBIENTE")
        print("=" * 40)
        
        # Verificar diretório atual
        current_dir = os.getcwd()
        print(f"📁 Diretório atual: {current_dir}")
        
        # Verificar variáveis de ambiente
        data_path = os.getenv("DATA_PATH", "/data/df_credit_amostra.csv")
        results_path = os.getenv("RESULTS_PATH", "/results")
        print(f"📊 DATA_PATH: {data_path}")
        print(f"💾 RESULTS_PATH: {results_path}")
        
        # Verificar se diretórios existem
        dirs_to_check = ["/app", "/data", "/results", "/logs"]
        for dir_path in dirs_to_check:
            if os.path.exists(dir_path):
                print(f"✅ Diretório existe: {dir_path}")
                try:
                    contents = os.listdir(dir_path)
                    print(f"   📂 Conteúdo ({len(contents)} itens): {contents[:5]}")  # Primeiros 5
                except PermissionError:
                    print(f"   ⚠️  Sem permissão para listar: {dir_path}")
            else:
                print(f"❌ Diretório NÃO existe: {dir_path}")
        
        # Verificar arquivo de dados específico
        if os.path.exists(data_path):
            size = os.path.getsize(data_path)
            print(f"✅ Arquivo de dados encontrado: {data_path} ({size} bytes)")
        else:
            print(f"❌ Arquivo de dados NÃO encontrado: {data_path}")
            
        # Verificar permissões do diretório results
        try:
            test_file = os.path.join(results_path, "test_write.txt")
            with open(test_file, 'w') as f:
                f.write("teste")
            os.remove(test_file)
            print(f"✅ Diretório results é gravável: {results_path}")
        except Exception as e:
            print(f"❌ Erro ao testar gravação em results: {e}")
        
    def load_data(self, file_path: str):
        """Carregar dados do arquivo CSV"""
        print(f"\n📂 Carregando dados de: {file_path}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        self.raw_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        print(f"📊 Dados carregados: {self.raw_df.count()} registros, {len(self.raw_df.columns)} colunas")
        
        # Mostrar schema
        print("\n Schema dos dados:")
        self.raw_df.printSchema()
        
        return self.raw_df
    
    def clean_data(self):
        """Limpeza e transformação dos dados"""
        print("\n Iniciando limpeza dos dados...")
        
        if self.raw_df is None:
            raise ValueError("Dados não carregados. Execute load_data() primeiro.")
        
        # Fazer uma cópia para limpeza
        df = self.raw_df
        
        print("📝 Aplicando transformações:")
        
        # 1. Converter risk_score para double (tratar 'none' como null)
        print("   - Convertendo risk_score para double")
        df = df.withColumn("risk_score_clean", 
                          col("risk_score").cast("string"))
        
        # Substituir 'none' por null e converter para double
        from pyspark.sql.functions import when
        df = df.withColumn("risk_score", 
                          when(col("risk_score_clean") == "none", None)
                          .otherwise(col("risk_score_clean").cast(DoubleType())))
        df = df.drop("risk_score_clean")
        
        # 2. Converter timestamp para formato de data legível
        print("   - Convertendo timestamp para formato de data")
        df = df.withColumn("timestamp_readable", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        
        # 3. Remover registros com valores críticos nulos (exceto risk_score)
        print("   - Removendo registros com valores críticos nulos")
        initial_count = df.count()
        
        # Colunas críticas que não podem ser nulas (removemos risk_score da lista)
        critical_columns = ["timestamp", "receiving_address", "amount", "transaction_type", "location_region"]
        
        for column in critical_columns:
            df = df.filter(col(column).isNotNull())
        
        final_count = df.count()
        removed_count = initial_count - final_count
        
        if removed_count > 0:
            print(f"   ⚠️  Removidos {removed_count} registros com valores nulos em colunas críticas")
        
        # 4. Filtrar valores de amount negativos ou zero (se existirem)
        print("   - Filtrando amounts inválidos")
        df = df.filter(col("amount") > 0)
        
        # 5. Padronizar strings
        print("   - Padronizando strings")
        string_columns = ["transaction_type", "location_region", "purchase_pattern", "age_group", "anomaly"]
        for column in string_columns:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast("string"))
        
        self.cleaned_df = df
        
        print(f"✅ Limpeza concluída! Registros finais: {self.cleaned_df.count()}")
        
        return self.cleaned_df
    
    def create_location_risk_analysis(self):
        """
        Criar tabela-resultado: location_region por média de risk_score (ordem decrescente)
        """
        print("\n📍 Criando análise de risco por localização...")
        
        if self.cleaned_df is None:
            raise ValueError("Dados limpos não disponíveis. Execute clean_data() primeiro.")
        
        # Filtrar apenas registros com risk_score válido para esta análise
        valid_risk_df = self.cleaned_df.filter(col("risk_score").isNotNull())
        
        # Calcular média de risk_score por location_region
        location_risk_df = valid_risk_df.groupBy("location_region") \
            .agg(
                avg("risk_score").alias("avg_risk_score"),
                col("location_region")
            ) \
            .select("location_region", "avg_risk_score") \
            .orderBy(desc("avg_risk_score"))
        
        print("📊 Resultado - Location Regions por Média de Risk Score:")
        
        # Converter para Pandas para visualização
        result_pandas = location_risk_df.toPandas()
        result_pandas["avg_risk_score"] = result_pandas["avg_risk_score"].round(2)
        
        print(tabulate(result_pandas, headers=['Location Region', 'Avg Risk Score'], 
                      tablefmt='grid', showindex=False))
        
        # Salvar resultado - MÚLTIPLOS FORMATOS
        results_path = os.getenv("RESULTS_PATH", "/results")
        
        # CSV
        csv_path = os.path.join(results_path, "location_risk_analysis.csv")
        result_pandas.to_csv(csv_path, index=False)
        print(f"💾 Resultado salvo em CSV: {csv_path}")
        
        # JSON
        json_path = os.path.join(results_path, "location_risk_analysis.json")
        result_pandas.to_json(json_path, orient='records', indent=2)
        print(f"💾 Resultado salvo em JSON: {json_path}")
        
        # Parquet via Spark (para compatibilidade)
        try:
            parquet_path = os.path.join(results_path, "location_risk_analysis_parquet")
            location_risk_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
            print(f"💾 Resultado salvo em Parquet: {parquet_path}")
        except Exception as e:
            print(f"⚠️  Aviso: Não foi possível salvar Parquet: {e}")
        
        return location_risk_df
    
    def create_top_sale_addresses_analysis(self):
        """
        Criar tabela-resultado: 3 receiving_address com maior amount 
        considerando apenas a transação mais recente de "sale" de cada receiving_address
        """
        print("\n💰 Criando análise dos top 3 receiving addresses para vendas...")
        
        if self.cleaned_df is None:
            raise ValueError("Dados limpos não disponíveis. Execute clean_data() primeiro.")
        
        # Filtrar apenas transações do tipo "sale"
        sales_df = self.cleaned_df.filter(col("transaction_type") == "sale")
        
        print(f"   📋 Total de transações de venda: {sales_df.count()}")
        
        if sales_df.count() == 0:
            print("   ⚠️  Nenhuma transação de venda encontrada!")
            return None
        
        # Para cada receiving_address, pegar apenas a transação mais recente
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        # Criar window function particionando por receiving_address e ordenando por timestamp desc
        window_spec = Window.partitionBy("receiving_address").orderBy(desc("timestamp"))
        
        # Adicionar row_number e filtrar apenas a linha 1 (mais recente)
        latest_sales_df = sales_df.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        # Ordenar por amount decrescente e pegar os top 3
        top_3_sales = latest_sales_df.orderBy(desc("amount")).limit(3)
        
        # Selecionar apenas as colunas necessárias
        result_df = top_3_sales.select("receiving_address", "amount", "timestamp", "timestamp_readable")
        
        print("🏆 Top 3 Receiving Addresses (última transação de venda):")
        
        # Converter para Pandas para visualização
        result_pandas = result_df.toPandas()
        
        print(tabulate(result_pandas[["receiving_address", "amount", "timestamp_readable"]], 
                      headers=['Receiving Address', 'Amount', 'Timestamp'], 
                      tablefmt='grid', showindex=False))
        
        # Salvar resultado - MÚLTIPLOS FORMATOS
        results_path = os.getenv("RESULTS_PATH", "/results")
        
        # CSV
        csv_path = os.path.join(results_path, "top_sale_addresses.csv")
        result_pandas.to_csv(csv_path, index=False)
        print(f"💾 Resultado salvo em CSV: {csv_path}")
        
        # JSON
        json_path = os.path.join(results_path, "top_sale_addresses.json")
        result_pandas.to_json(json_path, orient='records', indent=2)
        print(f"💾 Resultado salvo em JSON: {json_path}")
        
        # Parquet via Spark
        try:
            parquet_path = os.path.join(results_path, "top_sale_addresses_parquet")
            result_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
            print(f"💾 Resultado salvo em Parquet: {parquet_path}")
        except Exception as e:
            print(f"⚠️  Aviso: Não foi possível salvar Parquet: {e}")
        
        return result_df
    
    def save_quality_reports(self):
        """Salvar relatórios de qualidade"""
        results_path = os.getenv("RESULTS_PATH", "/results")
        
        # Salvar relatório JSON
        json_path = os.path.join(results_path, "quality_report.json")
        self.quality_checker.save_report_to_file(json_path)
        
        # Criar relatório resumido em TXT
        txt_path = os.path.join(results_path, "quality_summary.txt")
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write("RELATÓRIO DE QUALIDADE DE DADOS\n")
            f.write("=" * 50 + "\n\n")
            
            if 'cleaned_data' in self.quality_checker.quality_report:
                report = self.quality_checker.quality_report['cleaned_data']
                f.write(f"Timestamp: {report['timestamp']}\n")
                f.write(f"Total de registros: {report['basic_metrics']['total_records']}\n")
                f.write(f"Total de colunas: {report['basic_metrics']['total_columns']}\n")
                f.write(f"Score de qualidade: {report['quality_score']}/100\n\n")
                
                f.write(f"Colunas com nulos: {report['null_analysis']['columns_with_nulls']}\n")
                f.write(f"Registros duplicados: {report['duplicate_analysis']['duplicate_count']}\n")
                f.write(f"Anomalias detectadas: {len(report['anomalies'])}\n")
        
        print(f"💾 Relatório de qualidade salvo: {txt_path}")
    
    def run_pipeline(self, file_path: str = "/data/df_credit_amostra.csv"):
        """Executar pipeline completo"""
        try:
            print("🎯 INICIANDO PIPELINE DE DADOS DE CRÉDITO v2.0")
            print("="*60)
            
            # Debug do ambiente
            self.debug_environment()
            
            # Inicializar Spark
            self.initialize_spark()
            
            # Carregar dados
            self.load_data(file_path)
            
            # Verificar qualidade dos dados brutos
            print("\n🔍 VERIFICAÇÃO DE QUALIDADE - DADOS BRUTOS")
            self.quality_checker.check_data_quality(self.raw_df, "raw_data")
            
            # Limpar dados
            self.clean_data()
            
            # Verificar qualidade dos dados limpos
            print("\n🔍 VERIFICAÇÃO DE QUALIDADE - DADOS LIMPOS")
            self.quality_checker.check_data_quality(self.cleaned_df, "cleaned_data")
            
            # Executar análises
            print("\n📊 EXECUTANDO ANÁLISES")
            print("-" * 40)
            
            self.create_location_risk_analysis()
            self.create_top_sale_addresses_analysis()
            
            # Salvar relatórios finais
            self.save_quality_reports()
            
            print("\n🎉 PIPELINE EXECUTADO COM SUCESSO!")
            
            # Verificar arquivos salvos
            results_path = os.getenv("RESULTS_PATH", "/results")
            if os.path.exists(results_path):
                files = os.listdir(results_path)
                print(f"📁 Arquivos salvos em {results_path}:")
                for file in sorted(files):
                    file_path_full = os.path.join(results_path, file)
                    if os.path.isfile(file_path_full):
                        size = os.path.getsize(file_path_full)
                        print(f"   📄 {file} ({size} bytes)")
                    else:
                        print(f"   📁 {file}/ (diretório)")
            
        except Exception as e:
            print(f"❌ Erro na execução do pipeline: {str(e)}")
            import traceback
            traceback.print_exc()
            raise e
        
        finally:
            if self.spark:
                print("\n🔚 Encerrando Spark Session...")
                self.spark.stop()


def main():
    """Função principal"""
    pipeline = CreditDataPipeline()
    
    # Verificar se arquivo existe
    file_path = "/data/df_credit_amostra.csv"
    if not os.path.exists(file_path):
        # Tentar path local para desenvolvimento
        file_path = "df_credit_amostra.csv"
        if not os.path.exists(file_path):
            print("❌ Arquivo de dados não encontrado!")
            print("   Verificar se o arquivo está em /data/df_credit_amostra.csv")
            return
    
    pipeline.run_pipeline(file_path)


if __name__ == "__main__":
    main()
