from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum, round as spark_round
from typing import Dict, List, Any
import json
from datetime import datetime


class QualityCheck:
    """
    Classe para realizar verificações de qualidade de dados em DataFrames PySpark
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.quality_report = {}
        
    def check_data_quality(self, df: DataFrame, table_name: str = "dataset") -> Dict[str, Any]:
        """
        Executa todas as verificações de qualidade de dados
        """
        print(f"🔍 Iniciando verificação de qualidade para: {table_name}")
        
        # Métricas básicas
        total_records = df.count()
        total_columns = len(df.columns)
        
        self.quality_report[table_name] = {
            "timestamp": datetime.now().isoformat(),
            "basic_metrics": {
                "total_records": total_records,
                "total_columns": total_columns
            },
            "null_analysis": self._check_nulls(df),
            "duplicate_analysis": self._check_duplicates(df),
            "data_types_analysis": self._check_data_types(df),
            "statistical_analysis": self._check_statistics(df),
            "anomalies": self._check_anomalies(df),
            "quality_score": 0.0
        }
        
        # Calcular score de qualidade
        self.quality_report[table_name]["quality_score"] = self._calculate_quality_score(
            total_records, self.quality_report[table_name]
        )
        
        # Imprimir relatório
        self._print_quality_report(table_name)
        
        return self.quality_report[table_name]
    
    def _check_nulls(self, df: DataFrame) -> Dict[str, Any]:
        """Verificar valores nulos por coluna"""
        null_counts = []
        
        for column in df.columns:
            null_count = df.select(
                spark_sum(
                    when(col(column).isNull() | isnan(col(column)), 1).otherwise(0)
                ).alias("null_count")
            ).collect()[0]["null_count"]
            
            total_count = df.count()
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
            
            null_counts.append({
                "column": column,
                "null_count": null_count,
                "null_percentage": round(null_percentage, 2)
            })
        
        return {
            "null_details": null_counts,
            "columns_with_nulls": len([x for x in null_counts if x["null_count"] > 0])
        }
    
    def _check_duplicates(self, df: DataFrame) -> Dict[str, Any]:
        """Verificar registros duplicados"""
        total_records = df.count()
        unique_records = df.distinct().count()
        duplicate_count = total_records - unique_records
        duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
        
        return {
            "total_records": total_records,
            "unique_records": unique_records,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": round(duplicate_percentage, 2)
        }
    
    def _check_data_types(self, df: DataFrame) -> Dict[str, Any]:
        """Verificar tipos de dados"""
        data_types = []
        
        for column, dtype in df.dtypes:
            data_types.append({
                "column": column,
                "data_type": dtype
            })
        
        return {
            "data_types": data_types,
            "type_distribution": {
                dtype: len([x for x in data_types if x["data_type"] == dtype])
                for dtype in set([x["data_type"] for x in data_types])
            }
        }
    
    def _check_statistics(self, df: DataFrame) -> Dict[str, Any]:
        """Verificar estatísticas descritivas para colunas numéricas"""
        numeric_columns = [field.name for field in df.schema.fields 
                          if field.dataType.typeName() in ['double', 'float', 'integer', 'long']]
        
        if not numeric_columns:
            return {"message": "Nenhuma coluna numérica encontrada"}
        
        stats = df.select(numeric_columns).describe().collect()
        
        statistics = {}
        for col_name in numeric_columns:
            statistics[col_name] = {
                "count": int([row[col_name] for row in stats if row["summary"] == "count"][0]),
                "mean": float([row[col_name] for row in stats if row["summary"] == "mean"][0]),
                "stddev": float([row[col_name] for row in stats if row["summary"] == "stddev"][0]),
                "min": float([row[col_name] for row in stats if row["summary"] == "min"][0]),
                "max": float([row[col_name] for row in stats if row["summary"] == "max"][0])
            }
        
        return {
            "numeric_columns": numeric_columns,
            "statistics": statistics
        }
    
    def _check_anomalies(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Detectar possíveis anomalias nos dados"""
        anomalies = []
        
        # Verificar valores negativos em amount
        if "amount" in df.columns:
            negative_amounts = df.filter(col("amount") < 0).count()
            if negative_amounts > 0:
                anomalies.append({
                    "type": "negative_values",
                    "column": "amount",
                    "count": negative_amounts,
                    "description": "Valores negativos encontrados na coluna amount"
                })
        
        # Verificar timestamps muito antigos ou futuros
        if "timestamp" in df.columns:
            current_timestamp = 1698642474  # timestamp atual aproximado
            old_threshold = current_timestamp - (365 * 24 * 60 * 60 * 10)  # 10 anos atrás
            future_threshold = current_timestamp + (365 * 24 * 60 * 60)  # 1 ano no futuro
            
            old_timestamps = df.filter(col("timestamp") < old_threshold).count()
            future_timestamps = df.filter(col("timestamp") > future_threshold).count()
            
            if old_timestamps > 0:
                anomalies.append({
                    "type": "old_timestamps",
                    "column": "timestamp",
                    "count": old_timestamps,
                    "description": "Timestamps muito antigos encontrados"
                })
            
            if future_timestamps > 0:
                anomalies.append({
                    "type": "future_timestamps",
                    "column": "timestamp",
                    "count": future_timestamps,
                    "description": "Timestamps futuros encontrados"
                })
        
        return anomalies
    
    def _calculate_quality_score(self, total_records: int, quality_data: Dict[str, Any]) -> float:
        """Calcular score de qualidade (0-100)"""
        score = 100.0
        
        # Penalizar por nulls (máximo -20 pontos)
        null_penalty = min(20, quality_data["null_analysis"]["columns_with_nulls"] * 5)
        score -= null_penalty
        
        # Penalizar por duplicatas (máximo -30 pontos)
        duplicate_penalty = min(30, quality_data["duplicate_analysis"]["duplicate_percentage"])
        score -= duplicate_penalty
        
        # Penalizar por anomalias (máximo -20 pontos)
        anomaly_penalty = min(20, len(quality_data["anomalies"]) * 10)
        score -= anomaly_penalty
        
        # Score mínimo é 0
        return max(0.0, round(score, 2))
    
    def _print_quality_report(self, table_name: str):
        """Imprimir relatório de qualidade"""
        report = self.quality_report[table_name]
        
        print(f"\n{'='*60}")
        print(f"📋 RELATÓRIO DE QUALIDADE - {table_name.upper()}")
        print(f"{'='*60}")
        print(f"🕐 Timestamp: {report['timestamp']}")
        print(f"📊 Total de registros: {report['basic_metrics']['total_records']:,}")
        print(f"📋 Total de colunas: {report['basic_metrics']['total_columns']}")
        print(f"⭐ Score de qualidade: {report['quality_score']}/100")
        
        print(f"\n🔍 ANÁLISE DE NULOS:")
        print(f"   Colunas com nulos: {report['null_analysis']['columns_with_nulls']}")
        for null_info in report['null_analysis']['null_details']:
            if null_info['null_count'] > 0:
                print(f"   - {null_info['column']}: {null_info['null_count']} ({null_info['null_percentage']:.2f}%)")
        
        print(f"\n📋 ANÁLISE DE DUPLICATAS:")
        print(f"   Registros duplicados: {report['duplicate_analysis']['duplicate_count']} ({report['duplicate_analysis']['duplicate_percentage']:.2f}%)")
        
        print(f"\n⚠️  ANOMALIAS DETECTADAS: {len(report['anomalies'])}")
        for anomaly in report['anomalies']:
            print(f"   - {anomaly['type']}: {anomaly['count']} casos na coluna {anomaly['column']}")
        
        print(f"\n{'='*60}")
    
    def save_report_to_file(self, filename: str = "quality_report.json"):
        """Salvar relatório em arquivo JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.quality_report, f, ensure_ascii=False, indent=2)
        print(f"📁 Relatório salvo em: {filename}")
    
    def get_quality_alerts(self, table_name: str) -> List[str]:
        """Retornar lista de alertas de qualidade"""
        alerts = []
        report = self.quality_report.get(table_name, {})
        
        if report.get("quality_score", 100) < 80:
            alerts.append(f"🚨 Score de qualidade baixo: {report['quality_score']}/100")
        
        null_columns = report.get("null_analysis", {}).get("columns_with_nulls", 0)
        if null_columns > 0:
            alerts.append(f"⚠️  {null_columns} colunas com valores nulos")
        
        duplicate_pct = report.get("duplicate_analysis", {}).get("duplicate_percentage", 0)
        if duplicate_pct > 5:
            alerts.append(f"🔄 {duplicate_pct:.1f}% de registros duplicados")
        
        anomalies_count = len(report.get("anomalies", []))
        if anomalies_count > 0:
            alerts.append(f"🔍 {anomalies_count} tipos de anomalias detectadas")
        
        return alerts
