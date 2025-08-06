"""
=============================================================================
GUIA COMPLETO DE DATA ENGINEERING PARA ENTREVISTAS
Pipelines ETL, Big Data, Cloud, Streaming e Arquitetura de Dados
=============================================================================
"""

import pandas as pd
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
import threading
import time

print("=== GUIA DATA ENGINEERING PARA ENTREVISTAS ===\n")

# =============================================================================
# 1. DESIGN DE PIPELINES ETL
# =============================================================================

print("1. DESIGN DE PIPELINES ETL")
print("=" * 50)

# Padrão ETL com classes
class DataExtractor(ABC):
    """Classe abstrata para extração de dados"""
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass

class DatabaseExtractor(DataExtractor):
    """Extrai dados de banco SQL"""
    
    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query
    
    def extract(self) -> pd.DataFrame:
        print(f"Extraindo dados do banco: {self.connection_string}")
        # Simulação de extração
        return pd.DataFrame({
            'id': range(1, 1001),
            'user_id': range(1001, 2001),
            'event_time': pd.date_range('2024-01-01', periods=1000, freq='H'),
            'event_type': ['login', 'purchase', 'logout'] * 334
        })

class APIExtractor(DataExtractor):
    """Extrai dados de API REST"""
    
    def __init__(self, endpoint: str, headers: Dict[str, str] = None):
        self.endpoint = endpoint
        self.headers = headers or {}
    
    def extract(self) -> pd.DataFrame:
        print(f"Extraindo dados da API: {self.endpoint}")
        # Simulação de API
        data = [
            {'customer_id': i, 'orders': i % 10, 'revenue': i * 100.5}
            for i in range(1, 501)
        ]
        return pd.DataFrame(data)

class DataTransformer:
    """Aplica transformações nos dados"""
    
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicatas e valores nulos"""
        print("Limpando dados...")
        initial_rows = len(df)
        
        # Remove duplicatas
        df = df.drop_duplicates()
        
        # Remove valores nulos em colunas críticas
        df = df.dropna(subset=['id'] if 'id' in df.columns else [])
        
        final_rows = len(df)
        print(f"Linhas removidas: {initial_rows - final_rows}")
        return df
    
    @staticmethod
    def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas calculadas"""
        print("Adicionando colunas derivadas...")
        
        if 'event_time' in df.columns:
            df['hour'] = pd.to_datetime(df['event_time']).dt.hour
            df['day_of_week'] = pd.to_datetime(df['event_time']).dt.dayofweek
        
        if 'orders' in df.columns and 'revenue' in df.columns:
            df['avg_order_value'] = df['revenue'] / (df['orders'] + 1)  # +1 para evitar divisão por zero
        
        return df
    
    @staticmethod
    def aggregate_data(df: pd.DataFrame, group_cols: List[str], agg_dict: Dict) -> pd.DataFrame:
        """Agrega dados por grupos"""
        print(f"Agregando dados por: {group_cols}")
        return df.groupby(group_cols).agg(agg_dict).reset_index()

class DataLoader:
    """Carrega dados no destino final"""
    
    def __init__(self, destination_type: str = "database"):
        self.destination_type = destination_type
    
    def load(self, df: pd.DataFrame, table_name: str, mode: str = "append") -> bool:
        """Carrega dados no destino"""
        print(f"Carregando {len(df)} linhas na tabela {table_name} (modo: {mode})")
        
        if self.destination_type == "database":
            # Simulação de carga em banco
            print(f"✓ Dados carregados com sucesso no banco")
        elif self.destination_type == "data_lake":
            # Simulação de carga em data lake
            print(f"✓ Dados carregados no data lake: s3://bucket/{table_name}/")
        
        return True

# Pipeline ETL completo
class ETLPipeline:
    """Orquestra todo o processo ETL"""
    
    def __init__(self, extractor: DataExtractor, transformer: DataTransformer, loader: DataLoader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        logger = logging.getLogger('ETLPipeline')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    def run(self, table_name: str) -> bool:
        """Executa o pipeline completo"""
        try:
            self.logger.info("Iniciando pipeline ETL")
            
            # Extract
            self.logger.info("Fase de Extração")
            raw_data = self.extractor.extract()
            
            # Transform
            self.logger.info("Fase de Transformação")
            cleaned_data = self.transformer.clean_data(raw_data)
            transformed_data = self.transformer.add_derived_columns(cleaned_data)
            
            # Load
            self.logger.info("Fase de Carga")
            success = self.loader.load(transformed_data, table_name)
            
            if success:
                self.logger.info("Pipeline executado com sucesso!")
                return True
            else:
                self.logger.error("Falha na carga dos dados")
                return False
                
        except Exception as e:
            self.logger.error(f"Erro no pipeline: {str(e)}")
            return False

# Exemplo de uso
print("EXEMPLO DE PIPELINE ETL:")
extractor = DatabaseExtractor("postgresql://localhost/db", "SELECT * FROM events")
transformer = DataTransformer()
loader = DataLoader("database")

pipeline = ETLPipeline(extractor, transformer, loader)
pipeline.run("processed_events")
print()

# =============================================================================
# 2. PROCESSAMENTO DE DADOS EM BATCH
# =============================================================================

print("2. PROCESSAMENTO DE DADOS EM BATCH")
print("=" * 50)

class BatchProcessor:
    """Processa grandes volumes de dados em lotes"""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
    
    def process_file_in_batches(self, file_path: str, process_func) -> List[pd.DataFrame]:
        """Processa arquivo grande em lotes"""
        print(f"Processando arquivo em lotes de {self.batch_size}")
        
        results = []
        chunk_number = 0
        
        # Simulação de leitura em chunks
        # Em produção, usaria: pd.read_csv(file_path, chunksize=self.batch_size)
        for chunk_number in range(5):  # Simula 5 chunks
            print(f"Processando chunk {chunk_number + 1}")
            
            # Simula dados do chunk
            chunk_data = pd.DataFrame({
                'transaction_id': range(chunk_number * self.batch_size, (chunk_number + 1) * self.batch_size),
                'amount': [100.0 + i for i in range(self.batch_size)],
                'timestamp': pd.date_range('2024-01-01', periods=self.batch_size, freq='1min')
            })
            
            # Aplica função de processamento
            processed_chunk = process_func(chunk_data)
            results.append(processed_chunk)
        
        return results
    
    def parallel_batch_processing(self, data_chunks: List[pd.DataFrame], process_func) -> List[pd.DataFrame]:
        """Processa chunks em paralelo"""
        print("Processamento paralelo iniciado")
        
        def process_chunk_with_id(chunk_info):
            chunk_id, chunk = chunk_info
            print(f"Thread processando chunk {chunk_id}")
            time.sleep(0.5)  # Simula processamento
            return process_func(chunk)
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            chunk_info = [(i, chunk) for i, chunk in enumerate(data_chunks)]
            results = list(executor.map(process_chunk_with_id, chunk_info))
        
        print("Processamento paralelo concluído")
        return results

# Função de processamento de exemplo
def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula métricas nos dados"""
    df = df.copy()
    df['amount_squared'] = df['amount'] ** 2
    df['amount_log'] = df['amount'].apply(lambda x: x if x > 0 else 0)
    return df

# Exemplo de uso
print("EXEMPLO DE PROCESSAMENTO EM BATCH:")
processor = BatchProcessor(batch_size=500)
chunks = processor.process_file_in_batches("large_file.csv", calculate_metrics)
parallel_results = processor.parallel_batch_processing(chunks, calculate_metrics)
print(f"Processados {len(parallel_results)} chunks em paralelo")
print()

# =============================================================================
# 3. DATA QUALITY E VALIDAÇÃO
# =============================================================================

print("3. DATA QUALITY E VALIDAÇÃO")
print("=" * 50)

class DataQualityChecker:
    """Verifica qualidade dos dados"""
    
    def __init__(self):
        self.quality_report = {}
    
    def check_completeness(self, df: pd.DataFrame) -> Dict[str, float]:
        """Verifica completude dos dados"""
        completeness = {}
        for col in df.columns:
            non_null_pct = (df[col].notna().sum() / len(df)) * 100
            completeness[col] = round(non_null_pct, 2)
        
        self.quality_report['completeness'] = completeness
        return completeness
    
    def check_duplicates(self, df: pd.DataFrame, subset: List[str] = None) -> Dict[str, Any]:
        """Verifica duplicatas"""
        if subset is None:
            duplicates = df.duplicated().sum()
        else:
            duplicates = df.duplicated(subset=subset).sum()
        
        duplicate_info = {
            'total_duplicates': duplicates,
            'duplicate_percentage': round((duplicates / len(df)) * 100, 2)
        }
        
        self.quality_report['duplicates'] = duplicate_info
        return duplicate_info
    
    def check_data_types(self, df: pd.DataFrame, expected_types: Dict[str, str]) -> Dict[str, bool]:
        """Verifica tipos de dados"""
        type_check = {}
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                type_check[col] = expected_type in actual_type
            else:
                type_check[col] = False
        
        self.quality_report['data_types'] = type_check
        return type_check
    
    def check_value_ranges(self, df: pd.DataFrame, range_rules: Dict[str, Dict]) -> Dict[str, Dict]:
        """Verifica se valores estão em ranges esperados"""
        range_violations = {}
        
        for col, rules in range_rules.items():
            if col not in df.columns:
                continue
            
            violations = {}
            
            if 'min' in rules:
                below_min = (df[col] < rules['min']).sum()
                violations['below_min'] = below_min
            
            if 'max' in rules:
                above_max = (df[col] > rules['max']).sum()
                violations['above_max'] = above_max
            
            if 'allowed_values' in rules:
                invalid_values = (~df[col].isin(rules['allowed_values'])).sum()
                violations['invalid_values'] = invalid_values
            
            range_violations[col] = violations
        
        self.quality_report['range_violations'] = range_violations
        return range_violations
    
    def generate_report(self) -> str:
        """Gera relatório completo de qualidade"""
        report = ["=== DATA QUALITY REPORT ===\n"]
        
        if 'completeness' in self.quality_report:
            report.append("COMPLETENESS:")
            for col, pct in self.quality_report['completeness'].items():
                status = "✓" if pct >= 95 else "⚠️" if pct >= 80 else "❌"
                report.append(f"  {col}: {pct}% {status}")
            report.append("")
        
        if 'duplicates' in self.quality_report:
            dup_info = self.quality_report['duplicates']
            status = "✓" if dup_info['duplicate_percentage'] == 0 else "⚠️"
            report.append(f"DUPLICATES: {dup_info['total_duplicates']} ({dup_info['duplicate_percentage']}%) {status}")
            report.append("")
        
        if 'data_types' in self.quality_report:
            report.append("DATA TYPES:")
            for col, is_correct in self.quality_report['data_types'].items():
                status = "✓" if is_correct else "❌"
                report.append(f"  {col}: {status}")
            report.append("")
        
        return "\n".join(report)

# Exemplo de uso
print("EXEMPLO DE DATA QUALITY:")
# Criar dados de teste com problemas intencionais
test_data = pd.DataFrame({
    'user_id': [1, 2, 3, 3, 5, None],  # Duplicata e valor nulo
    'age': [25, 30, -5, 150, 35, 40],  # Valores fora do range
    'status': ['active', 'inactive', 'active', 'invalid', 'active', 'inactive'],  # Valor inválido
    'revenue': [100.5, 200.0, 300.0, 400.0, 500.0, 600.0]
})

quality_checker = DataQualityChecker()

# Executar verificações
completeness = quality_checker.check_completeness(test_data)
duplicates = quality_checker.check_duplicates(test_data, subset=['user_id'])
type_check = quality_checker.check_data_types(test_data, {
    'user_id': 'int',
    'age': 'int',
    'status': 'object',
    'revenue': 'float'
})
range_check = quality_checker.check_value_ranges(test_data, {
    'age': {'min': 0, 'max': 120},
    'status': {'allowed_values': ['active', 'inactive']},
    'revenue': {'min': 0}
})

print(quality_checker.generate_report())

# =============================================================================
# 4. DESIGN DE SCHEMA E MODELAGEM DE DADOS
# =============================================================================

print("4. DESIGN DE SCHEMA E MODELAGEM DE DADOS")
print("=" * 50)

class SchemaDesigner:
    """Auxilia no design de schemas de data warehouse"""
    
    @staticmethod
    def create_star_schema_example():
        """Exemplo de Star Schema"""
        print("STAR SCHEMA DESIGN:")
        print("""
        FACT TABLE (fact_sales):
        - sale_id (PK)
        - customer_id (FK)
        - product_id (FK)
        - date_id (FK)
        - store_id (FK)
        - quantity
        - unit_price
        - total_amount
        
        DIMENSION TABLES:
        
        dim_customer:
        - customer_id (PK)
        - customer_name
        - customer_email
        - customer_segment
        - registration_date
        
        dim_product:
        - product_id (PK)
        - product_name
        - category
        - brand
        - unit_cost
        
        dim_date:
        - date_id (PK)
        - full_date
        - year
        - month
        - day
        - quarter
        - day_of_week
        
        dim_store:
        - store_id (PK)
        - store_name
        - city
        - state
        - region
        """)
    
    @staticmethod
    def create_scd_type2_example():
        """Exemplo de Slowly Changing Dimension Type 2"""
        print("SCD TYPE 2 EXAMPLE (dim_customer):")
        
        scd_data = pd.DataFrame({
            'customer_key': [1, 2, 3, 4],  # Surrogate key
            'customer_id': [101, 101, 102, 102],  # Natural key
            'customer_name': ['João Silva', 'João Silva Santos', 'Maria Costa', 'Maria Costa Lima'],
            'email': ['joao@email.com', 'joao.santos@email.com', 'maria@email.com', 'maria.lima@email.com'],
            'valid_from': ['2023-01-01', '2023-06-01', '2023-01-01', '2023-08-01'],
            'valid_to': ['2023-05-31', '9999-12-31', '2023-07-31', '9999-12-31'],
            'is_current': [False, True, False, True]
        })
        
        print(scd_data.to_string(index=False))
        print("\nVantagens SCD Type 2:")
        print("- Mantém histórico completo de mudanças")
        print("- Permite análises históricas precisas")
        print("- Suporta relatórios 'point-in-time'")

# Exemplo de uso
schema_designer = SchemaDesigner()
schema_designer.create_star_schema_example()
print()
schema_designer.create_scd_type2_example()
print()

# =============================================================================
# 5. MONITORAMENTO E ALERTAS
# =============================================================================

print("5. MONITORAMENTO E ALERTAS")
print("=" * 50)

class DataPipelineMonitor:
    """Monitora execução de pipelines de dados"""
    
    def __init__(self):
        self.metrics = {}
        self.alerts = []
    
    def track_pipeline_execution(self, pipeline_name: str, start_time: datetime, 
                               end_time: datetime, status: str, records_processed: int):
        """Registra métricas de execução"""
        duration = (end_time - start_time).total_seconds()
        
        self.metrics[pipeline_name] = {
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': duration,
            'status': status,
            'records_processed': records_processed,
            'throughput_records_per_second': records_processed / duration if duration > 0 else 0
        }
    
    def check_sla_compliance(self, pipeline_name: str, sla_minutes: int) -> bool:
        """Verifica se pipeline atendeu SLA"""
        if pipeline_name not in self.metrics:
            return False
        
        duration_minutes = self.metrics[pipeline_name]['duration_seconds'] / 60
        return duration_minutes <= sla_minutes
    
    def generate_alert(self, alert_type: str, message: str, severity: str = "WARNING"):
        """Gera alerta"""
        alert = {
            'timestamp': datetime.now(),
            'type': alert_type,
            'message': message,
            'severity': severity
        }
        self.alerts.append(alert)
        print(f"🚨 ALERT [{severity}]: {message}")
    
    def check_data_freshness(self, table_name: str, last_update: datetime, 
                           max_age_hours: int = 24):
        """Verifica se dados estão atualizados"""
        age_hours = (datetime.now() - last_update).total_seconds() / 3600
        
        if age_hours > max_age_hours:
            self.generate_alert(
                "DATA_FRESHNESS",
                f"Tabela {table_name} não atualizada há {age_hours:.1f} horas",
                "ERROR"
            )
            return False
        return True
    
    def monitor_data_volume(self, table_name: str, current_count: int, 
                          expected_min: int, expected_max: int):
        """Monitora volume de dados"""
        if current_count < expected_min:
            self.generate_alert(
                "DATA_VOLUME",
                f"Volume baixo em {table_name}: {current_count} (min esperado: {expected_min})",
                "WARNING"
            )
        elif current_count > expected_max:
            self.generate_alert(
                "DATA_VOLUME",
                f"Volume alto em {table_name}: {current_count} (max esperado: {expected_max})",
                "WARNING"
            )
    
    def get_dashboard_metrics(self) -> Dict:
        """Retorna métricas para dashboard"""
        dashboard = {
            'total_pipelines': len(self.metrics),
            'successful_pipelines': len([m for m in self.metrics.values() if m['status'] == 'SUCCESS']),
            'failed_pipelines': len([m for m in self.metrics.values() if m['status'] == 'FAILED']),
            'total_alerts': len(self.alerts),
            'critical_alerts': len([a for a in self.alerts if a['severity'] == 'ERROR'])
        }
        return dashboard

# Exemplo de uso
print("EXEMPLO DE MONITORAMENTO:")
monitor = DataPipelineMonitor()

# Simular execução de pipelines
pipelines = [
    ("etl_customer_data", datetime.now() - timedelta(hours=1), datetime.now() - timedelta(minutes=45), "SUCCESS", 10000),
    ("etl_sales_data", datetime.now() - timedelta(hours=2), datetime.now() - timedelta(hours=1, minutes=30), "SUCCESS", 50000),
    ("etl_inventory_data", datetime.now() - timedelta(minutes=30), datetime.now() - timedelta(minutes=15), "FAILED", 0)
]

for pipeline_name, start, end, status, records in pipelines:
    monitor.track_pipeline_execution(pipeline_name, start, end, status, records)

# Verificar SLAs
for pipeline_name in ["etl_customer_data", "etl_sales_data", "etl_inventory_data"]:
    sla_ok = monitor.check_sla_compliance(pipeline_name, 60)  # SLA de 60 minutos
    print(f"Pipeline {pipeline_name} - SLA: {'✓' if sla_ok else '❌'}")

# Verificar freshness
monitor.check_data_freshness("customer_table", datetime.now() - timedelta(hours=25), 24)
monitor.check_data_volume("sales_table", 1500, 1000, 2000)

# Dashboard
dashboard = monitor.get_dashboard_metrics()
print(f"\nDASHBOARD METRICS:")
for metric, value in dashboard.items():
    print(f"  {metric}: {value}")

print()

# =============================================================================
# 6. CENÁRIOS DE ENTREVISTA - DESIGN DE SISTEMA
# =============================================================================

print("6. CENÁRIOS DE ENTREVISTA - DESIGN DE SISTEMA")
print("=" * 50)

def design_streaming_analytics_system():
    """Cenário: Design de sistema de analytics em tempo real"""
    print("CENÁRIO: Sistema de Analytics em Tempo Real para E-commerce")
    print("=" * 60)
    
    print("""
    REQUISITOS:
    - Processar 100K eventos/segundo (cliques, compras, visualizações)
    - Dashboards em tempo real (latência < 5 segundos)
    - Análises históricas (batch processing)
    - Alta disponibilidade (99.9% uptime)
    - Escalabilidade horizontal
    
    ARQUITETURA PROPOSTA:
    
    1. INGESTION LAYER:
       - Apache Kafka: Message broker para eventos
       - Schema Registry: Versionamento de schemas
       - Kafka Connect: Conectores para diversas fontes
    
    2. STREAM PROCESSING:
       - Apache Flink / Kafka Streams: Processamento em tempo real
       - Windowing: Agregações por tempo (1min, 5min, 1hour)
       - State Stores: Manter estado para joins e agregações
    
    3. STORAGE LAYER:
       - Apache Cassandra: Dados de baixa latência (dashboards)
       - Apache Hadoop/S3: Data Lake para dados históricos
       - Redis: Cache para queries frequentes
    
    4. BATCH PROCESSING:
       - Apache Spark: Processamento de grandes volumes
       - Apache Airflow: Orquestração de workflows
       - Delta Lake: Versionamento e ACID para data lake
    
    5. SERVING LAYER:
       - Apache Druid: OLAP engine para analytics
       - Elasticsearch: Busca e aggregações rápidas
       - GraphQL/REST APIs: Interface para aplicações
    
    6. MONITORING:
       - Prometheus + Grafana: Métricas de sistema
       - Apache Kafka monitoring: Lag, throughput
       - Data quality monitoring: Anomalias, volumes
    
    TRADE-OFFS:
    - Complexidade vs Performance
    - Consistência vs Disponibilidade (CAP Theorem)
    - Custo vs Latência
    - Schema evolution vs Backward compatibility
    """)

def design_data_lake_architecture():
    """Cenário: Arquitetura de Data Lake"""
    print("\nCENÁRIO: Arquitetura de Data Lake Empresarial")
    print("=" * 50)
    
    print("""
    REQUISITOS:
    - Múltiplas fontes de dados (APIs, DBs, arquivos)
    - Suporte para dados estruturados e não-estruturados
    - Self-service analytics para analistas
    - Machine Learning workflows
    - Governança e segurança
    
    ARQUITETURA (MEDALLION):
    
    1. BRONZE LAYER (Raw Data):
       - Formato: Parquet, Avro, JSON
       - Particionamento: Por data de ingestão
       - Schema: Schema-on-read
       - Retenção: 7 anos
    
    2. SILVER LAYER (Cleaned Data):
       - Dados limpos e validados
       - Deduplicação e padronização
       - Schema enforcement
       - SCD (Slowly Changing Dimensions)
    
    3. GOLD LAYER (Business Ready):
       - Agregações e métricas de negócio
       - Tabelas dimensionais
       - Data marts específicos
       - Pronto para consumo
    
    COMPONENTES:
    
    - Data Ingestion: Azure Data Factory / AWS Glue
    - Storage: Azure Data Lake / AWS S3
    - Processing: Apache Spark (Databricks/EMR)
    - Catalog: Apache Hive Metastore / AWS Glue Catalog
    - Security: Apache Ranger / AWS IAM
    - Orchestration: Apache Airflow / Azure Data Factory
    - Monitoring: Apache Atlas / AWS CloudWatch
    
    GOVERNANÇA:
    - Data Lineage: Rastreamento origem → destino
    - Data Quality: Validação automática
    - Security: RBAC, Column-level security
    - Privacy: GDPR compliance, data masking
    """)

# Executar cenários
design_streaming_analytics_system()
design_data_lake_architecture()

# =============================================================================
# 7. OTIMIZAÇÃO DE PERFORMANCE
# =============================================================================

print("\n7. OTIMIZAÇÃO DE PERFORMANCE")
print("=" * 50)

class PerformanceOptimizer:
    """Técnicas de otimização para pipelines de dados"""
    
    @staticmethod
    def optimize_spark_job():
        """Dicas de otimização para Spark"""
        print("OTIMIZAÇÃO APACHE SPARK:")
        print("""
        1. PARTICIONAMENTO:
           - Use .repartition() para distribuir dados uniformemente
           - Evite pequenas partições (< 128MB)
           - Particionamento por colunas frequentemente filtradas
        
        2. CACHING:
           - .cache() ou .persist() para DataFrames reutilizados
           - Escolha nível adequado (MEMORY_ONLY, MEMORY_AND_DISK)
           - Unpersist quando não precisar mais
        
        3. BROADCAST JOINS:
           - Use broadcast para tabelas pequenas (< 10MB)
           - spark.sql.adaptive.enabled=true (Spark 3.0+)
           - Evite joins desnecessários
        
        4. CONFIGURAÇÕES:
           - spark.sql.adaptive.enabled=true
           - spark.sql.adaptive.coalescePartitions.enabled=true
           - spark.sql.adaptive.skewJoin.enabled=true
        
        5. EVITAR:
           - collect() em DataFrames grandes
           - UDFs desnecessárias (use funções built-in)
           - Múltiplas ações no mesmo DataFrame
        """)
    
    @staticmethod
    def optimize_sql_queries():
        """Dicas de otimização para SQL"""
        print("OTIMIZAÇÃO SQL:")
        print("""
        1. INDEXAÇÃO:
           - Índices em colunas de WHERE e JOIN
           - Índices compostos para queries multi-coluna
           - Evite índices em colunas com baixa cardinalidade
        
        2. QUERY STRUCTURE:
           - Use LIMIT quando possível
           - WHERE antes de JOIN
           - EXISTS ao invés de IN com subqueries
           - UNION ALL ao invés de UNION quando duplicatas são OK
        
        3. PARTICIONAMENTO:
           - Partition pruning em data warehouses
           - Particionamento por data em time-series
           - Use filtros em colunas de partição
        
        4. ESTATÍSTICAS:
           - Mantenha estatísticas de tabela atualizadas
           - ANALYZE TABLE após grandes cargas
           - Histogramas para colunas com skew
        """)

# Exemplo de otimização
optimizer = PerformanceOptimizer()
optimizer.optimize_spark_job()
print()
optimizer.optimize_sql_queries()

# =============================================================================
# 8. EXERCÍCIOS PRÁTICOS DE ENTREVISTA
# =============================================================================

print("\n8. EXERCÍCIOS PRÁTICOS DE ENTREVISTA")
print("=" * 50)

print("EXERCÍCIO 1: Design de Pipeline de ML")
print("-" * 40)
print("""
PROBLEMA: Design um pipeline para treinar modelo de recomendação
- Dados: Interações usuário-produto (100GB/dia)
- Modelo: Collaborative Filtering
- Latência: Predições < 100ms
- Retreinamento: Diário

SUA SOLUÇÃO DEVE INCLUIR:
1. Arquitetura de dados
2. Feature engineering
3. Training pipeline
4. Serving infrastructure
5. Monitoring e A/B testing
""")

print("\nEXERCÍCIO 2: Recuperação de Desastre")
print("-" * 40)
print("""
PROBLEMA: Data pipeline crítico falhou e corrompeu dados
- Pipeline processa transações financeiras
- Última execução correta: 3 dias atrás
- RTO (Recovery Time Objective): 2 horas
- RPO (Recovery Point Objective): 1 hora

PLANO DE RECUPERAÇÃO:
1. Como identificar extensão da corrupção?
2. Estratégia de rollback
3. Reprocessamento de dados
4. Validação da integridade
5. Prevenção de futuros problemas
""")

print("\nEXERCÍCIO 3: Migração de Legacy")
print("-" * 40)
print("""
PROBLEMA: Migrar sistema legado para cloud
- Sistema atual: Oracle + ETL Informatica
- Destino: AWS (S3 + Redshift + Glue)
- Dados: 50TB, 200 tabelas
- Downtime máximo: 4 horas

PLANO DE MIGRAÇÃO:
1. Estratégia de migração (big bang vs incremental)
2. Data mapping e transformação
3. Testing e validação
4. Rollback plan
5. Timeline e marcos
""")

# =============================================================================
# 9. CHECKLIST PARA ENTREVISTAS
# =============================================================================

print("\n9. CHECKLIST PARA ENTREVISTAS DE DATA ENGINEERING")
print("=" * 60)

checklist = {
    "🏗️ ARQUITETURA": [
        "Lambda vs Kappa architecture",
        "Microservices vs Monolith para dados",
        "Event-driven architecture",
        "Data mesh concepts"
    ],
    
    "🔧 FERRAMENTAS": [
        "Apache Spark (RDDs, DataFrames, Datasets)",
        "Apache Kafka (producers, consumers, streams)",
        "Apache Airflow (DAGs, operators, sensors)",
        "Docker & Kubernetes",
        "Cloud platforms (AWS/GCP/Azure)"
    ],
    
    "💾 STORAGE": [
        "Data Lake vs Data Warehouse",
        "Columnar formats (Parquet, ORC)",
        "NoSQL databases (Cassandra, MongoDB)",
        "Time-series databases (InfluxDB, TimescaleDB)"
    ],
    
    "🚀 PERFORMANCE": [
        "Partitioning strategies",
        "Indexing best practices",
        "Caching layers",
        "Parallel processing",
        "Memory management"
    ],
    
    "🔍 MONITORING": [
        "Data quality metrics",
        "Pipeline observability",
        "Error handling strategies",
        "Alerting systems"
    ],
    
    "🔒 SECURITY": [
        "Data encryption (at rest, in transit)",
        "Access control (RBAC, ABAC)",
        "Data masking/anonymization",
        "Compliance (GDPR, CCPA)"
    ]
}

for category, items in checklist.items():
    print(f"\n{category}")
    for item in items:
        print(f"  ☐ {item}")

print("\n" + "=" * 60)
print("💡 DICAS FINAIS:")
print("- Sempre pergunte sobre volume de dados e SLAs")
print("- Discuta trade-offs (CAP theorem, performance vs custo)")
print("- Pense em escalabilidade desde o início")
print("- Considere failure scenarios e recovery")
print("- Mantenha-se atualizado com tecnologias cloud")
print("=" * 60)

print("\n🚀 PREPARAÇÃO CONCLUÍDA!")
print("Este guia cobre os principais conceitos de Data Engineering.")
print("Pratique os exercícios e esteja pronto para qualquer entrevista!")