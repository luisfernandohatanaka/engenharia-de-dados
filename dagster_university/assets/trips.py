import requests  # Biblioteca para realizar requisições HTTP
from . import constants  # Importa constantes definidas em outro módulo
from dagster import asset  # Ferramenta de definição de ativos no Dagster
import duckdb  # Banco de dados em memória otimizado para análise
import os  # Para interagir com o sistema operacional
from dagster._utils.backoff import backoff  # Função para implementar retentativas automáticas

@asset
def taxi_trips_file() -> None:
    """
    Baixa os dados brutos dos trajetos de táxi no formato Parquet do bucket S3 e os salva localmente.
    """
    month_to_fetch = '2023-03'  # Define o mês de interesse
    # Faz a requisição do arquivo Parquet armazenado no S3
    raw_trips = requests.get(
        f"https://meu-projeto-arquivos-pesados.s3.us-east-2.amazonaws.com/yellow_tripdata_{month_to_fetch}.parquet"
    )

    # Salva o conteúdo do arquivo baixado no diretório local
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

@asset
def taxi_zones_file() -> None:
    """
    Baixa os dados das zonas de táxi em formato CSV do bucket S3 e os salva localmente.
    """
    # Faz a requisição do arquivo CSV armazenado no S3
    raw_taxi_zones = requests.get(
        "https://meu-projeto-arquivos-pesados.s3.us-east-2.amazonaws.com/taxi_zones_file.csv"
    )

    # Salva o conteúdo do arquivo baixado no diretório local
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_taxi_zones.content)

@asset(
    deps=["taxi_trips_file"]  # Define dependência do arquivo de trajetos
)
def taxi_trips() -> None:
    """
    Carrega os dados de trajetos de táxi no banco de dados DuckDB.
    """
    # Query SQL para criar ou substituir a tabela 'trips' no banco de dados
    query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    # Cria uma conexão com o banco de dados DuckDB e executa a query
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),  # Caminho do banco de dados fornecido por variável de ambiente
        },
        max_retries=10,  # Número máximo de tentativas em caso de falha
    )
    conn.execute(query)

@asset(
    deps=["taxi_zones_file"]  # Define dependência do arquivo de zonas de táxi
)
def taxi_zones() -> None:
    """
    Carrega os dados das zonas de táxi no banco de dados DuckDB.
    """
    # Query SQL para criar ou substituir a tabela 'zones' no banco de dados
    query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    # Cria uma conexão com o banco de dados DuckDB e executa a query
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),  # Caminho do banco de dados fornecido por variável de ambiente
        },
        max_retries=10,  # Número máximo de tentativas em caso de falha
    )
    conn.execute(query)
