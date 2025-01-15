from dagster import asset
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import duckdb
import os
from . import constants

# Este ativo processa dados de viagens de táxi e zonas de táxi para calcular estatísticas de Manhattan.
@asset(
    deps=["taxi_trips", "taxi_zones"]  # Define dependências: o processamento depende de dados anteriores.
)
def manhattan_stats() -> None:
    """
    Este ativo calcula as estatísticas de viagens de táxi em Manhattan, agrupadas por zona. 
    Os dados são armazenados em um arquivo JSON para visualização.
    """
    # Consulta SQL que conta o número de viagens por zona em Manhattan
    query = """
        select
            zones.zone,  -- Nome da zona
            zones.borough,  -- Bairro correspondente
            zones.geometry,  -- Geometria da zona (usada para mapas)
            count(1) as num_trips  -- Contagem de viagens por zona
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id  -- Junta as viagens com as zonas
        where borough = 'Manhattan' and geometry is not null  -- Filtra somente as zonas em Manhattan
        group by zone, borough, geometry
    """

    # Conecta ao banco de dados DuckDB onde os dados de táxi e zonas estão armazenados
    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))

    # Executa a consulta SQL e converte o resultado para um dataframe
    trips_by_zone = conn.execute(query).fetch_df()

    # Converte a geometria (em texto WKT) para uma estrutura geográfica utilizável
    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    # Salva os dados calculados em um arquivo JSON, pronto para visualização
    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

# Este ativo gera um mapa de calor com base nas estatísticas calculadas em "manhattan_stats".
@asset(
    deps=["manhattan_stats"]  # Define dependência: só será executado após "manhattan_stats".
)
def manhattan_map() -> None:
    """
    Este ativo gera um mapa de calor para visualizar o número de viagens de táxi em Manhattan, 
    usando as zonas e o número de viagens calculado.
    """
    # Carrega os dados do arquivo JSON gerado por "manhattan_stats"
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    # Gera o mapa de calor com a biblioteca Plotly
    fig = px.choropleth_mapbox(
        trips_by_zone,  # Dados a serem usados
        geojson=trips_by_zone.geometry.__geo_interface__,  # Dados geográficos (zonas de Manhattan)
        locations=trips_by_zone.index,  # Índice dos dados
        color='num_trips',  # Define a intensidade do mapa de calor com base no número de viagens
        color_continuous_scale='Plasma',  # Paleta de cores
        mapbox_style='carto-positron',  # Estilo do mapa
        center={'lat': 40.758, 'lon': -73.985},  # Centraliza o mapa em Manhattan
        zoom=11,  # Nível de zoom
        opacity=0.7,  # Transparência do mapa de calor
        labels={'num_trips': 'Número de Viagens'}  # Rótulo para a legenda
    )

    # Salva o mapa gerado como uma imagem
    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)
