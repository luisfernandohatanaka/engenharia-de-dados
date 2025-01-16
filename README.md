Projeto: Análise e Visualização de Dados de Táxis de Nova York
Este projeto consiste na criação de um pipeline de dados utilizando Dagster, que coleta e processa dados de táxis de Nova York para gerar insights sobre as rotas mais frequentadas e os horários de maior movimento.

A análise é realizada a partir de dois conjuntos de dados:
 - Dados de viagens de táxis: Inclui informações sobre o percurso, distância, número de passageiros e valores totais das corridas.
 - Dados de zonas de táxis: Contém informações sobre as zonas de coleta e entrega dos passageiros em Nova York.

A pipeline realiza as seguintes etapas:
 - Coleta e processamento de dados: Utiliza APIs públicas para obter arquivos de dados brutos no formato Parquet e CSV e os armazena em um banco de dados DuckDB.

 - Processamento e agregação: Cria tabelas agregadas que calculam o número de viagens por zona e horário.

 - Visualização: Utiliza Plotly e GeoPandas para gerar um mapa de calor interativo que exibe as zonas de táxi em Manhattan, com base no número de viagens realizadas em cada área.

 - Alterações Recentes: Recentemente, o pipeline foi ajustado para usar links diretos do Amazon S3 em vez de arquivos locais, garantindo maior flexibilidade e escalabilidade no acesso aos dados armazenados.

Tecnologias utilizadas:
 - Dagster: Orquestração de pipelines de dados.
 - DuckDB: Banco de dados SQL de alto desempenho para análise de dados. 
 - Plotly e GeoPandas: Bibliotecas para visualização e análise geoespacial.
 - Amazon S3: Armazenamento de grandes arquivos de dados.
Esse projeto é uma ótima demonstração de como usar tecnologias de processamento de dados em grande escala para extrair e visualizar insights valiosos de dados do mundo real. 
