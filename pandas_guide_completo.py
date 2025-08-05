"""
=============================================================================
GUIA COMPLETO DE PANDAS PARA ENTREVISTAS
Operações essenciais: Leitura/Escrita, Agregações, Filtragem, 
Concatenação, Merge e Transformações
=============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("=== GUIA COMPLETO DE PANDAS PARA ENTREVISTAS ===\n")

# =============================================================================
# 1. CRIAÇÃO E LEITURA DE DATAFRAMES
# =============================================================================

print("1. CRIAÇÃO E LEITURA DE DATAFRAMES")
print("=" * 50)

# Criando DataFrames de diferentes formas
# Método 1: A partir de dicionário
dados_vendas = {
    'vendedor_id': [101, 102, 103, 101, 102, 103, 101],
    'produto': ['Notebook', 'Mouse', 'Teclado', 'Monitor', 'Headset', 'Webcam', 'Tablet'],
    'categoria': ['Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos'],
    'valor': [2500.00, 80.00, 150.00, 1200.00, 200.00, 300.00, 800.00],
    'quantidade': [1, 3, 2, 1, 1, 2, 1],
    'data_venda': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19', '2024-01-20', '2024-01-21'],
    'regiao': ['Sul', 'Norte', 'Centro', 'Sul', 'Norte', 'Centro', 'Sul']
}

df_vendas = pd.DataFrame(dados_vendas)
print("DataFrame criado a partir de dicionário:")
print(df_vendas.head())
print()

# Convertendo tipos de dados
df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
df_vendas['valor_total'] = df_vendas['valor'] * df_vendas['quantidade']

# Método 2: Lendo de diferentes fontes
print("Exemplos de leitura de arquivos:")
print("# CSV")
print("df = pd.read_csv('vendas.csv')")
print("df = pd.read_csv('vendas.csv', sep=';', encoding='utf-8')")
print()

print("# Excel")
print("df = pd.read_excel('vendas.xlsx', sheet_name='Vendas')")
print()

print("# JSON")
print("df = pd.read_json('vendas.json')")
print()

print("# SQL")
print("import sqlite3")
print("conn = sqlite3.connect('database.db')")
print("df = pd.read_sql('SELECT * FROM vendas', conn)")
print()

# =============================================================================
# 2. EXPLORAÇÃO INICIAL DOS DADOS
# =============================================================================

print("\n2. EXPLORAÇÃO INICIAL DOS DADOS")
print("=" * 50)

print("Informações básicas do DataFrame:")
print(f"Shape: {df_vendas.shape}")
print(f"Colunas: {list(df_vendas.columns)}")
print()

print("Tipos de dados:")
print(df_vendas.dtypes)
print()

print("Informações gerais:")
print(df_vendas.info())
print()

print("Estatísticas descritivas:")
print(df_vendas.describe())
print()

print("Valores únicos por coluna:")
for col in df_vendas.columns:
    print(f"{col}: {df_vendas[col].nunique()} valores únicos")
print()

# =============================================================================
# 3. SELEÇÃO E FILTRAGEM DE DADOS
# =============================================================================

print("\n3. SELEÇÃO E FILTRAGEM DE DADOS")
print("=" * 50)

# Seleção de colunas
print("Selecionando colunas específicas:")
colunas_importantes = df_vendas[['vendedor_id', 'produto', 'valor_total']]
print(colunas_importantes.head())
print()

# Filtragem com condições simples
print("Filtragem simples - vendas acima de R$ 1000:")
vendas_altas = df_vendas[df_vendas['valor_total'] > 1000]
print(vendas_altas[['produto', 'valor_total']])
print()

# Filtragem com múltiplas condições
print("Filtragem múltipla - vendas na região Sul com valor > R$ 500:")
filtro_complexo = df_vendas[
    (df_vendas['regiao'] == 'Sul') & 
    (df_vendas['valor_total'] > 500)
]
print(filtro_complexo[['produto', 'regiao', 'valor_total']])
print()

# Filtragem com isin()
print("Filtragem com isin() - produtos específicos:")
produtos_interesse = ['Notebook', 'Monitor', 'Tablet']
filtro_produtos = df_vendas[df_vendas['produto'].isin(produtos_interesse)]
print(filtro_produtos[['produto', 'valor_total']])
print()

# Filtragem por datas
print("Filtragem por período:")
inicio = pd.to_datetime('2024-01-17')
fim = pd.to_datetime('2024-01-20')
vendas_periodo = df_vendas[
    (df_vendas['data_venda'] >= inicio) & 
    (df_vendas['data_venda'] <= fim)
]
print(vendas_periodo[['produto', 'data_venda', 'valor_total']])
print()

# =============================================================================
# 4. AGREGAÇÕES E GROUP BY
# =============================================================================

print("\n4. AGREGAÇÕES E GROUP BY")
print("=" * 50)

# Agregações básicas
print("Estatísticas gerais:")
print(f"Faturamento total: R$ {df_vendas['valor_total'].sum():,.2f}")
print(f"Ticket médio: R$ {df_vendas['valor_total'].mean():,.2f}")
print(f"Maior venda: R$ {df_vendas['valor_total'].max():,.2f}")
print(f"Total de vendas: {len(df_vendas)}")
print()

# Group by simples
print("Faturamento por vendedor:")
faturamento_vendedor = df_vendas.groupby('vendedor_id')['valor_total'].agg([
    'sum', 'count', 'mean', 'max'
]).round(2)
faturamento_vendedor.columns = ['Faturamento_Total', 'Qtd_Vendas', 'Ticket_Medio', 'Maior_Venda']
print(faturamento_vendedor)
print()

# Group by múltiplas colunas
print("Faturamento por vendedor e região:")
faturamento_vendedor_regiao = df_vendas.groupby(['vendedor_id', 'regiao']).agg({
    'valor_total': ['sum', 'count'],
    'quantidade': 'sum'
}).round(2)
print(faturamento_vendedor_regiao)
print()

# Agregações personalizadas
print("Agregações personalizadas:")
def calcular_performance(serie):
    return {
        'min_venda': serie.min(),
        'max_venda': serie.max(),
        'amplitude': serie.max() - serie.min(),
        'desvio_padrao': serie.std()
    }

performance_vendedor = df_vendas.groupby('vendedor_id')['valor_total'].apply(
    calcular_performance
).apply(pd.Series).round(2)
print(performance_vendedor)
print()

# =============================================================================
# 5. OPERAÇÕES DE MERGE E JOIN
# =============================================================================

print("\n5. OPERAÇÕES DE MERGE E JOIN")
print("=" * 50)

# Criando DataFrames auxiliares para demonstrar joins
df_vendedores = pd.DataFrame({
    'vendedor_id': [101, 102, 103, 104],
    'nome': ['João Silva', 'Maria Santos', 'Pedro Costa', 'Ana Lima'],
    'equipe': ['A', 'B', 'A', 'B'],
    'meta_mensal': [15000, 20000, 18000, 16000]
})

df_produtos = pd.DataFrame({
    'produto': ['Notebook', 'Mouse', 'Teclado', 'Monitor', 'Headset', 'Webcam', 'Tablet'],
    'categoria': ['Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos'],
    'custo': [2000, 50, 100, 900, 150, 200, 600],
    'fornecedor': ['Fornecedor A', 'Fornecedor B', 'Fornecedor B', 'Fornecedor A', 'Fornecedor C', 'Fornecedor C', 'Fornecedor A']
})

print("DataFrame vendedores:")
print(df_vendedores)
print()

print("DataFrame produtos:")
print(df_produtos)
print()

# INNER JOIN
print("INNER JOIN - vendas com dados dos vendedores:")
vendas_com_vendedores = pd.merge(
    df_vendas, 
    df_vendedores, 
    on='vendedor_id', 
    how='inner'
)
print(vendas_com_vendedores[['produto', 'nome', 'valor_total', 'equipe']].head())
print()

# LEFT JOIN
print("LEFT JOIN - todas as vendas, mesmo sem dados do produto:")
vendas_completas = pd.merge(
    df_vendas,
    df_produtos,
    on='produto',
    how='left'
)
print(vendas_completas[['produto', 'valor_total', 'custo', 'fornecedor']].head())
print()

# RIGHT JOIN
print("RIGHT JOIN - todos os vendedores, mesmo sem vendas:")
vendedores_vendas = pd.merge(
    df_vendas,
    df_vendedores,
    on='vendedor_id',
    how='right'
)
print("Vendedores sem vendas:")
vendedores_sem_vendas = vendedores_vendas[vendedores_vendas['produto'].isna()]
print(vendedores_sem_vendas[['vendedor_id', 'nome', 'equipe']])
print()

# Merge com múltiplas chaves
print("Merge com múltiplas colunas:")
# Criando dados com chave composta
df_estoque = pd.DataFrame({
    'produto': ['Notebook', 'Mouse', 'Teclado', 'Monitor'],
    'categoria': ['Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos'],
    'estoque_atual': [50, 200, 150, 80]
})

vendas_com_estoque = pd.merge(
    df_vendas,
    df_estoque,
    on=['produto', 'categoria'],
    how='left'
)
print(vendas_com_estoque[['produto', 'quantidade', 'estoque_atual']].head())
print()

# =============================================================================
# 6. CONCATENAÇÃO DE DATAFRAMES
# =============================================================================

print("\n6. CONCATENAÇÃO DE DATAFRAMES")
print("=" * 50)

# Criando DataFrames para demonstrar concatenação
df_vendas_q1 = df_vendas[df_vendas['data_venda'] <= '2024-01-18'].copy()
df_vendas_q2 = df_vendas[df_vendas['data_venda'] > '2024-01-18'].copy()

print(f"Vendas Q1: {len(df_vendas_q1)} registros")
print(f"Vendas Q2: {len(df_vendas_q2)} registros")

# Concatenação vertical (empilhar linhas)
print("\nConcatenação vertical:")
vendas_consolidadas = pd.concat([df_vendas_q1, df_vendas_q2], ignore_index=True)
print(f"Total após concatenação: {len(vendas_consolidadas)} registros")
print()

# Concatenação horizontal (adicionar colunas)
print("Concatenação horizontal:")
df_metricas = df_vendas.groupby('vendedor_id').agg({
    'valor_total': ['count', 'sum']
}).round(2)
df_metricas.columns = ['qtd_vendas', 'faturamento']
df_metricas = df_metricas.reset_index()

vendedores_com_metricas = pd.concat([
    df_vendedores.set_index('vendedor_id'),
    df_metricas.set_index('vendedor_id')
], axis=1).reset_index()
print(vendedores_com_metricas)
print()

# =============================================================================
# 7. TRANSFORMAÇÕES LINHA A LINHA E COLUNA A COLUNA
# =============================================================================

print("\n7. TRANSFORMAÇÕES LINHA A LINHA E COLUNA A COLUNA")
print("=" * 50)

# Aplicando funções em colunas
print("Transformações em colunas:")
df_vendas_copy = df_vendas.copy()

# Função lambda simples
df_vendas_copy['valor_com_desconto'] = df_vendas_copy['valor'].apply(lambda x: x * 0.9)

# Função mais complexa
def classificar_venda(valor):
    if valor >= 2000:
        return 'Alto Valor'
    elif valor >= 500:
        return 'Médio Valor'
    else:
        return 'Baixo Valor'

df_vendas_copy['classificacao'] = df_vendas_copy['valor_total'].apply(classificar_venda)
print(df_vendas_copy[['produto', 'valor_total', 'classificacao']].head())
print()

# Transformações linha a linha (apply em axis=1)
print("Transformações linha a linha:")
def calcular_margem(row):
    # Simulando cálculo de margem baseado em produto
    custos = {'Notebook': 2000, 'Mouse': 50, 'Teclado': 100, 'Monitor': 900, 
              'Headset': 150, 'Webcam': 200, 'Tablet': 600}
    custo = custos.get(row['produto'], 0)
    margem = ((row['valor'] - custo) / row['valor']) * 100
    return round(margem, 2)

df_vendas_copy['margem_pct'] = df_vendas_copy.apply(calcular_margem, axis=1)
print(df_vendas_copy[['produto', 'valor', 'margem_pct']].head())
print()

# Map para substituições rápidas
print("Usando map para substituições:")
mapeamento_regioes = {'Sul': 'S', 'Norte': 'N', 'Centro': 'C'}
df_vendas_copy['regiao_cod'] = df_vendas_copy['regiao'].map(mapeamento_regioes)
print(df_vendas_copy[['regiao', 'regiao_cod']].head())
print()

# =============================================================================
# 8. TRATAMENTO DE DADOS FALTANTES
# =============================================================================

print("\n8. TRATAMENTO DE DADOS FALTANTES")
print("=" * 50)

# Criando dados com valores faltantes para demonstração
df_com_nulos = df_vendas.copy()
df_com_nulos.loc[1, 'valor'] = np.nan
df_com_nulos.loc[3, 'regiao'] = np.nan
df_com_nulos.loc[5, 'quantidade'] = np.nan

print("Identificando valores faltantes:")
print(df_com_nulos.isnull().sum())
print()

print("Removendo linhas com valores faltantes:")
df_sem_nulos = df_com_nulos.dropna()
print(f"Registros antes: {len(df_com_nulos)}, depois: {len(df_sem_nulos)}")
print()

print("Preenchendo valores faltantes:")
# Preenchendo com valor específico
df_preenchido = df_com_nulos.fillna({
    'valor': df_com_nulos['valor'].mean(),
    'quantidade': df_com_nulos['quantidade'].median(),
    'regiao': 'Desconhecido'
})
print(df_preenchido.isnull().sum())
print()

# =============================================================================
# 9. ORDENAÇÃO E RANKING
# =============================================================================

print("\n9. ORDENAÇÃO E RANKING")
print("=" * 50)

# Ordenação simples
print("Top 5 vendas por valor:")
top_vendas = df_vendas.sort_values('valor_total', ascending=False).head()
print(top_vendas[['produto', 'valor_total', 'vendedor_id']])
print()

# Ordenação múltipla
print("Ordenação por vendedor e valor:")
vendas_ordenadas = df_vendas.sort_values(['vendedor_id', 'valor_total'], ascending=[True, False])
print(vendas_ordenadas[['vendedor_id', 'produto', 'valor_total']])
print()

# Ranking
print("Ranking de vendas:")
df_vendas_copy['ranking_geral'] = df_vendas['valor_total'].rank(ascending=False)
df_vendas_copy['ranking_por_vendedor'] = df_vendas.groupby('vendedor_id')['valor_total'].rank(ascending=False)
print(df_vendas_copy[['produto', 'vendedor_id', 'valor_total', 'ranking_geral', 'ranking_por_vendedor']])
print()

# =============================================================================
# 10. PIVOT TABLES E RESHAPE DE DADOS
# =============================================================================

print("\n10. PIVOT TABLES E RESHAPE DE DADOS")
print("=" * 50)

# Pivot Table
print("Pivot Table - Faturamento por vendedor e região:")
pivot_vendas = df_vendas.pivot_table(
    values='valor_total',
    index='vendedor_id',
    columns='regiao',
    aggfunc='sum',
    fill_value=0
)
print(pivot_vendas)
print()

# Melt (reshape long format)
print("Transformando pivot em formato long:")
pivot_melted = pivot_vendas.reset_index().melt(
    id_vars='vendedor_id',
    var_name='regiao',
    value_name='faturamento'
)
print(pivot_melted)
print()

# Crosstab
print("Crosstab - Contagem de vendas por vendedor e região:")
crosstab_vendas = pd.crosstab(df_vendas['vendedor_id'], df_vendas['regiao'])
print(crosstab_vendas)
print()

# =============================================================================
# 11. OPERAÇÕES COM DATAS
# =============================================================================

print("\n11. OPERAÇÕES COM DATAS")
print("=" * 50)

# Extraindo componentes de datas
df_datas = df_vendas.copy()
df_datas['ano'] = df_datas['data_venda'].dt.year
df_datas['mes'] = df_datas['data_venda'].dt.month
df_datas['dia_semana'] = df_datas['data_venda'].dt.day_name()
df_datas['trimestre'] = df_datas['data_venda'].dt.quarter

print("Componentes de data extraídos:")
print(df_datas[['data_venda', 'ano', 'mes', 'dia_semana', 'trimestre']].head())
print()

# Agregação por período
print("Faturamento por dia da semana:")
faturamento_dia = df_datas.groupby('dia_semana')['valor_total'].sum().sort_values(ascending=False)
print(faturamento_dia)
print()

# Criando ranges de datas
print("Criando range de datas:")
datas_range = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
print(f"Primeiras 5 datas: {datas_range[:5].tolist()}")
print()

# =============================================================================
# 12. ESCRITA DE DADOS
# =============================================================================

print("\n12. ESCRITA DE DADOS")
print("=" * 50)

print("Exemplos de escrita em diferentes formatos:")
print("# CSV")
print("df.to_csv('resultado.csv', index=False, encoding='utf-8')")
print()

print("# Excel")
print("df.to_excel('resultado.xlsx', sheet_name='Vendas', index=False)")
print()

print("# JSON")
print("df.to_json('resultado.json', orient='records', indent=2)")
print()

print("# Múltiplas abas no Excel")
print("with pd.ExcelWriter('relatorio.xlsx') as writer:")
print("    df_vendas.to_excel(writer, sheet_name='Vendas', index=False)")
print("    faturamento_vendedor.to_excel(writer, sheet_name='Por_Vendedor')")
print()

# =============================================================================
# 13. EXEMPLOS PRÁTICOS PARA ENTREVISTAS
# =============================================================================

print("\n13. EXEMPLOS PRÁTICOS PARA ENTREVISTAS")
print("=" * 50)

print("EXERCÍCIO 1: Top 3 produtos por vendedor")
top3_por_vendedor = (df_vendas
                    .groupby('vendedor_id')
                    .apply(lambda x: x.nlargest(3, 'valor_total')[['produto', 'valor_total']])
                    .reset_index(level=1, drop=True))
print(top3_por_vendedor)
print()

print("EXERCÍCIO 2: Vendedores que bateram meta (assumindo meta de R$ 3000)")
meta = 3000
performance_meta = (df_vendas
                   .groupby('vendedor_id')['valor_total']
                   .sum()
                   .reset_index())
performance_meta['bateu_meta'] = performance_meta['valor_total'] >= meta
performance_meta['percentual_meta'] = (performance_meta['valor_total'] / meta * 100).round(2)
print(performance_meta)
print()

print("EXERCÍCIO 3: Análise de crescimento diário")
crescimento_diario = (df_vendas
                     .groupby('data_venda')['valor_total']
                     .sum()
                     .reset_index()
                     .sort_values('data_venda'))
crescimento_diario['faturamento_anterior'] = crescimento_diario['valor_total'].shift(1)
crescimento_diario['crescimento_pct'] = (
    (crescimento_diario['valor_total'] - crescimento_diario['faturamento_anterior']) / 
    crescimento_diario['faturamento_anterior'] * 100
).round(2)
print(crescimento_diario)
print()

print("EXERCÍCIO 4: Produtos com maior variação de preço entre vendedores")
variacao_precos = (df_vendas
                  .groupby('produto')['valor']
                  .agg(['min', 'max', 'std'])
                  .round(2))
variacao_precos['variacao_pct'] = ((variacao_precos['max'] - variacao_precos['min']) / 
                                  variacao_precos['min'] * 100).round(2)
variacao_precos = variacao_precos.sort_values('variacao_pct', ascending=False)
print(variacao_precos)
print()

# =============================================================================
# 14. DICAS DE PERFORMANCE
# =============================================================================

print("\n14. DICAS DE PERFORMANCE E BOAS PRÁTICAS")
print("=" * 50)

print("✅ BOAS PRÁTICAS:")
print("1. Use vectorização em vez de loops quando possível")
print("2. Prefira query() para filtragens complexas")
print("3. Use categorical para strings repetitivas")
print("4. Especifique dtypes ao ler arquivos grandes")
print("5. Use chunksize para arquivos muito grandes")
print()

print("Exemplo de query():")
vendas_query = df_vendas.query('valor_total > 1000 and regiao == "Sul"')
print(f"Registros encontrados: {len(vendas_query)}")
print()

print("Exemplo de categorical:")
df_categorical = df_vendas.copy()
df_categorical['regiao'] = df_categorical['regiao'].astype('category')
print(f"Memória economizada: {df_vendas['regiao'].memory_usage(deep=True) - df_categorical['regiao'].memory_usage(deep=True)} bytes")
print()

print("=== FIM DO GUIA PANDAS ===")
print("Este guia cobre os principais conceitos de Pandas necessários para entrevistas técnicas.")
print("Pratique estes exemplos e adapte-os para seus próprios conjuntos de dados!")