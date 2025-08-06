"""
=============================================================================
EXERCÍCIOS PRÁTICOS PARA ENTREVISTAS - CENÁRIOS REAIS
Simulando problemas comuns em entrevistas de análise de dados
=============================================================================
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

print("=== EXERCÍCIOS PRÁTICOS PARA ENTREVISTAS ===\n")

# =============================================================================
# SETUP: CRIANDO BASE DE DADOS PARA OS EXERCÍCIOS
# =============================================================================

print("PREPARANDO BASE DE DADOS PARA EXERCÍCIOS")
print("=" * 50)

# Criando dados realistas para e-commerce
np.random.seed(42)

# Dados de clientes
clientes_data = {
    'cliente_id': range(1, 501),
    'nome': [f'Cliente_{i}' for i in range(1, 501)],
    'idade': np.random.randint(18, 70, 500),
    'sexo': np.random.choice(['M', 'F'], 500),
    'cidade': np.random.choice(['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 
                               'Porto Alegre', 'Recife', 'Salvador', 'Brasília'], 500),
    'data_cadastro': pd.date_range('2020-01-01', '2023-12-31', periods=500)
}
df_clientes = pd.DataFrame(clientes_data)

# Dados de produtos
produtos_nomes = ['Smartphone', 'Notebook', 'Tablet', 'Headphone', 'Mouse', 
                 'Teclado', 'Monitor', 'Webcam', 'Cabo USB', 'Carregador']
produtos_data = {
    'produto_id': range(1, 101),
    'nome_produto': np.random.choice(produtos_nomes, 100),
    'categoria': np.random.choice(['Eletrônicos', 'Acessórios', 'Informática'], 100),
    'preco': np.round(np.random.uniform(50, 3000, 100), 2),
    'custo': lambda x: np.round(x * np.random.uniform(0.4, 0.7, len(x)), 2)
}
produtos_data['custo'] = produtos_data['custo'](produtos_data['preco'])
df_produtos = pd.DataFrame(produtos_data)

# Dados de vendas - mais complexo e realista
n_vendas = 10000
vendas_data = {
    'venda_id': range(1, n_vendas + 1),
    'cliente_id': np.random.choice(df_clientes['cliente_id'], n_vendas),
    'produto_id': np.random.choice(df_produtos['produto_id'], n_vendas),
    'quantidade': np.random.choice([1, 2, 3, 4, 5], n_vendas, p=[0.6, 0.2, 0.1, 0.06, 0.04]),
    'data_venda': pd.date_range('2023-01-01', '2023-12-31', periods=n_vendas),
    'canal': np.random.choice(['Online', 'Loja Física', 'App Mobile'], n_vendas),
    'desconto_pct': np.random.choice([0, 5, 10, 15, 20], n_vendas, p=[0.4, 0.3, 0.2, 0.08, 0.02])
}
df_vendas = pd.DataFrame(vendas_data)

# Criando valor da venda baseado no preço do produto
df_vendas = df_vendas.merge(df_produtos[['produto_id', 'preco']], on='produto_id')
df_vendas['valor_unitario'] = df_vendas['preco'] * (1 - df_vendas['desconto_pct']/100)
df_vendas['valor_total'] = df_vendas['valor_unitario'] * df_vendas['quantidade']
df_vendas = df_vendas.drop(['preco'], axis=1)

print(f"Base criada com:")
print(f"- {len(df_clientes)} clientes")
print(f"- {len(df_produtos)} produtos")
print(f"- {len(df_vendas)} vendas")
print()

# =============================================================================
# EXERCÍCIO 1: ANÁLISE DE COORTE DE CLIENTES
# =============================================================================

print("EXERCÍCIO 1: ANÁLISE DE COORTE DE CLIENTES")
print("=" * 70)
print("Pergunta: Calcule a retenção de clientes por coorte mensal")
print("(clientes que voltaram a comprar nos meses seguintes)")
print()

def analise_coorte_clientes(df_vendas, df_clientes):
    """
    Calcula análise de coorte mostrando retenção de clientes
    """
    # Adicionar dados do cliente
    vendas_com_cliente = df_vendas.merge(df_clientes[['cliente_id', 'data_cadastro']], 
                                        on='cliente_id')
    
    # Primeira compra de cada cliente
    primeira_compra = vendas_com_cliente.groupby('cliente_id')['data_venda'].min().reset_index()
    primeira_compra.columns = ['cliente_id', 'primeira_compra']
    
    # Determinar coorte (mês da primeira compra)
    primeira_compra['coorte'] = primeira_compra['primeira_compra'].dt.to_period('M')
    
    # Juntar dados de volta
    vendas_coorte = vendas_com_cliente.merge(primeira_compra, on='cliente_id')
    
    # Calcular período desde primeira compra
    vendas_coorte['periodo_venda'] = vendas_coorte['data_venda'].dt.to_period('M')
    vendas_coorte['meses_desde_primeira'] = (
        vendas_coorte['periodo_venda'] - vendas_coorte['coorte']
    ).apply(attrgetter('n'))
    
    # Criar tabela de coorte
    coorte_data = vendas_coorte.groupby(['coorte', 'meses_desde_primeira'])['cliente_id'].nunique().reset_index()
    coorte_table = coorte_data.pivot(index='coorte', 
                                   columns='meses_desde_primeira', 
                                   values='cliente_id')
    
    # Calcular tamanho de cada coorte
    coorte_sizes = primeira_compra.groupby('coorte')['cliente_id'].nunique()
    
    # Calcular taxa de retenção
    retention_table = coorte_table.divide(coorte_sizes, axis=0)
    
    return retention_table

# Solução alternativa usando pandas
from operator import attrgetter

retention_rates = analise_coorte_clientes(df_vendas, df_clientes)
print("TAXA DE RETENÇÃO POR COORTE (primeiros 6 meses):")
print(retention_rates.iloc[:, :7].round(3))
print()

print("INSIGHTS:")
print("- Coorte = mês da primeira compra do cliente")
print("- Coluna 0 = todos os clientes (100%)")
print("- Coluna 1 = % que voltou no mês seguinte") 
print("- Coluna 2 = % que voltou no 2º mês, etc.")
print()

# =============================================================================
# EXERCÍCIO 2: RFM ANALYSIS (RECENCY, FREQUENCY, MONETARY)
# =============================================================================

print("EXERCÍCIO 2: RFM ANALYSIS")
print("=" * 70)
print("Pergunta: Segmente clientes usando análise RFM")
print("(Recência, Frequência e Valor Monetário)")
print()

def calcular_rfm(df_vendas, data_referencia=None):
    """
    Calcula métricas RFM para cada cliente
    """
    if data_referencia is None:
        data_referencia = df_vendas['data_venda'].max()
    
    # Agrupar por cliente
    rfm = df_vendas.groupby('cliente_id').agg({
        'data_venda': lambda x: (data_referencia - x.max()).days,  # Recency
        'venda_id': 'count',  # Frequency
        'valor_total': 'sum'  # Monetary
    }).reset_index()
    
    rfm.columns = ['cliente_id', 'recency', 'frequency', 'monetary']
    
    # Criar quintis para cada métrica (1 = pior, 5 = melhor)
    rfm['r_score'] = pd.qcut(rfm['recency'], 5, labels=[5,4,3,2,1])  # Invertido: menor recency = melhor
    rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5])
    rfm['m_score'] = pd.qcut(rfm['monetary'], 5, labels=[1,2,3,4,5])
    
    # Score RFM combinado
    rfm['rfm_score'] = rfm['r_score'].astype(str) + rfm['f_score'].astype(str) + rfm['m_score'].astype(str)
    
    return rfm

def segmentar_clientes_rfm(rfm_df):
    """
    Cria segmentos de clientes baseado em RFM
    """
    def classificar_rfm(row):
        r, f, m = int(row['r_score']), int(row['f_score']), int(row['m_score'])
        
        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        elif r >= 3 and f >= 3 and m >= 3:
            return 'Loyal Customers'
        elif r >= 4 and f <= 2:
            return 'New Customers'
        elif r >= 3 and f <= 2 and m >= 3:
            return 'Potential Loyalists'
        elif r <= 2 and f >= 3 and m >= 3:
            return 'At Risk'
        elif r <= 2 and f <= 2 and m >= 3:
            return 'Cannot Lose Them'
        elif r >= 3 and f <= 2 and m <= 2:
            return 'Promising'
        elif r <= 2 and f >= 2 and m <= 2:
            return 'Need Attention'
        else:
            return 'Lost Customers'
    
    rfm_df['segmento'] = rfm_df.apply(classificar_rfm, axis=1)
    return rfm_df

# Executar análise RFM
rfm_data = calcular_rfm(df_vendas)
rfm_segmentado = segmentar_clientes_rfm(rfm_data)

print("DISTRIBUIÇÃO DOS SEGMENTOS RFM:")
segmentos_count = rfm_segmentado['segmento'].value_counts()
print(segmentos_count)
print()

print("ESTATÍSTICAS POR SEGMENTO:")
segmento_stats = rfm_segmentado.groupby('segmento').agg({
    'recency': 'mean',
    'frequency': 'mean', 
    'monetary': 'mean',
    'cliente_id': 'count'
}).round(2)
segmento_stats.columns = ['Recency_Avg', 'Frequency_Avg', 'Monetary_Avg', 'Count']
print(segmento_stats.sort_values('Monetary_Avg', ascending=False))
print()

# =============================================================================
# EXERCÍCIO 3: ANÁLISE DE MARKET BASKET (PRODUTOS FREQUENTEMENTE COMPRADOS JUNTOS)
# =============================================================================

print("EXERCÍCIO 3: MARKET BASKET ANALYSIS")
print("=" * 70)
print("Pergunta: Encontre produtos frequentemente comprados juntos")
print("(Análise de cesta de compras)")
print()

def analise_market_basket(df_vendas, min_support=0.01):
    """
    Encontra combinações de produtos frequentemente comprados juntos
    """
    # Agrupar por cliente e data para simular "cestas"
    df_vendas['data_simples'] = df_vendas['data_venda'].dt.date
    cestas = df_vendas.groupby(['cliente_id', 'data_simples'])['produto_id'].apply(list).reset_index()
    cestas.columns = ['cliente_id', 'data', 'produtos']
    
    # Converter para formato de transações
    from collections import Counter
    
    # Contar combinações de pares de produtos
    pares_produtos = []
    produtos_individuais = Counter()
    
    for produtos in cestas['produtos']:
        if len(produtos) > 1:
            # Produtos individuais
            for produto in produtos:
                produtos_individuais[produto] += 1
            
            # Pares de produtos
            for i in range(len(produtos)):
                for j in range(i+1, len(produtos)):
                    par = tuple(sorted([produtos[i], produtos[j]]))
                    pares_produtos.append(par)
    
    # Contar pares
    contador_pares = Counter(pares_produtos)
    total_cestas = len(cestas)
    
    # Calcular métricas
    resultados = []
    for (prod1, prod2), count in contador_pares.most_common():
        if count >= total_cestas * min_support:
            support = count / total_cestas
            
            # Support individual dos produtos
            support_prod1 = produtos_individuais[prod1] / total_cestas
            support_prod2 = produtos_individuais[prod2] / total_cestas
            
            # Confidence (A -> B)
            confidence_1_2 = count / produtos_individuais[prod1]
            confidence_2_1 = count / produtos_individuais[prod2]
            
            # Lift
            lift = support / (support_prod1 * support_prod2)
            
            resultados.append({
                'produto_1': prod1,
                'produto_2': prod2,
                'support': support,
                'confidence_1_2': confidence_1_2,
                'confidence_2_1': confidence_2_1,
                'lift': lift,
                'count': count
            })
    
    return pd.DataFrame(resultados)

# Executar análise
market_basket_results = analise_market_basket(df_vendas, min_support=0.005)

if not market_basket_results.empty:
    # Adicionar nomes dos produtos
    market_basket_results = market_basket_results.merge(
        df_produtos[['produto_id', 'nome_produto']], 
        left_on='produto_1', right_on='produto_id'
    ).drop('produto_id', axis=1).rename(columns={'nome_produto': 'nome_produto_1'})
    
    market_basket_results = market_basket_results.merge(
        df_produtos[['produto_id', 'nome_produto']], 
        left_on='produto_2', right_on='produto_id'
    ).drop('produto_id', axis=1).rename(columns={'nome_produto': 'nome_produto_2'})
    
    print("TOP 10 COMBINAÇÕES DE PRODUTOS (por Lift):")
    top_combinations = market_basket_results.nlargest(10, 'lift')[
        ['nome_produto_1', 'nome_produto_2', 'support', 'confidence_1_2', 'lift']
    ].round(3)
    print(top_combinations.to_string(index=False))
else:
    print("Nenhuma combinação significativa encontrada com o suporte mínimo definido")
print()

# =============================================================================
# EXERCÍCIO 4: ANÁLISE DE SAZONALIDADE E TENDÊNCIA
# =============================================================================

print("EXERCÍCIO 4: ANÁLISE DE SAZONALIDADE")
print("=" * 70)
print("Pergunta: Identifique padrões sazonais nas vendas")
print()

def analise_sazonalidade(df_vendas):
    """
    Analisa padrões sazonais nas vendas
    """
    # Preparar dados temporais
    vendas_tempo = df_vendas.copy()
    vendas_tempo['ano'] = vendas_tempo['data_venda'].dt.year
    vendas_tempo['mes'] = vendas_tempo['data_venda'].dt.month
    vendas_tempo['dia_semana'] = vendas_tempo['data_venda'].dt.day_name()
    vendas_tempo['semana_ano'] = vendas_tempo['data_venda'].dt.isocalendar().week
    
    # Análise por mês
    vendas_por_mes = vendas_tempo.groupby('mes').agg({
        'valor_total': ['sum', 'mean', 'count'],
        'quantidade': 'sum'
    }).round(2)
    vendas_por_mes.columns = ['Faturamento_Total', 'Ticket_Medio', 'Num_Vendas', 'Qtd_Produtos']
    
    # Análise por dia da semana
    vendas_por_dia = vendas_tempo.groupby('dia_semana').agg({
        'valor_total': ['sum', 'mean', 'count']
    }).round(2)
    vendas_por_dia.columns = ['Faturamento_Total', 'Ticket_Medio', 'Num_Vendas']
    
    # Reordenar dias da semana
    ordem_dias = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    vendas_por_dia = vendas_por_dia.reindex(ordem_dias)
    
    return vendas_por_mes, vendas_por_dia, vendas_tempo

vendas_mes, vendas_dia, dados_tempo = analise_sazonalidade(df_vendas)

print("VENDAS POR MÊS:")
print(vendas_mes)
print()

print("VENDAS POR DIA DA SEMANA:")
print(vendas_dia)
print()

# Identificar padrões
mes_maior_faturamento = vendas_mes['Faturamento_Total'].idxmax()
dia_maior_faturamento = vendas_dia['Faturamento_Total'].idxmax()

print("INSIGHTS SAZONAIS:")
print(f"- Mês com maior faturamento: {mes_maior_faturamento}")
print(f"- Dia da semana com maior faturamento: {dia_maior_faturamento}")
print(f"- Variação mensal: {vendas_mes['Faturamento_Total'].std():.2f}")
print(f"- Coeficiente de variação mensal: {(vendas_mes['Faturamento_Total'].std() / vendas_mes['Faturamento_Total'].mean()):.2%}")
print()

# =============================================================================
# EXERCÍCIO 5: ANÁLISE DE CHURN DE CLIENTES
# =============================================================================

print("EXERCÍCIO 5: PREDIÇÃO DE CHURN")
print("=" * 70)
print("Pergunta: Identifique clientes com risco de churn")
print("(baseado em padrões de compra)")
print()

def calcular_metricas_churn(df_vendas, df_clientes, dias_churn=90):
    """
    Calcula métricas para identificar clientes em risco de churn
    """
    data_referencia = df_vendas['data_venda'].max()
    data_limite_churn = data_referencia - timedelta(days=dias_churn)
    
    # Métricas por cliente
    metricas_cliente = df_vendas.groupby('cliente_id').agg({
        'data_venda': ['min', 'max', 'count'],
        'valor_total': ['sum', 'mean'],
        'quantidade': 'sum'
    }).reset_index()
    
    # Flatten column names
    metricas_cliente.columns = [
        'cliente_id', 'primeira_compra', 'ultima_compra', 'total_compras',
        'valor_total_gasto', 'ticket_medio', 'quantidade_total'
    ]
    
    # Calcular features de churn
    metricas_cliente['dias_desde_ultima_compra'] = (
        data_referencia - metricas_cliente['ultima_compra']
    ).dt.days
    
    metricas_cliente['dias_como_cliente'] = (
        data_referencia - metricas_cliente['primeira_compra']
    ).dt.days
    
    metricas_cliente['frequencia_compra'] = (
        metricas_cliente['total_compras'] / metricas_cliente['dias_como_cliente'] * 30
    )  # Compras por mês
    
    # Definir churn (não comprou nos últimos X dias)
    metricas_cliente['churn'] = metricas_cliente['dias_desde_ultima_compra'] > dias_churn
    
    # Score de risco (0-100)
    metricas_cliente['risco_churn'] = np.minimum(
        100, 
        metricas_cliente['dias_desde_ultima_compra'] / dias_churn * 100
    )
    
    # Adicionar dados demográficos
    resultado = metricas_cliente.merge(
        df_clientes[['cliente_id', 'idade', 'sexo', 'cidade']], 
        on='cliente_id'
    )
    
    return resultado

# Executar análise de churn
churn_analysis = calcular_metricas_churn(df_vendas, df_clientes, dias_churn=60)

print("DISTRIBUIÇÃO DE CHURN:")
churn_dist = churn_analysis['churn'].value_counts()
print(f"Clientes ativos: {churn_dist[False]}")
print(f"Clientes em churn: {churn_dist[True]}")
print(f"Taxa de churn: {churn_dist[True] / len(churn_analysis):.2%}")
print()

print("CLIENTES COM MAIOR RISCO DE CHURN:")
alto_risco = churn_analysis[churn_analysis['risco_churn'] > 80].sort_values('risco_churn', ascending=False)
print(alto_risco[['cliente_id', 'dias_desde_ultima_compra', 'valor_total_gasto', 
                 'total_compras', 'risco_churn']].head(10).to_string(index=False))
print()

print("ESTATÍSTICAS POR GRUPO:")
stats_churn = churn_analysis.groupby('churn').agg({
    'valor_total_gasto': 'mean',
    'total_compras': 'mean',
    'frequencia_compra': 'mean',
    'dias_como_cliente': 'mean',
    'idade': 'mean'
}).round(2)
stats_churn.index = ['Ativo', 'Churn']
print(stats_churn)
print()

# =============================================================================
# EXERCÍCIO 6: ANÁLISE DE PERFORMANCE DE CANAL
# =============================================================================

print("EXERCÍCIO 6: PERFORMANCE POR CANAL DE VENDAS")
print("=" * 70)
print("Pergunta: Compare a performance dos diferentes canais")
print()

def analise_canais(df_vendas, df_produtos):
    """
    Analisa performance dos canais de venda
    """
    # Juntar com dados de produtos para margem
    vendas_completas = df_vendas.merge(df_produtos[['produto_id', 'custo']], on='produto_id')
    vendas_completas['margem'] = vendas_completas['valor_total'] - (vendas_completas['custo'] * vendas_completas['quantidade'])
    
    # Análise por canal
    perf_canal = vendas_completas.groupby('canal').agg({
        'venda_id': 'count',
        'valor_total': ['sum', 'mean'],
        'margem': ['sum', 'mean'],
        'cliente_id': 'nunique',
        'quantidade': 'sum'
    }).round(2)
    
    # Flatten columns
    perf_canal.columns = [
        'Num_Vendas', 'Faturamento_Total', 'Ticket_Medio',
        'Margem_Total', 'Margem_Media', 'Clientes_Unicos', 'Qtd_Produtos'
    ]
    
    # Calcular métricas adicionais
    perf_canal['Margem_Percentual'] = (perf_canal['Margem_Total'] / perf_canal['Faturamento_Total'] * 100).round(2)
    perf_canal['Vendas_por_Cliente'] = (perf_canal['Num_Vendas'] / perf_canal['Clientes_Unicos']).round(2)
    
    # Análise temporal por canal
    vendas_completas['mes'] = vendas_completas['data_venda'].dt.month
    trend_canal = vendas_completas.groupby(['canal', 'mes'])['valor_total'].sum().unstack(fill_value=0)
    
    return perf_canal, trend_canal

performance_canais, tendencia_canais = analise_canais(df_vendas, df_produtos)

print("PERFORMANCE GERAL POR CANAL:")
print(performance_canais.sort_values('Faturamento_Total', ascending=False))
print()

print("RANKING DOS CANAIS:")
ranking_faturam = performance_canais.sort_values('Faturamento_Total', ascending=False)
ranking_margem = performance_canais.sort_values('Margem_Percentual', ascending=False)

print("Por Faturamento:")
for i, canal in enumerate(ranking_faturam.index, 1):
    print(f"{i}. {canal}: R$ {ranking_faturam.loc[canal, 'Faturamento_Total']:,.2f}")

print("\nPor Margem %:")
for i, canal in enumerate(ranking_margem.index, 1):
    print(f"{i}. {canal}: {ranking_margem.loc[canal, 'Margem_Percentual']:.2f}%")
print()

# =============================================================================
# EXERCÍCIO 7: SQL EQUIVALENTE DOS EXERCÍCIOS (DEMONSTRAÇÃO)
# =============================================================================

print("EXERCÍCIO 7: EQUIVALENTE EM SQL")
print("=" * 70)
print("Como resolver os mesmos problemas usando SQL puro")
print()

# Criar banco SQLite em memória para demonstração
conn = sqlite3.connect(':memory:')

# Inserir dados no SQLite
df_clientes.to_sql('clientes', conn, index=False)
df_produtos.to_sql('produtos', conn, index=False)
df_vendas.to_sql('vendas', conn, index=False)

print("QUERIES SQL PARA OS EXERCÍCIOS:")
print()

# RFM em SQL
sql_rfm = """
-- RFM Analysis em SQL
WITH rfm_calc AS (
    SELECT 
        cliente_id,
        JULIANDAY(MAX(data_venda)) - JULIANDAY('2023-12-31') as recency,
        COUNT(venda_id) as frequency,
        SUM(valor_total) as monetary
    FROM vendas
    GROUP BY cliente_id
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency) as f_score,
        NTILE(5) OVER (ORDER BY monetary) as m_score
    FROM rfm_calc
)
SELECT 
    r_score,
    f_score, 
    m_score,
    COUNT(*) as num_clientes,
    AVG(monetary) as avg_monetary
FROM rfm_scores
GROUP BY r_score, f_score, m_score
ORDER BY r_score DESC, f_score DESC, m_score DESC
LIMIT 10;
"""

print("1. RFM ANALYSIS:")
print(sql_rfm)
resultado_rfm_sql = pd.read_sql_query(sql_rfm, conn)
print("Resultado (Top 10):")
print(resultado_rfm_sql)
print()

# Top produtos por canal
sql_top_produtos = """
-- Top produtos por canal
SELECT 
    v.canal,
    p.nome_produto,
    COUNT(v.venda_id) as num_vendas,
    SUM(v.valor_total) as faturamento,
    AVG(v.valor_total) as ticket_medio
FROM vendas v
JOIN produtos p ON v.produto_id = p.produto_id
GROUP BY v.canal, p.nome_produto
HAVING COUNT(v.venda_id) >= 5
ORDER BY v.canal, SUM(v.valor_total) DESC;
"""

print("2. TOP PRODUTOS POR CANAL:")
print(sql_top_produtos)
resultado_produtos_sql = pd.read_sql_query(sql_top_produtos, conn)
print("Resultado (Top 15):")
print(resultado_produtos_sql.head(15).to_string(index=False))
print()

# Fechar conexão
conn.close()

# =============================================================================
# RESUMO E DICAS PARA ENTREVISTAS
# =============================================================================

print("\nRESUMO E DICAS PARA ENTREVISTAS")
print("=" * 70)

print("✅ PROBLEMAS COMUNS EM ENTREVISTAS:")
print("1. Análise de Coorte - Retenção de usuários")
print("2. RFM Analysis - Segmentação de clientes")
print("3. Market Basket - Recomendações de produtos")
print("4. Sazonalidade - Padrões temporais")
print("5. Churn Analysis - Predição de cancelamentos")
print("6. Performance de Canais - Otimização de vendas")
print("7. Funnel Analysis - Conversão por etapas")
print("8. A/B Testing - Análise de experimentos")
print()

print("🚀 ABORDAGEM RECOMENDADA:")
print("1. Entenda o problema de negócio")
print("2. Explore os dados (shape, missing values, distributions)")
print("3. Defina métricas relevantes")
print("4. Implemente a solução (pandas/SQL)")
print("5. Interprete os resultados")
print("6. Sugira ações práticas")
print()

print("💡 DICAS DE IMPLEMENTAÇÃO:")
print("- Sempre valide seus cálculos com casos simples")
print("- Comente seu código explicando a lógica")
print("- Trate dados faltantes e outliers")
print("- Use visualizações quando apropriado")
print("- Pense em escalabilidade (big data)")
print("- Prepare-se para perguntas de follow-up")
print()

print("🔍 PERGUNTAS DE FOLLOW-UP COMUNS:")
print("- Como você escalaria isso para 100M+ registros?")
print("- Como trataria dados em tempo real?")
print("- Quais são as limitações da sua análise?")
print("- Como validaria se os resultados fazem sentido?")
print("- Que outras análises você faria?")
print()

print("=== FIM DOS EXERCÍCIOS PRÁTICOS ===")
print("Pratique estes cenários e adapte para diferentes contextos de negócio!")
print("Lembre-se: o importante é o raciocínio, não apenas o código!")