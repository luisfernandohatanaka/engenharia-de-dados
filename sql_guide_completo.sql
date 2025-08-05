-- =============================================================================
-- GUIA COMPLETO DE SQL PARA ENTREVISTAS
-- Conceitos essenciais: Agregações, Window Functions, CTEs e Joins
-- =============================================================================

-- =============================================================================
-- 1. AGREGAÇÕES
-- =============================================================================

-- Dados de exemplo para demonstrações
CREATE TABLE vendas (
    id INT PRIMARY KEY,
    vendedor_id INT,
    produto VARCHAR(50),
    categoria VARCHAR(30),
    valor DECIMAL(10,2),
    quantidade INT,
    data_venda DATE,
    regiao VARCHAR(20)
);

INSERT INTO vendas VALUES
(1, 101, 'Notebook', 'Eletrônicos', 2500.00, 1, '2024-01-15', 'Sul'),
(2, 102, 'Mouse', 'Eletrônicos', 50.00, 3, '2024-01-16', 'Norte'),
(3, 101, 'Teclado', 'Eletrônicos', 150.00, 2, '2024-01-17', 'Sul'),
(4, 103, 'Mesa', 'Móveis', 800.00, 1, '2024-01-18', 'Centro'),
(5, 102, 'Cadeira', 'Móveis', 400.00, 2, '2024-01-19', 'Norte'),
(6, 101, 'Monitor', 'Eletrônicos', 1200.00, 1, '2024-01-20', 'Sul');

-- Agregações básicas
SELECT 
    COUNT(*) as total_vendas,
    SUM(valor * quantidade) as faturamento_total,
    AVG(valor) as valor_medio,
    MIN(valor) as menor_valor,
    MAX(valor) as maior_valor
FROM vendas;

-- Agregações com GROUP BY
SELECT 
    categoria,
    COUNT(*) as qtd_vendas,
    SUM(valor * quantidade) as faturamento,
    AVG(valor) as valor_medio
FROM vendas 
GROUP BY categoria
ORDER BY faturamento DESC;

-- Agregações com HAVING (filtro após agregação)
SELECT 
    vendedor_id,
    COUNT(*) as qtd_vendas,
    SUM(valor * quantidade) as faturamento_total
FROM vendas 
GROUP BY vendedor_id
HAVING COUNT(*) >= 2  -- Apenas vendedores com 2+ vendas
ORDER BY faturamento_total DESC;

-- Agregações por múltiplas colunas
SELECT 
    categoria,
    regiao,
    COUNT(*) as vendas,
    SUM(valor * quantidade) as faturamento,
    ROUND(AVG(valor), 2) as valor_medio
FROM vendas 
GROUP BY categoria, regiao
ORDER BY categoria, faturamento DESC;

-- =============================================================================
-- 2. WINDOW FUNCTIONS (FUNÇÕES DE JANELA)
-- =============================================================================

-- ROW_NUMBER: Numera linhas sequencialmente
SELECT 
    produto,
    valor,
    categoria,
    ROW_NUMBER() OVER (ORDER BY valor DESC) as ranking_geral,
    ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY valor DESC) as ranking_categoria
FROM vendas;

-- RANK e DENSE_RANK: Rankings com empates
SELECT 
    produto,
    valor,
    categoria,
    RANK() OVER (ORDER BY valor DESC) as rank_com_gaps,
    DENSE_RANK() OVER (ORDER BY valor DESC) as rank_sem_gaps
FROM vendas;

-- LAG e LEAD: Acessar valores de linhas anteriores/posteriores
SELECT 
    produto,
    data_venda,
    valor,
    LAG(valor, 1) OVER (ORDER BY data_venda) as valor_venda_anterior,
    LEAD(valor, 1) OVER (ORDER BY data_venda) as valor_proxima_venda,
    valor - LAG(valor, 1) OVER (ORDER BY data_venda) as diferenca_anterior
FROM vendas
ORDER BY data_venda;

-- SUM com WINDOW: Totais acumulados
SELECT 
    data_venda,
    produto,
    valor,
    SUM(valor) OVER (ORDER BY data_venda) as faturamento_acumulado,
    SUM(valor) OVER (PARTITION BY EXTRACT(MONTH FROM data_venda) ORDER BY data_venda) as acumulado_mensal
FROM vendas
ORDER BY data_venda;

-- PERCENTILE e distribuições
SELECT 
    produto,
    valor,
    categoria,
    PERCENT_RANK() OVER (ORDER BY valor) as percentual_rank,
    NTILE(4) OVER (ORDER BY valor) as quartil,
    CUME_DIST() OVER (ORDER BY valor) as distribuicao_cumulativa
FROM vendas;

-- =============================================================================
-- 3. CTEs (Common Table Expressions)
-- =============================================================================

-- CTE simples: Resumo por vendedor
WITH resumo_vendedor AS (
    SELECT 
        vendedor_id,
        COUNT(*) as qtd_vendas,
        SUM(valor * quantidade) as faturamento_total,
        AVG(valor) as valor_medio
    FROM vendas 
    GROUP BY vendedor_id
)
SELECT 
    vendedor_id,
    qtd_vendas,
    faturamento_total,
    CASE 
        WHEN faturamento_total > 3000 THEN 'Alto'
        WHEN faturamento_total > 1000 THEN 'Médio'
        ELSE 'Baixo'
    END as nivel_performance
FROM resumo_vendedor
ORDER BY faturamento_total DESC;

-- CTEs múltiplas e encadeadas
WITH vendas_por_categoria AS (
    SELECT 
        categoria,
        SUM(valor * quantidade) as faturamento,
        COUNT(*) as qtd_vendas
    FROM vendas 
    GROUP BY categoria
),
total_geral AS (
    SELECT SUM(faturamento) as faturamento_total_empresa
    FROM vendas_por_categoria
)
SELECT 
    vpc.categoria,
    vpc.faturamento,
    vpc.qtd_vendas,
    ROUND((vpc.faturamento * 100.0 / tg.faturamento_total_empresa), 2) as percentual_faturamento
FROM vendas_por_categoria vpc
CROSS JOIN total_geral tg
ORDER BY vpc.faturamento DESC;

-- CTE recursiva: Hierarquia organizacional
CREATE TABLE funcionarios (
    id INT PRIMARY KEY,
    nome VARCHAR(50),
    gerente_id INT,
    cargo VARCHAR(30)
);

INSERT INTO funcionarios VALUES
(1, 'João Silva', NULL, 'CEO'),
(2, 'Maria Santos', 1, 'Diretora Vendas'),
(3, 'Pedro Costa', 1, 'Diretor TI'),
(4, 'Ana Lima', 2, 'Gerente Vendas'),
(5, 'Carlos Souza', 2, 'Gerente Vendas'),
(6, 'Paula Rocha', 4, 'Vendedor');

WITH hierarquia AS (
    -- Caso base: CEO (sem gerente)
    SELECT 
        id, 
        nome, 
        cargo, 
        gerente_id, 
        0 as nivel,
        CAST(nome AS VARCHAR(500)) as caminho_hierarquia
    FROM funcionarios 
    WHERE gerente_id IS NULL
    
    UNION ALL
    
    -- Caso recursivo: funcionários com gerente
    SELECT 
        f.id,
        f.nome,
        f.cargo,
        f.gerente_id,
        h.nivel + 1,
        CAST(h.caminho_hierarquia || ' -> ' || f.nome AS VARCHAR(500))
    FROM funcionarios f
    INNER JOIN hierarquia h ON f.gerente_id = h.id
)
SELECT 
    REPEAT('  ', nivel) || nome as hierarquia_visual,
    cargo,
    nivel,
    caminho_hierarquia
FROM hierarquia
ORDER BY nivel, nome;

-- =============================================================================
-- 4. JOINS (MÚLTIPLAS COLUNAS E CENÁRIOS COMPLEXOS)
-- =============================================================================

-- Criando tabelas para demonstrar joins complexos
CREATE TABLE produtos (
    produto_id INT,
    categoria_id INT,
    nome VARCHAR(50),
    preco_unitario DECIMAL(10,2),
    PRIMARY KEY (produto_id, categoria_id)  -- Chave composta
);

CREATE TABLE categorias (
    categoria_id INT PRIMARY KEY,
    nome_categoria VARCHAR(30),
    margem_lucro DECIMAL(5,2)
);

CREATE TABLE vendas_detalhadas (
    venda_id INT PRIMARY KEY,
    produto_id INT,
    categoria_id INT,
    vendedor_id INT,
    quantidade INT,
    data_venda DATE,
    desconto DECIMAL(5,2)
);

-- Dados de exemplo
INSERT INTO categorias VALUES
(1, 'Eletrônicos', 0.25),
(2, 'Móveis', 0.35),
(3, 'Roupas', 0.50);

INSERT INTO produtos VALUES
(101, 1, 'Notebook Dell', 2500.00),
(102, 1, 'Mouse Logitech', 80.00),
(103, 2, 'Mesa Escritório', 800.00),
(104, 2, 'Cadeira Ergonômica', 600.00),
(105, 3, 'Camisa Polo', 120.00);

INSERT INTO vendas_detalhadas VALUES
(1, 101, 1, 201, 2, '2024-01-15', 0.10),
(2, 102, 1, 202, 5, '2024-01-16', 0.05),
(3, 103, 2, 201, 1, '2024-01-17', 0.00),
(4, 105, 3, 203, 3, '2024-01-18', 0.15);

-- INNER JOIN com chave composta
SELECT 
    vd.venda_id,
    p.nome as produto,
    c.nome_categoria,
    vd.quantidade,
    p.preco_unitario,
    (p.preco_unitario * vd.quantidade * (1 - vd.desconto)) as valor_total,
    (p.preco_unitario * vd.quantidade * (1 - vd.desconto) * c.margem_lucro) as lucro_estimado
FROM vendas_detalhadas vd
INNER JOIN produtos p ON vd.produto_id = p.produto_id AND vd.categoria_id = p.categoria_id
INNER JOIN categorias c ON p.categoria_id = c.categoria_id;

-- LEFT JOIN para encontrar produtos sem vendas
SELECT 
    p.nome as produto,
    c.nome_categoria,
    p.preco_unitario,
    COALESCE(SUM(vd.quantidade), 0) as total_vendido,
    CASE 
        WHEN SUM(vd.quantidade) IS NULL THEN 'Sem vendas'
        WHEN SUM(vd.quantidade) < 3 THEN 'Baixa demanda'
        ELSE 'Boa demanda'
    END as status_produto
FROM produtos p
INNER JOIN categorias c ON p.categoria_id = c.categoria_id
LEFT JOIN vendas_detalhadas vd ON p.produto_id = vd.produto_id AND p.categoria_id = vd.categoria_id
GROUP BY p.produto_id, p.categoria_id, p.nome, c.nome_categoria, p.preco_unitario
ORDER BY total_vendido DESC;

-- FULL OUTER JOIN simulado (nem todos SGBDs suportam)
-- Usando UNION para simular FULL OUTER JOIN
SELECT 
    COALESCE(p.nome, 'Produto Inexistente') as produto,
    COALESCE(vd.venda_id, 0) as venda_id,
    'Produto com venda' as tipo
FROM produtos p
LEFT JOIN vendas_detalhadas vd ON p.produto_id = vd.produto_id AND p.categoria_id = vd.categoria_id

UNION

SELECT 
    'Produto Removido' as produto,
    vd.venda_id,
    'Venda órfã' as tipo
FROM vendas_detalhadas vd
LEFT JOIN produtos p ON vd.produto_id = p.produto_id AND vd.categoria_id = p.categoria_id
WHERE p.produto_id IS NULL;

-- Self JOIN: Comparando vendedores
CREATE TABLE performance_vendedor AS
SELECT 
    vendedor_id,
    SUM(p.preco_unitario * vd.quantidade * (1 - vd.desconto)) as faturamento_total
FROM vendas_detalhadas vd
INNER JOIN produtos p ON vd.produto_id = p.produto_id AND vd.categoria_id = p.categoria_id
GROUP BY vendedor_id;

SELECT 
    v1.vendedor_id as vendedor_1,
    v1.faturamento_total as faturamento_1,
    v2.vendedor_id as vendedor_2,
    v2.faturamento_total as faturamento_2,
    (v1.faturamento_total - v2.faturamento_total) as diferenca
FROM performance_vendedor v1
INNER JOIN performance_vendedor v2 ON v1.vendedor_id < v2.vendedor_id  -- Evita duplicatas
ORDER BY diferenca DESC;

-- =============================================================================
-- EXERCÍCIOS PRÁTICOS PARA ENTREVISTAS
-- =============================================================================

/*
EXERCÍCIO 1: Top 3 produtos por categoria com window function
Encontre os 3 produtos mais vendidos (por quantidade) em cada categoria
*/

WITH vendas_produto AS (
    SELECT 
        p.nome as produto,
        c.nome_categoria,
        SUM(vd.quantidade) as total_vendido,
        RANK() OVER (PARTITION BY c.nome_categoria ORDER BY SUM(vd.quantidade) DESC) as ranking
    FROM vendas_detalhadas vd
    INNER JOIN produtos p ON vd.produto_id = p.produto_id AND vd.categoria_id = p.categoria_id
    INNER JOIN categorias c ON p.categoria_id = c.categoria_id
    GROUP BY p.produto_id, p.categoria_id, p.nome, c.nome_categoria
)
SELECT produto, nome_categoria, total_vendido, ranking
FROM vendas_produto
WHERE ranking <= 3
ORDER BY nome_categoria, ranking;

/*
EXERCÍCIO 2: Análise de crescimento mensal
Compare faturamento mês a mês usando LAG
*/

WITH faturamento_mensal AS (
    SELECT 
        EXTRACT(YEAR FROM vd.data_venda) as ano,
        EXTRACT(MONTH FROM vd.data_venda) as mes,
        SUM(p.preco_unitario * vd.quantidade * (1 - vd.desconto)) as faturamento
    FROM vendas_detalhadas vd
    INNER JOIN produtos p ON vd.produto_id = p.produto_id AND vd.categoria_id = p.categoria_id
    GROUP BY EXTRACT(YEAR FROM vd.data_venda), EXTRACT(MONTH FROM vd.data_venda)
)
SELECT 
    ano,
    mes,
    faturamento,
    LAG(faturamento, 1) OVER (ORDER BY ano, mes) as faturamento_mes_anterior,
    ROUND(
        ((faturamento - LAG(faturamento, 1) OVER (ORDER BY ano, mes)) * 100.0 / 
         LAG(faturamento, 1) OVER (ORDER BY ano, mes)), 2
    ) as crescimento_percentual
FROM faturamento_mensal
ORDER BY ano, mes;

/*
EXERCÍCIO 3: Análise de coorte simples
Agrupe clientes por mês de primeira compra
*/

WITH primeira_compra AS (
    SELECT 
        vendedor_id,
        MIN(data_venda) as primeira_venda,
        EXTRACT(YEAR FROM MIN(data_venda)) as ano_primeira_venda,
        EXTRACT(MONTH FROM MIN(data_venda)) as mes_primeira_venda
    FROM vendas_detalhadas
    GROUP BY vendedor_id
),
vendas_com_coorte AS (
    SELECT 
        vd.*,
        pc.primeira_venda,
        pc.ano_primeira_venda,
        pc.mes_primeira_venda,
        EXTRACT(MONTH FROM AGE(vd.data_venda, pc.primeira_venda)) as meses_desde_primeira_venda
    FROM vendas_detalhadas vd
    INNER JOIN primeira_compra pc ON vd.vendedor_id = pc.vendedor_id
)
SELECT 
    ano_primeira_venda,
    mes_primeira_venda,
    meses_desde_primeira_venda,
    COUNT(DISTINCT vendedor_id) as vendedores_ativos,
    COUNT(*) as total_vendas
FROM vendas_com_coorte
GROUP BY ano_primeira_venda, mes_primeira_venda, meses_desde_primeira_venda
ORDER BY ano_primeira_venda, mes_primeira_venda, meses_desde_primeira_venda;