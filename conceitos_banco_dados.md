# Conceitos Fundamentais de Banco de Dados para Entrevistas

## Índice
1. [Índices](#índices)
2. [Views](#views)
3. [Stored Procedures](#stored-procedures)
4. [Schemas](#schemas)
5. [Triggers](#triggers)
6. [Transactions e ACID](#transactions-e-acid)
7. [Normalização](#normalização)
8. [Performance e Otimização](#performance-e-otimização)

---

## Índices

### O que são Índices?
Índices são estruturas de dados que melhoram a velocidade de operações de consulta em uma tabela, criando um "atalho" para encontrar dados específicos.

### Tipos de Índices

#### 1. Índice Primário (Primary Index)
```sql
-- Criado automaticamente com PRIMARY KEY
CREATE TABLE usuarios (
    id INT PRIMARY KEY,  -- Índice primário automático
    nome VARCHAR(100),
    email VARCHAR(100)
);
```

#### 2. Índice Único (Unique Index)
```sql
-- Garante unicidade e melhora performance
CREATE UNIQUE INDEX idx_email_unico ON usuarios(email);

-- Ou durante criação da tabela
CREATE TABLE usuarios (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    email VARCHAR(100) UNIQUE  -- Índice único automático
);
```

#### 3. Índice Composto (Composite Index)
```sql
-- Índice em múltiplas colunas
CREATE INDEX idx_nome_data ON vendas(vendedor_id, data_venda);

-- A ordem das colunas importa!
-- Eficiente para: WHERE vendedor_id = X AND data_venda = Y
-- Eficiente para: WHERE vendedor_id = X
-- NÃO eficiente para: WHERE data_venda = Y (apenas)
```

#### 4. Índice Parcial
```sql
-- Índice apenas em registros que atendem uma condição
CREATE INDEX idx_vendas_ativas 
ON vendas(data_venda) 
WHERE status = 'ativa';
```

### Quando usar Índices?

#### ✅ Use índices quando:
- Colunas frequentemente usadas em WHERE, JOIN, ORDER BY
- Colunas de chaves estrangeiras
- Consultas que retornam poucos registros
- Tabelas com muitas operações de leitura

#### ❌ Evite índices quando:
- Tabelas pequenas (< 1000 registros)
- Colunas com poucos valores distintos
- Tabelas com muitas operações INSERT/UPDATE/DELETE
- Colunas raramente consultadas

### Exemplo Prático: Análise de Performance
```sql
-- Sem índice - consulta lenta
EXPLAIN ANALYZE 
SELECT * FROM vendas WHERE vendedor_id = 123;
-- Resultado: Seq Scan (varredura completa)

-- Criando índice
CREATE INDEX idx_vendedor ON vendas(vendedor_id);

-- Com índice - consulta rápida
EXPLAIN ANALYZE 
SELECT * FROM vendas WHERE vendedor_id = 123;
-- Resultado: Index Scan (busca direta)
```

---

## Views

### O que são Views?
Views são "tabelas virtuais" baseadas em queries SQL. Não armazenam dados fisicamente, mas apresentam dados de uma ou mais tabelas.

### Tipos de Views

#### 1. View Simples
```sql
CREATE VIEW vendas_resumo AS
SELECT 
    vendedor_id,
    COUNT(*) as total_vendas,
    SUM(valor) as faturamento_total,
    AVG(valor) as ticket_medio
FROM vendas 
GROUP BY vendedor_id;

-- Uso da view
SELECT * FROM vendas_resumo WHERE faturamento_total > 10000;
```

#### 2. View com Joins
```sql
CREATE VIEW vendas_detalhadas AS
SELECT 
    v.id,
    v.data_venda,
    v.valor,
    p.nome as produto,
    c.nome as categoria,
    vend.nome as vendedor
FROM vendas v
JOIN produtos p ON v.produto_id = p.id
JOIN categorias c ON p.categoria_id = c.id
JOIN vendedores vend ON v.vendedor_id = vend.id;
```

#### 3. View Materializada (PostgreSQL, Oracle)
```sql
-- View que armazena fisicamente os resultados
CREATE MATERIALIZED VIEW relatorio_mensal AS
SELECT 
    EXTRACT(YEAR FROM data_venda) as ano,
    EXTRACT(MONTH FROM data_venda) as mes,
    categoria,
    SUM(valor) as faturamento,
    COUNT(*) as vendas
FROM vendas_detalhadas
GROUP BY ano, mes, categoria;

-- Atualizar dados da view materializada
REFRESH MATERIALIZED VIEW relatorio_mensal;
```

### Quando usar Views?

#### ✅ Use views para:
- **Segurança**: Limitar acesso a colunas específicas
- **Simplicidade**: Simplificar queries complexas frequentes
- **Abstração**: Ocultar complexidade do schema
- **Reutilização**: Padronizar consultas entre aplicações

#### ❌ Evite views quando:
- Performance é crítica (views complexas podem ser lentas)
- Dados mudam frequentemente (materialized views)
- Lógica simples que não justifica abstração

### View vs Tabela: Quando escolher cada uma?

| Critério | View | Tabela |
|----------|------|--------|
| **Armazenamento** | Não ocupa espaço | Ocupa espaço físico |
| **Performance** | Pode ser mais lenta | Mais rápida |
| **Atualização** | Dados sempre atuais | Precisa ser atualizada |
| **Flexibilidade** | Fácil de modificar | Difícil de modificar |
| **Segurança** | Controle granular | Controle por tabela |

**Exemplo prático:**
```sql
-- Use VIEW para relatórios dinâmicos
CREATE VIEW dashboard_vendas AS
SELECT 
    DATE(data_venda) as data,
    SUM(valor) as faturamento_dia,
    COUNT(*) as vendas_dia
FROM vendas 
WHERE data_venda >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(data_venda);

-- Use TABELA para dados históricos consolidados
CREATE TABLE relatorio_historico_mensal (
    ano INT,
    mes INT,
    faturamento DECIMAL(15,2),
    vendas INT,
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Stored Procedures

### O que são Stored Procedures?
Stored Procedures são blocos de código SQL armazenados no banco de dados que podem ser executados quando necessário.

### Exemplo Básico
```sql
-- PostgreSQL / SQL Server
CREATE OR REPLACE PROCEDURE calcular_comissao(
    IN vendedor_id INT,
    IN percentual DECIMAL(5,2),
    OUT comissao_total DECIMAL(10,2)
)
LANGUAGE plpgsql
AS $$
BEGIN
    SELECT SUM(valor) * (percentual / 100)
    INTO comissao_total
    FROM vendas 
    WHERE vendedor_id = vendedor_id
    AND EXTRACT(MONTH FROM data_venda) = EXTRACT(MONTH FROM CURRENT_DATE);
END;
$$;

-- Executando a procedure
CALL calcular_comissao(123, 5.5, @comissao);
SELECT @comissao;
```

### Exemplo Complexo: Procedure de Processamento de Vendas
```sql
CREATE OR REPLACE PROCEDURE processar_venda(
    IN p_produto_id INT,
    IN p_vendedor_id INT,
    IN p_quantidade INT,
    IN p_desconto DECIMAL(5,2) DEFAULT 0,
    OUT p_venda_id INT,
    OUT p_valor_total DECIMAL(10,2)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_preco DECIMAL(10,2);
    v_estoque INT;
BEGIN
    -- Verificar estoque
    SELECT estoque, preco 
    INTO v_estoque, v_preco
    FROM produtos 
    WHERE id = p_produto_id;
    
    IF v_estoque < p_quantidade THEN
        RAISE EXCEPTION 'Estoque insuficiente. Disponível: %', v_estoque;
    END IF;
    
    -- Calcular valor total
    p_valor_total := v_preco * p_quantidade * (1 - p_desconto/100);
    
    -- Inserir venda
    INSERT INTO vendas (produto_id, vendedor_id, quantidade, valor, data_venda)
    VALUES (p_produto_id, p_vendedor_id, p_quantidade, p_valor_total, CURRENT_DATE)
    RETURNING id INTO p_venda_id;
    
    -- Atualizar estoque
    UPDATE produtos 
    SET estoque = estoque - p_quantidade 
    WHERE id = p_produto_id;
    
    -- Log da operação
    INSERT INTO log_vendas (venda_id, acao, data_acao)
    VALUES (p_venda_id, 'VENDA_PROCESSADA', CURRENT_TIMESTAMP);
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
$$;
```

### Quando usar Stored Procedures?

#### ✅ Use quando:
- Lógica de negócio complexa
- Operações que envolvem múltiplas tabelas
- Necessita de controle transacional
- Performance crítica (processamento no servidor)
- Reutilização de código entre aplicações

#### ❌ Evite quando:
- Lógica simples (melhor na aplicação)
- Equipe não tem conhecimento em SQL avançado
- Necessita de versionamento complexo
- Portabilidade entre SGBDs é importante

---

## Schemas

### O que são Schemas?
Schemas são namespaces que organizam objetos de banco de dados (tabelas, views, procedures, etc.) logicamente.

### Estrutura Típica por Schema
```sql
-- Schema para vendas
CREATE SCHEMA vendas;

CREATE TABLE vendas.produtos (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    preco DECIMAL(10,2)
);

CREATE TABLE vendas.pedidos (
    id INT PRIMARY KEY,
    produto_id INT REFERENCES vendas.produtos(id),
    data_pedido DATE
);

-- Schema para recursos humanos
CREATE SCHEMA rh;

CREATE TABLE rh.funcionarios (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    cargo VARCHAR(50),
    salario DECIMAL(10,2)
);

-- Schema para auditoria
CREATE SCHEMA auditoria;

CREATE TABLE auditoria.log_acesso (
    id INT PRIMARY KEY,
    usuario VARCHAR(50),
    acao VARCHAR(100),
    timestamp TIMESTAMP
);
```

### Organização por Ambiente
```sql
-- Separação por ambiente
CREATE SCHEMA dev;
CREATE SCHEMA teste;
CREATE SCHEMA producao;

-- Cada schema tem suas próprias tabelas
CREATE TABLE dev.vendas (...);
CREATE TABLE teste.vendas (...);
CREATE TABLE producao.vendas (...);
```

### Benefícios dos Schemas

1. **Organização**: Separa logicamente diferentes áreas do negócio
2. **Segurança**: Controle de acesso granular por schema
3. **Namespace**: Evita conflitos de nomes
4. **Manutenção**: Facilita backup/restore seletivo
5. **Multi-tenancy**: Separação de dados por cliente

### Exemplo Prático: E-commerce
```sql
-- Schema para catálogo de produtos
CREATE SCHEMA catalogo;
CREATE TABLE catalogo.produtos (...);
CREATE TABLE catalogo.categorias (...);

-- Schema para pedidos
CREATE SCHEMA pedidos;
CREATE TABLE pedidos.carrinho (...);
CREATE TABLE pedidos.checkout (...);

-- Schema para usuários
CREATE SCHEMA usuarios;
CREATE TABLE usuarios.clientes (...);
CREATE TABLE usuarios.enderecos (...);

-- Schema para relatórios
CREATE SCHEMA relatorios;
CREATE VIEW relatorios.vendas_por_categoria AS ...;
CREATE VIEW relatorios.clientes_ativos AS ...;
```

---

## Triggers

### O que são Triggers?
Triggers são procedimentos que executam automaticamente em resposta a eventos específicos no banco de dados.

### Tipos de Triggers

#### 1. BEFORE Trigger
```sql
-- Trigger que executa ANTES da inserção
CREATE OR REPLACE FUNCTION atualizar_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.data_atualizacao = CURRENT_TIMESTAMP;
    NEW.usuario_atualizacao = current_user;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_atualizar_timestamp
    BEFORE UPDATE ON produtos
    FOR EACH ROW
    EXECUTE FUNCTION atualizar_timestamp();
```

#### 2. AFTER Trigger
```sql
-- Trigger para auditoria APÓS modificações
CREATE OR REPLACE FUNCTION auditoria_vendas()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO auditoria.log_vendas (venda_id, acao, data_acao)
        VALUES (NEW.id, 'INSERT', CURRENT_TIMESTAMP);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO auditoria.log_vendas (venda_id, acao, data_acao, valor_anterior, valor_novo)
        VALUES (NEW.id, 'UPDATE', CURRENT_TIMESTAMP, OLD.valor, NEW.valor);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO auditoria.log_vendas (venda_id, acao, data_acao)
        VALUES (OLD.id, 'DELETE', CURRENT_TIMESTAMP);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_auditoria_vendas
    AFTER INSERT OR UPDATE OR DELETE ON vendas
    FOR EACH ROW
    EXECUTE FUNCTION auditoria_vendas();
```

### Casos de Uso Comuns

1. **Auditoria**: Log automático de mudanças
2. **Validação**: Regras de negócio complexas
3. **Sincronização**: Atualizar tabelas relacionadas
4. **Notificações**: Alertas automáticos
5. **Cálculos**: Campos calculados automaticamente

---

## Transactions e ACID

### Propriedades ACID

#### **A**tomicity (Atomicidade)
```sql
BEGIN TRANSACTION;
    INSERT INTO contas (id, saldo) VALUES (1, 1000);
    INSERT INTO contas (id, saldo) VALUES (2, 500);
    -- Se qualquer operação falhar, todas são desfeitas
COMMIT;
```

#### **C**onsistency (Consistência)
```sql
-- Transferência bancária mantém consistência
BEGIN TRANSACTION;
    UPDATE contas SET saldo = saldo - 100 WHERE id = 1;
    UPDATE contas SET saldo = saldo + 100 WHERE id = 2;
    
    -- Verificação de consistência
    IF (SELECT saldo FROM contas WHERE id = 1) < 0 THEN
        ROLLBACK;
    ELSE
        COMMIT;
    END IF;
```

#### **I**solation (Isolamento)
```sql
-- Diferentes níveis de isolamento
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

#### **D**urability (Durabilidade)
```sql
-- Após COMMIT, dados são permanentes mesmo com falha do sistema
BEGIN TRANSACTION;
    INSERT INTO vendas (...) VALUES (...);
COMMIT;  -- Dados são persistidos permanentemente
```

---

## Normalização

### Formas Normais

#### 1ª Forma Normal (1NF)
```sql
-- ❌ Não normalizado
CREATE TABLE pedidos_ruim (
    id INT,
    cliente VARCHAR(100),
    produtos TEXT  -- "Produto1,Produto2,Produto3"
);

-- ✅ 1NF: Valores atômicos
CREATE TABLE pedidos (
    id INT PRIMARY KEY,
    cliente VARCHAR(100)
);

CREATE TABLE itens_pedido (
    pedido_id INT REFERENCES pedidos(id),
    produto VARCHAR(100)
);
```

#### 2ª Forma Normal (2NF)
```sql
-- ❌ Dependência parcial
CREATE TABLE vendas_ruim (
    produto_id INT,
    vendedor_id INT,
    nome_produto VARCHAR(100),  -- Depende apenas de produto_id
    nome_vendedor VARCHAR(100), -- Depende apenas de vendedor_id
    quantidade INT,
    PRIMARY KEY (produto_id, vendedor_id)
);

-- ✅ 2NF: Sem dependências parciais
CREATE TABLE produtos (
    id INT PRIMARY KEY,
    nome VARCHAR(100)
);

CREATE TABLE vendedores (
    id INT PRIMARY KEY,
    nome VARCHAR(100)
);

CREATE TABLE vendas (
    produto_id INT REFERENCES produtos(id),
    vendedor_id INT REFERENCES vendedores(id),
    quantidade INT,
    PRIMARY KEY (produto_id, vendedor_id)
);
```

#### 3ª Forma Normal (3NF)
```sql
-- ❌ Dependência transitiva
CREATE TABLE funcionarios_ruim (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    departamento_id INT,
    nome_departamento VARCHAR(100),  -- Depende de departamento_id
    gerente_departamento VARCHAR(100) -- Depende de departamento_id
);

-- ✅ 3NF: Sem dependências transitivas
CREATE TABLE departamentos (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    gerente VARCHAR(100)
);

CREATE TABLE funcionarios (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    departamento_id INT REFERENCES departamentos(id)
);
```

---

## Performance e Otimização

### Ferramentas de Análise

#### EXPLAIN PLAN
```sql
-- Analisar plano de execução
EXPLAIN (ANALYZE, BUFFERS) 
SELECT v.*, p.nome 
FROM vendas v 
JOIN produtos p ON v.produto_id = p.id 
WHERE v.data_venda >= '2024-01-01';

-- Resultado exemplo:
-- Hash Join  (cost=4.64..15.23 rows=100 width=68) (actual time=0.123..0.456 rows=150 loops=1)
-- -> Seq Scan on produtos p  (cost=0.00..1.15 rows=15 width=36)
-- -> Hash  (cost=3.50..3.50 rows=100 width=40)
--    -> Index Scan on vendas v  (cost=0.29..3.50 rows=100 width=40)
```

### Técnicas de Otimização

#### 1. Índices Estratégicos
```sql
-- Para consultas com WHERE
CREATE INDEX idx_vendas_data ON vendas(data_venda);

-- Para JOINs
CREATE INDEX idx_vendas_produto ON vendas(produto_id);

-- Para ORDER BY
CREATE INDEX idx_vendas_valor ON vendas(valor DESC);

-- Índice composto para consultas específicas
CREATE INDEX idx_vendas_complexo ON vendas(vendedor_id, data_venda, status)
WHERE valor > 100;
```

#### 2. Particionamento
```sql
-- Particionamento por data (PostgreSQL)
CREATE TABLE vendas_particionada (
    id BIGSERIAL,
    produto_id INT,
    vendedor_id INT,
    valor DECIMAL(10,2),
    data_venda DATE
) PARTITION BY RANGE (data_venda);

-- Criar partições
CREATE TABLE vendas_2024_q1 PARTITION OF vendas_particionada
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE vendas_2024_q2 PARTITION OF vendas_particionada
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
```

#### 3. Estatísticas e Manutenção
```sql
-- Atualizar estatísticas (PostgreSQL)
ANALYZE vendas;

-- Reorganizar índices
REINDEX TABLE vendas;

-- Limpar espaço não utilizado
VACUUM FULL vendas;
```

### Checklist de Performance

1. **Índices**
   - [ ] Colunas em WHERE, JOIN, ORDER BY têm índices?
   - [ ] Índices compostos estão na ordem correta?
   - [ ] Existem índices não utilizados?

2. **Queries**
   - [ ] SELECT apenas colunas necessárias?
   - [ ] WHERE filtra o máximo possível?
   - [ ] JOINs são eficientes?
   - [ ] Subqueries podem ser otimizadas?

3. **Estrutura**
   - [ ] Normalização adequada?
   - [ ] Tipos de dados apropriados?
   - [ ] Particionamento se necessário?

4. **Manutenção**
   - [ ] Estatísticas atualizadas?
   - [ ] Limpeza regular (VACUUM)?
   - [ ] Monitoramento de performance?

---

## Resumo: Quando Usar Cada Conceito

| Conceito | Use quando | Evite quando |
|----------|------------|--------------|
| **Índices** | Consultas frequentes, tabelas grandes | Tabelas pequenas, muitos INSERTs |
| **Views** | Simplificar queries, segurança | Performance crítica, lógica simples |
| **Stored Procedures** | Lógica complexa, performance | Portabilidade, versionamento simples |
| **Schemas** | Organização, multi-tenancy | Projetos pequenos, overhead desnecessário |
| **Triggers** | Auditoria, validações automáticas | Lógica de negócio complexa |
| **Normalização** | Consistência, flexibilidade | Performance de leitura crítica |

Este guia fornece uma base sólida para entrevistas técnicas, cobrindo não apenas o "como" mas principalmente o "quando" e "por que" usar cada conceito.