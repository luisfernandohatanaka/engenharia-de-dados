"""
=============================================================================
FUNDAMENTOS PYTHON PARA ENTREVISTAS
Estruturas de dados, loops, POO e conceitos de engenharia de software
=============================================================================
"""

import sys
from collections import defaultdict, Counter, deque
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Union, Tuple
import itertools
from functools import reduce, wraps
from datetime import datetime

print("=== FUNDAMENTOS PYTHON PARA ENTREVISTAS ===\n")

# =============================================================================
# 1. ESTRUTURAS DE DADOS BÁSICAS
# =============================================================================

print("1. ESTRUTURAS DE DADOS BÁSICAS")
print("=" * 50)

# Listas - Mutáveis, ordenadas, permitem duplicatas
print("LISTAS:")
vendas = [2500, 1200, 800, 1500, 900]
produtos = ['Notebook', 'Monitor', 'Tablet', 'Headset', 'Mouse']

print(f"Vendas: {vendas}")
print(f"Produtos: {produtos}")

# Operações importantes em listas
print("\nOperações em listas:")
vendas.append(1100)  # Adicionar ao final
print(f"Após append: {vendas}")

vendas.insert(2, 750)  # Inserir em posição específica
print(f"Após insert na posição 2: {vendas}")

vendas.extend([300, 450])  # Adicionar múltiplos elementos
print(f"Após extend: {vendas}")

print(f"Maior venda: {max(vendas)}")
print(f"Menor venda: {min(vendas)}")
print(f"Soma total: {sum(vendas)}")
print(f"Índice da venda 800: {vendas.index(800)}")

# List comprehensions
print("\nList comprehensions:")
vendas_com_imposto = [v * 1.1 for v in vendas if v > 500]
print(f"Vendas > 500 com 10% imposto: {vendas_com_imposto}")

# Tuplas - Imutáveis, ordenadas, permitem duplicatas
print("\nTUPLAS:")
coordenadas = (10.5, 20.3)
vendedor_info = ('João Silva', 101, 'Vendas', 5000.00)
print(f"Coordenadas: {coordenadas}")
print(f"Info vendedor: {vendedor_info}")

# Unpacking de tuplas
nome, id_vendedor, departamento, salario = vendedor_info
print(f"Nome: {nome}, ID: {id_vendedor}, Depto: {departamento}")

# Sets - Únicos, não ordenados
print("\nSETS:")
regioes_vendas = {'Sul', 'Norte', 'Centro', 'Sul', 'Leste'}
print(f"Regiões únicas: {regioes_vendas}")

# Operações de conjuntos
vendedores_q1 = {'João', 'Maria', 'Pedro'}
vendedores_q2 = {'Maria', 'Pedro', 'Ana', 'Carlos'}

print(f"Vendedores ambos trimestres: {vendedores_q1 & vendedores_q2}")
print(f"Todos vendedores: {vendedores_q1 | vendedores_q2}")
print(f"Apenas Q1: {vendedores_q1 - vendedores_q2}")

# Dicionários - Chave-valor, ordenados (Python 3.7+)
print("\nDICIONÁRIOS:")
vendedor = {
    'id': 101,
    'nome': 'João Silva',
    'vendas': [2500, 1800, 2200],
    'meta': 20000,
    'ativo': True
}

print(f"Vendedor: {vendedor}")
print(f"Nome: {vendedor['nome']}")
print(f"Meta: {vendedor.get('meta', 'Não definida')}")

# Operações importantes
vendedor['comissao'] = sum(vendedor['vendas']) * 0.05
print(f"Comissão calculada: {vendedor['comissao']}")

# Dictionary comprehension
vendas_por_mes = {'jan': 5000, 'fev': 6200, 'mar': 4800}
vendas_formatadas = {mes: f"R$ {valor:,.2f}" for mes, valor in vendas_por_mes.items()}
print(f"Vendas formatadas: {vendas_formatadas}")

print()

# =============================================================================
# 2. ESTRUTURAS DE DADOS AVANÇADAS
# =============================================================================

print("2. ESTRUTURAS DE DADOS AVANÇADAS")
print("=" * 50)

# defaultdict - Dicionário com valor padrão
print("DEFAULTDICT:")
vendas_por_regiao = defaultdict(list)
transacoes = [
    ('Sul', 2500), ('Norte', 1800), ('Sul', 3200), 
    ('Centro', 1500), ('Norte', 2100)
]

for regiao, valor in transacoes:
    vendas_por_regiao[regiao].append(valor)

print(f"Vendas por região: {dict(vendas_por_regiao)}")

# Counter - Contador de elementos
print("\nCOUNTER:")
produtos_vendidos = ['Notebook', 'Mouse', 'Notebook', 'Teclado', 'Mouse', 'Notebook']
contador_produtos = Counter(produtos_vendidos)
print(f"Produtos mais vendidos: {contador_produtos}")
print(f"Top 2: {contador_produtos.most_common(2)}")

# deque - Double-ended queue
print("\nDEQUE:")
fila_atendimento = deque(['Cliente1', 'Cliente2', 'Cliente3'])
print(f"Fila inicial: {list(fila_atendimento)}")

fila_atendimento.appendleft('ClienteUrgente')  # Adicionar no início
cliente_atendido = fila_atendimento.popleft()  # Remover do início
print(f"Cliente atendido: {cliente_atendido}")
print(f"Fila atual: {list(fila_atendimento)}")

print()

# =============================================================================
# 3. ESTRUTURAS DE CONTROLE E LOOPS
# =============================================================================

print("3. ESTRUTURAS DE CONTROLE E LOOPS")
print("=" * 50)

# For loops com diferentes estruturas
print("FOR LOOPS:")
vendas_mensais = [15000, 18000, 22000, 19000, 25000]

print("Índices e valores:")
for i, venda in enumerate(vendas_mensais):
    print(f"Mês {i+1}: R$ {venda:,}")

print("\nLoop em dicionário:")
metas = {'Jan': 20000, 'Fev': 22000, 'Mar': 25000}
for mes, meta in metas.items():
    print(f"{mes}: Meta R$ {meta:,}")

# Loops com condicionais
print("\nLoop com condicionais:")
for i, venda in enumerate(vendas_mensais):
    status = "Acima" if venda > 20000 else "Abaixo"
    print(f"Mês {i+1}: {status} da meta")

# While loops
print("\nWHILE LOOP:")
saldo = 10000
tentativas = 0
while saldo > 0 and tentativas < 5:
    desconto = saldo * 0.1
    saldo -= desconto
    tentativas += 1
    print(f"Tentativa {tentativas}: Saldo R$ {saldo:.2f}")

# List/Dict/Set comprehensions avançadas
print("\nCOMPREHENSIONS AVANÇADAS:")
vendedores_dados = [
    {'nome': 'João', 'vendas': 25000, 'meta': 20000},
    {'nome': 'Maria', 'vendas': 18000, 'meta': 20000},
    {'nome': 'Pedro', 'vendas': 30000, 'meta': 25000}
]

# Vendedores que bateram meta
bateram_meta = [v['nome'] for v in vendedores_dados if v['vendas'] >= v['meta']]
print(f"Bateram meta: {bateram_meta}")

# Dicionário de performance
performance = {v['nome']: v['vendas']/v['meta'] for v in vendedores_dados}
print(f"Performance: {performance}")

print()

# =============================================================================
# 4. FUNÇÕES E PROGRAMAÇÃO FUNCIONAL
# =============================================================================

print("4. FUNÇÕES E PROGRAMAÇÃO FUNCIONAL")
print("=" * 50)

# Funções básicas
def calcular_comissao(vendas: float, percentual: float = 0.05) -> float:
    """Calcula comissão sobre vendas"""
    return vendas * percentual

def aplicar_desconto(valor: float, desconto: float) -> float:
    """Aplica desconto ao valor"""
    return valor * (1 - desconto)

print("FUNÇÕES BÁSICAS:")
venda = 10000
comissao = calcular_comissao(venda, 0.07)
print(f"Comissão de R$ {venda}: R$ {comissao:.2f}")

# *args e **kwargs
def processar_vendas(*vendas, **opcoes):
    """Processa múltiplas vendas com opções"""
    total = sum(vendas)
    
    if opcoes.get('aplicar_imposto', False):
        total *= 1.1
    
    if opcoes.get('desconto'):
        total *= (1 - opcoes['desconto'])
        
    return total

resultado = processar_vendas(1000, 2000, 3000, aplicar_imposto=True, desconto=0.05)
print(f"Total processado: R$ {resultado:.2f}")

# Lambda functions
print("\nLAMBDA FUNCTIONS:")
vendas_lista = [1000, 2500, 1800, 3200, 900]
vendas_ordenadas = sorted(vendas_lista, key=lambda x: x, reverse=True)
print(f"Vendas ordenadas: {vendas_ordenadas}")

# Map, filter, reduce
print("\nMAP, FILTER, REDUCE:")
vendas_com_bonus = list(map(lambda x: x * 1.1, vendas_lista))
print(f"Vendas com bônus 10%: {vendas_com_bonus}")

vendas_altas = list(filter(lambda x: x > 2000, vendas_lista))
print(f"Vendas > 2000: {vendas_altas}")

total_vendas = reduce(lambda x, y: x + y, vendas_lista)
print(f"Total vendas: {total_vendas}")

# Decorators
print("\nDECORATORS:")
def log_execucao(func):
    """Decorator para logar execução de funções"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Executando {func.__name__} com args: {args}")
        resultado = func(*args, **kwargs)
        print(f"Resultado: {resultado}")
        return resultado
    return wrapper

@log_execucao
def calcular_meta_anual(vendas_mensais: List[float]) -> float:
    return sum(vendas_mensais) * 1.2

meta = calcular_meta_anual([15000, 18000, 22000])

print()

# =============================================================================
# 5. PROGRAMAÇÃO ORIENTADA A OBJETOS
# =============================================================================

print("5. PROGRAMAÇÃO ORIENTADA A OBJETOS")
print("=" * 50)

# Classe básica
class Vendedor:
    """Classe representando um vendedor"""
    
    # Atributo de classe
    comissao_padrao = 0.05
    
    def __init__(self, nome: str, id_vendedor: int, meta_mensal: float):
        # Atributos de instância
        self.nome = nome
        self.id_vendedor = id_vendedor
        self.meta_mensal = meta_mensal
        self._vendas = []  # Atributo "privado"
        
    def adicionar_venda(self, valor: float) -> None:
        """Adiciona uma venda ao vendedor"""
        self._vendas.append(valor)
        
    def total_vendas(self) -> float:
        """Retorna total de vendas"""
        return sum(self._vendas)
    
    def calcular_comissao(self, percentual: Optional[float] = None) -> float:
        """Calcula comissão do vendedor"""
        perc = percentual or self.comissao_padrao
        return self.total_vendas() * perc
    
    def bateu_meta(self) -> bool:
        """Verifica se bateu a meta mensal"""
        return self.total_vendas() >= self.meta_mensal
    
    def __str__(self) -> str:
        return f"Vendedor({self.nome}, ID: {self.id_vendedor})"
    
    def __repr__(self) -> str:
        return f"Vendedor('{self.nome}', {self.id_vendedor}, {self.meta_mensal})"

print("CLASSE BÁSICA:")
vendedor1 = Vendedor("João Silva", 101, 20000)
vendedor1.adicionar_venda(5000)
vendedor1.adicionar_venda(8000)
vendedor1.adicionar_venda(7500)

print(f"Vendedor: {vendedor1}")
print(f"Total vendas: R$ {vendedor1.total_vendas():,.2f}")
print(f"Comissão: R$ {vendedor1.calcular_comissao():,.2f}")
print(f"Bateu meta: {vendedor1.bateu_meta()}")

# Herança
class VendedorSenior(Vendedor):
    """Vendedor sênior com funcionalidades extras"""
    
    def __init__(self, nome: str, id_vendedor: int, meta_mensal: float, 
                 anos_experiencia: int):
        super().__init__(nome, id_vendedor, meta_mensal)
        self.anos_experiencia = anos_experiencia
        
    def calcular_comissao(self, percentual: Optional[float] = None) -> float:
        """Comissão maior para vendedores sêniores"""
        base = super().calcular_comissao(percentual)
        bonus_experiencia = self.anos_experiencia * 0.001  # 0.1% por ano
        return base * (1 + bonus_experiencia)
    
    def pode_gerenciar(self) -> bool:
        """Vendedores com 5+ anos podem gerenciar"""
        return self.anos_experiencia >= 5

print("\nHERANÇA:")
vendedor_senior = VendedorSenior("Maria Santos", 102, 25000, 8)
vendedor_senior.adicionar_venda(10000)
vendedor_senior.adicionar_venda(15000)

print(f"Vendedor sênior: {vendedor_senior}")
print(f"Comissão: R$ {vendedor_senior.calcular_comissao():,.2f}")
print(f"Pode gerenciar: {vendedor_senior.pode_gerenciar()}")

# Polimorfismo
def relatorio_vendedor(vendedor: Vendedor) -> None:
    """Gera relatório para qualquer tipo de vendedor"""
    print(f"\n--- Relatório {vendedor.nome} ---")
    print(f"Total vendas: R$ {vendedor.total_vendas():,.2f}")
    print(f"Meta: R$ {vendedor.meta_mensal:,.2f}")
    print(f"Comissão: R$ {vendedor.calcular_comissao():,.2f}")
    print(f"Status meta: {'✓' if vendedor.bateu_meta() else '✗'}")

print("\nPOLIMORFISMO:")
relatorio_vendedor(vendedor1)
relatorio_vendedor(vendedor_senior)

# Abstração
print("\nABSTRAÇÃO:")
class Relatorio(ABC):
    """Classe abstrata para relatórios"""
    
    @abstractmethod
    def gerar(self) -> str:
        """Método abstrato para gerar relatório"""
        pass
    
    @abstractmethod
    def salvar(self, arquivo: str) -> None:
        """Método abstrato para salvar relatório"""
        pass

class RelatorioVendas(Relatorio):
    """Implementação concreta do relatório de vendas"""
    
    def __init__(self, vendedores: List[Vendedor]):
        self.vendedores = vendedores
    
    def gerar(self) -> str:
        """Gera relatório de vendas"""
        total_geral = sum(v.total_vendas() for v in self.vendedores)
        linhas = ["=== RELATÓRIO DE VENDAS ==="]
        
        for vendedor in self.vendedores:
            linhas.append(f"{vendedor.nome}: R$ {vendedor.total_vendas():,.2f}")
        
        linhas.append(f"TOTAL GERAL: R$ {total_geral:,.2f}")
        return "\n".join(linhas)
    
    def salvar(self, arquivo: str) -> None:
        """Salva relatório em arquivo"""
        print(f"Salvando relatório em {arquivo}")
        # Aqui salvaria o arquivo
        
relatorio = RelatorioVendas([vendedor1, vendedor_senior])
print(relatorio.gerar())

# Encapsulamento com properties
print("\nENCAPSULAMENTO:")
class ContaBancaria:
    """Classe com encapsulamento usando properties"""
    
    def __init__(self, saldo_inicial: float = 0):
        self._saldo = saldo_inicial
        self._historico = []
    
    @property
    def saldo(self) -> float:
        """Getter para saldo"""
        return self._saldo
    
    @saldo.setter
    def saldo(self, valor: float) -> None:
        """Setter para saldo com validação"""
        if valor < 0:
            raise ValueError("Saldo não pode ser negativo")
        self._saldo = valor
    
    @property
    def historico(self) -> List[str]:
        """Histórico somente leitura"""
        return self._historico.copy()
    
    def depositar(self, valor: float) -> None:
        """Deposita valor na conta"""
        if valor <= 0:
            raise ValueError("Valor deve ser positivo")
        self._saldo += valor
        self._historico.append(f"Depósito: +R$ {valor:.2f}")
    
    def sacar(self, valor: float) -> None:
        """Saca valor da conta"""
        if valor <= 0:
            raise ValueError("Valor deve ser positivo")
        if valor > self._saldo:
            raise ValueError("Saldo insuficiente")
        self._saldo -= valor
        self._historico.append(f"Saque: -R$ {valor:.2f}")

conta = ContaBancaria(1000)
print(f"Saldo inicial: R$ {conta.saldo:.2f}")

conta.depositar(500)
conta.sacar(200)
print(f"Saldo final: R$ {conta.saldo:.2f}")
print(f"Histórico: {conta.historico}")

print()

# =============================================================================
# 6. TRATAMENTO DE ERROS E EXCEÇÕES
# =============================================================================

print("6. TRATAMENTO DE ERROS E EXCEÇÕES")
print("=" * 50)

# Exceções customizadas
class VendaInvalidaError(Exception):
    """Exceção para vendas inválidas"""
    pass

class MetaNaoAtingidaError(Exception):
    """Exceção para meta não atingida"""
    def __init__(self, vendas: float, meta: float):
        self.vendas = vendas
        self.meta = meta
        super().__init__(f"Meta não atingida: R$ {vendas} < R$ {meta}")

def processar_venda_segura(valor: float) -> Dict[str, Union[float, str]]:
    """Processa venda com tratamento de erros"""
    try:
        if valor <= 0:
            raise VendaInvalidaError(f"Valor inválido: {valor}")
        
        if valor < 1000:
            raise MetaNaoAtingidaError(valor, 1000)
        
        comissao = valor * 0.05
        return {
            'valor': valor,
            'comissao': comissao,
            'status': 'sucesso'
        }
        
    except VendaInvalidaError as e:
        return {
            'valor': valor,
            'erro': str(e),
            'status': 'erro_valor'
        }
    
    except MetaNaoAtingidaError as e:
        return {
            'valor': valor,
            'erro': str(e),
            'status': 'meta_nao_atingida'
        }
    
    except Exception as e:
        return {
            'valor': valor,
            'erro': f"Erro inesperado: {str(e)}",
            'status': 'erro_geral'
        }
    
    finally:
        print(f"Processamento finalizado para valor: {valor}")

print("TRATAMENTO DE EXCEÇÕES:")
valores_teste = [1500, -100, 500, 2000]

for valor in valores_teste:
    resultado = processar_venda_segura(valor)
    print(f"Valor {valor}: {resultado}")

print()

# =============================================================================
# 7. GERENCIAMENTO DE CONTEXTO E RECURSOS
# =============================================================================

print("7. GERENCIAMENTO DE CONTEXTO")
print("=" * 50)

# Context manager customizado
class ConexaoBancoDados:
    """Simulação de conexão com banco de dados"""
    
    def __init__(self, host: str):
        self.host = host
        self.conectado = False
    
    def __enter__(self):
        print(f"Conectando ao banco {self.host}...")
        self.conectado = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Fechando conexão com {self.host}...")
        self.conectado = False
        if exc_type:
            print(f"Erro durante operação: {exc_val}")
        return False  # Não suprimir exceções
    
    def executar_query(self, query: str) -> List[Dict]:
        """Simula execução de query"""
        if not self.conectado:
            raise RuntimeError("Não conectado ao banco")
        
        print(f"Executando: {query}")
        # Simulação de dados
        return [
            {'id': 1, 'nome': 'João', 'vendas': 25000},
            {'id': 2, 'nome': 'Maria', 'vendas': 30000}
        ]

print("CONTEXT MANAGER:")
try:
    with ConexaoBancoDados("localhost") as db:
        dados = db.executar_query("SELECT * FROM vendedores")
        print(f"Dados obtidos: {dados}")
except Exception as e:
    print(f"Erro: {e}")

print()

# =============================================================================
# 8. ITERADORES E GERADORES
# =============================================================================

print("8. ITERADORES E GERADORES")
print("=" * 50)

# Gerador simples
def fibonacci_generator(n: int):
    """Gera sequência de Fibonacci até n termos"""
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b

print("GERADORES:")
fib_seq = list(fibonacci_generator(10))
print(f"Fibonacci (10 termos): {fib_seq}")

# Gerador de vendas mensais
def vendas_mensais_generator(vendas_anuais: List[float]):
    """Gera relatório mensal de vendas"""
    meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
             'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    
    for i, venda in enumerate(vendas_anuais):
        mes = meses[i % 12]
        yield {
            'mes': mes,
            'vendas': venda,
            'acumulado': sum(vendas_anuais[:i+1])
        }

vendas_ano = [15000, 18000, 22000, 19000, 25000, 21000]
for relatorio_mes in vendas_mensais_generator(vendas_ano):
    print(f"{relatorio_mes['mes']}: R$ {relatorio_mes['vendas']:,} "
          f"(Acum: R$ {relatorio_mes['acumulado']:,})")

# Iterator customizado
class VendedorIterator:
    """Iterator para percorrer vendedores"""
    
    def __init__(self, vendedores: List[Vendedor]):
        self.vendedores = vendedores
        self.index = 0
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.index >= len(self.vendedores):
            raise StopIteration
        
        vendedor = self.vendedores[self.index]
        self.index += 1
        return vendedor

print("\nITERATOR CUSTOMIZADO:")
vendedores = [vendedor1, vendedor_senior]
for vendedor in VendedorIterator(vendedores):
    print(f"- {vendedor.nome}: R$ {vendedor.total_vendas():,.2f}")

print()

# =============================================================================
# 9. CONCEITOS AVANÇADOS E BOAS PRÁTICAS
# =============================================================================

print("9. CONCEITOS AVANÇADOS E BOAS PRÁTICAS")
print("=" * 50)

# Type hints avançados
from typing import Protocol, Generic, TypeVar

T = TypeVar('T')

class Processavel(Protocol):
    """Protocol para objetos processáveis"""
    def processar(self) -> float:
        ...

class ProcessadorGenerico(Generic[T]):
    """Processador genérico"""
    
    def __init__(self, items: List[T]):
        self.items = items
    
    def processar_todos(self) -> List[float]:
        """Processa todos os items que implementam Processavel"""
        resultados = []
        for item in self.items:
            if hasattr(item, 'processar'):
                resultados.append(item.processar())
        return resultados

# Dataclasses
from dataclasses import dataclass, field
from typing import List

@dataclass
class ProdutoVenda:
    """Dataclass para produto de venda"""
    nome: str
    preco: float
    categoria: str
    estoque: int = 0
    tags: List[str] = field(default_factory=list)
    
    def valor_estoque(self) -> float:
        """Calcula valor total do estoque"""
        return self.preco * self.estoque
    
    def __post_init__(self):
        """Validações pós-inicialização"""
        if self.preco < 0:
            raise ValueError("Preço não pode ser negativo")

print("DATACLASSES:")
produto = ProdutoVenda(
    nome="Notebook Dell",
    preco=2500.0,
    categoria="Eletrônicos",
    estoque=10,
    tags=["laptop", "dell", "intel"]
)

print(f"Produto: {produto}")
print(f"Valor estoque: R$ {produto.valor_estoque():,.2f}")

# Enum para constantes
from enum import Enum, auto

class StatusVenda(Enum):
    """Status possíveis de uma venda"""
    PENDENTE = auto()
    PROCESSANDO = auto()
    APROVADA = auto()
    REJEITADA = auto()
    CANCELADA = auto()

@dataclass
class Venda:
    """Dataclass para venda"""
    id: int
    vendedor_id: int
    valor: float
    status: StatusVenda = StatusVenda.PENDENTE
    
    def aprovar(self):
        """Aprova a venda"""
        if self.status == StatusVenda.PENDENTE:
            self.status = StatusVenda.APROVADA
        else:
            raise ValueError(f"Não é possível aprovar venda com status {self.status.name}")

print("\nENUMS:")
venda = Venda(1, 101, 1500.0)
print(f"Venda inicial: {venda}")
venda.aprovar()
print(f"Após aprovação: {venda}")

# Padrão Singleton
class GerenciadorConfiguracao:
    """Singleton para configurações"""
    _instance = None
    _configuracoes = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def set_config(self, chave: str, valor) -> None:
        """Define configuração"""
        self._configuracoes[chave] = valor
    
    def get_config(self, chave: str, padrao=None):
        """Obtém configuração"""
        return self._configuracoes.get(chave, padrao)

print("\nSINGLETON:")
config1 = GerenciadorConfiguracao()
config2 = GerenciadorConfiguracao()

config1.set_config('host_db', 'localhost')
print(f"Config1: {config1.get_config('host_db')}")
print(f"Config2: {config2.get_config('host_db')}")
print(f"São o mesmo objeto: {config1 is config2}")

print()

# =============================================================================
# 10. DICAS PARA ENTREVISTAS
# =============================================================================

print("10. DICAS PARA ENTREVISTAS")
print("=" * 50)

print("✅ CONCEITOS ESSENCIAIS:")
print("1. Complexidade de algoritmos (Big O)")
print("2. Estruturas de dados apropriadas para cada problema")
print("3. Diferenças entre lista, tupla, set e dict")
print("4. Quando usar herança vs composição")
print("5. Tratamento adequado de exceções")
print("6. Type hints e documentação")
print("7. Testes unitários e TDD")
print("8. Padrões de design (Singleton, Factory, Observer)")
print("9. Princípios SOLID")
print("10. Clean Code e refatoração")
print()

print("🚀 EXERCÍCIOS PRÁTICOS COMUNS:")

# Exercício 1: Encontrar duplicatas
def encontrar_duplicatas(lista: List[int]) -> List[int]:
    """Encontra elementos duplicados em uma lista"""
    vistos = set()
    duplicatas = set()
    
    for item in lista:
        if item in vistos:
            duplicatas.add(item)
        else:
            vistos.add(item)
    
    return list(duplicatas)

numeros = [1, 2, 3, 2, 4, 5, 3, 6]
print(f"Duplicatas em {numeros}: {encontrar_duplicatas(numeros)}")

# Exercício 2: Cache com decorator
def cache_resultado(func):
    """Decorator para cache de resultados"""
    cache = {}
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        chave = str(args) + str(kwargs)
        if chave not in cache:
            cache[chave] = func(*args, **kwargs)
        return cache[chave]
    
    wrapper.cache = cache
    return wrapper

@cache_resultado
def fibonacci(n: int) -> int:
    """Calcula Fibonacci com cache"""
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

print(f"Fibonacci(10): {fibonacci(10)}")
print(f"Cache hits: {len(fibonacci.cache)}")

print()
print("=== FIM DO GUIA PYTHON ===")
print("Este guia cobre os fundamentos essenciais do Python para entrevistas técnicas.")
print("Pratique estes conceitos e implemente suas próprias variações!")