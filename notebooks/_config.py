# Databricks notebook source
# MAGIC %md
# MAGIC # _config · Parâmetros e funções compartilhadas
# MAGIC
# MAGIC Notebook auxiliar carregado pelos demais via `%run ./_config`.
# MAGIC Centraliza nomes de catálogo/schemas, a lista de arquivos da fonte e as funções
# MAGIC de padronização reutilizadas nas camadas Silver e Gold.

# COMMAND ----------

import os

from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------- parâmetros
CATALOG = os.environ.get("MVP_CATALOG", "olist_mvp")
SCHEMA_BRONZE = os.environ.get("MVP_SCHEMA_BRONZE", "bronze")
SCHEMA_SILVER = os.environ.get("MVP_SCHEMA_SILVER", "silver")
SCHEMA_GOLD = os.environ.get("MVP_SCHEMA_GOLD", "gold")

BRONZE = f"{CATALOG}.{SCHEMA_BRONZE}"
SILVER = f"{CATALOG}.{SCHEMA_SILVER}"
GOLD = f"{CATALOG}.{SCHEMA_GOLD}"

# Área de pouso dos CSVs (Volume do Unity Catalog).
LANDING = os.environ.get("MVP_LANDING", f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing")

# Pasta onde os notebooks 06 e 07 gravam artefatos (gráficos, catálogo em markdown).
DIR_SAIDA = os.environ.get("MVP_SAIDA", f"/Volumes/{CATALOG}/{SCHEMA_GOLD}/relatorios")

# Espelho público do "Brazilian E-Commerce Public Dataset by Olist".
# O dataset original é publicado no Kaggle (licença CC BY-NC-SA 4.0), cujo download exige
# autenticação; para que a ingestão seja reprodutível por qualquer avaliador sem credenciais,
# a coleta aponta para um espelho público dos mesmos arquivos. A conferência de integridade
# (contagem de linhas e MD5) está no notebook 01.
FONTE_BASE_URL = "https://raw.githubusercontent.com/dujiaying/olist/master/data"

# arquivo lógico -> (nome do CSV, tabela bronze, opções de parsing)
ARQUIVOS_FONTE = {
    "clientes": ("olist_customers_dataset.csv", "clientes", {}),
    "geolocalizacao": ("olist_geolocation_dataset.csv", "geolocalizacao", {}),
    "itens_pedido": ("olist_order_items_dataset.csv", "itens_pedido", {}),
    "pagamentos": ("olist_order_payments_dataset.csv", "pagamentos", {}),
    "avaliacoes": ("olist_order_reviews_dataset.csv", "avaliacoes", {"multiLine": "true"}),
    "pedidos": ("olist_orders_dataset.csv", "pedidos", {}),
    "produtos": ("olist_products_dataset.csv", "produtos", {}),
    "vendedores": ("olist_sellers_dataset.csv", "vendedores", {}),
    "traducao_categoria": ("product_category_name_translation.csv", "traducao_categoria", {}),
}

# ------------------------------------------------------- domínio geográfico
UF_REGIAO = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte",
    "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}

# Caixa delimitadora do território brasileiro, usada para descartar coordenadas impossíveis.
BR_LAT_MIN, BR_LAT_MAX = -33.75, 5.28
BR_LNG_MIN, BR_LNG_MAX = -73.99, -34.79


def col_regiao(coluna_uf):
    """Mapeia a sigla da UF para a região geográfica do IBGE."""
    expressao = F.lit(None).cast("string")
    for uf, regiao in UF_REGIAO.items():
        expressao = F.when(F.upper(F.trim(coluna_uf)) == uf, F.lit(regiao)).otherwise(expressao)
    return expressao


def normaliza_cidade(coluna):
    """Padroniza nomes de município: minúsculas, sem acento, sem sufixo de UF, sem ruído.

    Trata os defeitos observados na perfilagem da Bronze: variações de acentuação
    ('sao paulo' / 'são paulo' / 'sãopaulo'), mojibake de encoding ('sa£o paulo'),
    sufixos coladas ('rio de janeiro / rj', 'aguas claras df') e espaços duplicados.
    """
    c = F.lower(F.trim(coluna))
    c = F.regexp_replace(c, r"[/\\].*$", "")          # remove ' / rj', ' \\ sp'
    c = F.translate(c, "áàâãäéèêëíìîïóòôõöúùûüçñ", "aaaaaeeeeiiiiooooouuuucn")
    c = F.regexp_replace(c, r"[^a-z0-9 ]", " ")        # descarta resíduo de encoding
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    c = F.regexp_replace(c, r" (ac|al|ap|am|ba|ce|df|es|go|ma|mt|ms|mg|pa|pb|pr|pe|pi|rj|rn|rs|ro|rr|sc|sp|se|to)$", "")
    return F.when(F.length(c) == 0, F.lit(None).cast("string")).otherwise(F.trim(c))


def distancia_km(lat1, lng1, lat2, lng2):
    """Distância de grande círculo (Haversine) em quilômetros."""
    r = F.lit(6371.0)
    dlat = F.radians(lat2 - lat1)
    dlng = F.radians(lng2 - lng1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlng / 2) ** 2)
    return F.round(2 * r * F.asin(F.sqrt(a)), 2)


def salva_tabela(df, nome_completo, comentario=None, particao=None):
    """Persiste um DataFrame como tabela gerenciada (Delta no Databricks), sobrescrevendo."""
    writer = df.write.mode("overwrite").option("overwriteSchema", "true")
    if particao:
        writer = writer.partitionBy(particao)
    writer.saveAsTable(nome_completo)
    if comentario:
        spark.sql(f"COMMENT ON TABLE {nome_completo} IS '{comentario}'")
    print(f"  ✔ {nome_completo}: {spark.table(nome_completo).count():,} linhas")


def documenta_colunas(nome_completo, descricoes):
    """Grava a descrição de cada coluna no Unity Catalog (catálogo de dados vivo)."""
    for coluna, descricao in descricoes.items():
        texto = descricao.replace("'", "''")
        spark.sql(f"ALTER TABLE {nome_completo} ALTER COLUMN {coluna} COMMENT '{texto}'")
    print(f"  ✔ {nome_completo}: {len(descricoes)} colunas documentadas")


# `display` existe no Databricks; o fallback permite executar o mesmo código em Spark OSS.
try:
    display  # type: ignore[used-before-def]
except NameError:  # pragma: no cover
    def display(df, n: int = 20):
        df.show(n, truncate=False)


print(f"Config carregada · catálogo={CATALOG} · landing={LANDING}")
