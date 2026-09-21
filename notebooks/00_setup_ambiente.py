# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente (Unity Catalog)
# MAGIC
# MAGIC Cria a estrutura que sustenta a **Arquitetura Medalhão** deste MVP dentro do Unity Catalog:
# MAGIC
# MAGIC | Objeto | Nome | Papel |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `olist_mvp` | isola todo o projeto do resto do workspace |
# MAGIC | Schema | `bronze` | dado bruto, exatamente como veio da fonte |
# MAGIC | Schema | `silver` | dado limpo, tipado e padronizado |
# MAGIC | Schema | `gold` | modelo dimensional (esquema estrela) pronto para consumo |
# MAGIC | Volume | `bronze.landing` | área de pouso dos arquivos CSV originais |
# MAGIC | Volume | `gold.relatorios` | gráficos gerados pela análise |
# MAGIC
# MAGIC Este notebook é **idempotente**: pode ser reexecutado sem efeito colateral.

# COMMAND ----------

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Parametrização: os valores default são os usados no Databricks.
# As variáveis de ambiente existem apenas para permitir execução local em Spark OSS (teste do código).
CATALOG = os.environ.get("MVP_CATALOG", "olist_mvp")
SCHEMA_BRONZE = os.environ.get("MVP_SCHEMA_BRONZE", "bronze")
SCHEMA_SILVER = os.environ.get("MVP_SCHEMA_SILVER", "silver")
SCHEMA_GOLD = os.environ.get("MVP_SCHEMA_GOLD", "gold")
VOLUME_LANDING = "landing"

print(f"Catálogo alvo: {CATALOG}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(
    f"COMMENT ON CATALOG {CATALOG} IS "
    "'MVP Engenharia de Dados - marketplace Olist: promessa de entrega, satisfação e custo de frete'"
)

for schema, descricao in [
    (SCHEMA_BRONZE, "Camada Bronze - dados crus ingeridos da fonte, sem transformação de conteúdo"),
    (SCHEMA_SILVER, "Camada Silver - dados limpos, tipados, deduplicados e padronizados"),
    (SCHEMA_GOLD, "Camada Gold - modelo dimensional (esquema estrela) pronto para análise"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{descricao}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.{VOLUME_LANDING} "
          "COMMENT 'Área de pouso dos arquivos CSV originais do dataset Olist'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}.relatorios "
          "COMMENT 'Gráficos e artefatos gerados pela camada de análise'")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Estrutura criada. Próximo passo: `01_ingestao_bronze`.
