# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta e ingestão → camada **Bronze**
# MAGIC
# MAGIC **O que este notebook faz**
# MAGIC
# MAGIC 1. Baixa os 9 arquivos CSV do dataset público da Olist direto para o Volume
# MAGIC    `olist_mvp.bronze.landing` (a coleta roda *dentro* da nuvem — nada passa pela máquina local).
# MAGIC 2. Confere a integridade do que chegou (tamanho em bytes e MD5 de cada arquivo).
# MAGIC 3. Lê cada CSV **com todas as colunas como STRING** e grava a tabela Bronze correspondente.
# MAGIC
# MAGIC **Princípio da camada Bronze:** o dado é preservado exatamente como veio. Nenhuma coluna é
# MAGIC renomeada, tipada, deduplicada ou corrigida aqui — inclusive os erros de digitação da fonte
# MAGIC (`product_name_lenght`) permanecem. A única adição são três colunas de controle
# MAGIC (`_arquivo_origem`, `_url_origem`, `_data_ingestao`) que garantem rastreabilidade/linhagem.
# MAGIC
# MAGIC A ingestão é **idempotente**: arquivos já presentes no Volume não são baixados de novo, e as
# MAGIC tabelas são sobrescritas a cada execução.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import hashlib
import os
import urllib.request
from datetime import datetime

os.makedirs(LANDING, exist_ok=True)

for chave, (arquivo, _tabela, _opcoes) in ARQUIVOS_FONTE.items():
    destino = f"{LANDING}/{arquivo}"
    if os.path.exists(destino) and os.path.getsize(destino) > 0:
        print(f"· {arquivo}: já presente, download ignorado")
        continue
    url = f"{FONTE_BASE_URL}/{arquivo}"
    print(f"↓ {arquivo} ← {url}")
    urllib.request.urlretrieve(url, destino)

print("\nColeta concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência de integridade dos arquivos coletados
# MAGIC
# MAGIC Registrar tamanho e hash é o que permite afirmar, meses depois, que a Bronze foi construída
# MAGIC a partir *destes* arquivos e não de outra versão do dataset.

# COMMAND ----------

inventario = []
for chave, (arquivo, _tabela, _opcoes) in ARQUIVOS_FONTE.items():
    caminho = f"{LANDING}/{arquivo}"
    md5 = hashlib.md5()
    with open(caminho, "rb") as fh:
        for bloco in iter(lambda: fh.read(1 << 20), b""):
            md5.update(bloco)
    inventario.append((arquivo, os.path.getsize(caminho), md5.hexdigest()))

df_inventario = spark.createDataFrame(inventario, "arquivo string, bytes long, md5 string")
display(df_inventario.orderBy("arquivo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga das tabelas Bronze
# MAGIC
# MAGIC `inferSchema` fica **desligado** de propósito: inferir tipo na Bronze significaria deixar o
# MAGIC Spark decidir, em silêncio, o que é número e o que é data — e engolir como `null` tudo que
# MAGIC não se encaixasse. Lendo tudo como texto, qualquer valor sujo sobrevive e pode ser medido no
# MAGIC notebook de qualidade (02) antes de ser tratado na Silver (03).
# MAGIC
# MAGIC `olist_order_reviews_dataset.csv` exige `multiLine=true`: comentários de clientes contêm
# MAGIC quebras de linha dentro do campo aspeado.

# COMMAND ----------

agora = datetime.utcnow().isoformat(timespec="seconds")

for chave, (arquivo, tabela, opcoes) in ARQUIVOS_FONTE.items():
    leitor = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
    )
    for chave_opcao, valor in opcoes.items():
        leitor = leitor.option(chave_opcao, valor)

    df = leitor.load(f"{LANDING}/{arquivo}")

    # Alguns CSVs da fonte trazem BOM (UTF-8 com marca de ordem de byte) grudado no nome
    # da primeira coluna: 'product_category_name' vira '﻿product_category_name'.
    df = df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])

    df = (
        df.withColumn("_arquivo_origem", F.lit(arquivo))
          .withColumn("_url_origem", F.lit(f"{FONTE_BASE_URL}/{arquivo}"))
          .withColumn("_data_ingestao", F.lit(agora).cast("timestamp"))
    )

    salva_tabela(
        df,
        f"{BRONZE}.{tabela}",
        comentario=f"Bronze - conteúdo bruto de {arquivo}, todas as colunas como STRING",
    )

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {BRONZE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contagem de linhas por tabela Bronze

# COMMAND ----------

contagens = [
    (tabela, spark.table(f"{BRONZE}.{tabela}").count())
    for _, (_a, tabela, _o) in ARQUIVOS_FONTE.items()
]
display(spark.createDataFrame(contagens, "tabela string, linhas long").orderBy("tabela"))
