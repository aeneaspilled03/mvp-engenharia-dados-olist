# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Análise de qualidade sobre a **Bronze**
# MAGIC
# MAGIC Antes de limpar qualquer coisa é preciso **medir**. Este notebook roda sobre o dado cru e
# MAGIC produz duas tabelas de evidência, que são o insumo das decisões tomadas na Silver:
# MAGIC
# MAGIC | Tabela | Conteúdo |
# MAGIC |---|---|
# MAGIC | `bronze.qualidade_perfilagem` | uma linha por **coluna** de cada tabela: nulos, distintos, mínimo e máximo |
# MAGIC | `bronze.qualidade_checks` | uma linha por **verificação** nas 5 dimensões: completude, unicidade, consistência, acurácia e outliers (+ integridade referencial) |
# MAGIC
# MAGIC Nenhum dado é corrigido aqui. Este notebook só diagnostica.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Perfilagem coluna a coluna
# MAGIC
# MAGIC Como a Bronze é toda textual, "vazio" precisa considerar tanto `NULL` quanto string em branco —
# MAGIC um CSV representa ausência das duas formas.

# COMMAND ----------

def perfila(tabela: str):
    df = spark.table(f"{BRONZE}.{tabela}")
    colunas = [c for c in df.columns if not c.startswith("_")]
    total = df.count()

    agregacoes = []
    for c in colunas:
        vazio = F.col(c).isNull() | (F.trim(F.col(c)) == F.lit(""))
        agregacoes += [
            F.sum(F.when(vazio, 1).otherwise(0)).alias(f"{c}||nulos"),
            F.countDistinct(F.col(c)).alias(f"{c}||distintos"),
            F.min(F.col(c)).alias(f"{c}||min"),
            F.max(F.col(c)).alias(f"{c}||max"),
        ]

    linha = df.agg(*agregacoes).collect()[0].asDict()
    saida = []
    for c in colunas:
        nulos = int(linha[f"{c}||nulos"])
        saida.append((
            tabela, c, total, nulos, round(100.0 * nulos / total, 2) if total else None,
            int(linha[f"{c}||distintos"]),
            (str(linha[f"{c}||min"])[:60] if linha[f"{c}||min"] is not None else None),
            (str(linha[f"{c}||max"])[:60] if linha[f"{c}||max"] is not None else None),
        ))
    return saida


perfil = []
for _chave, (_arquivo, tabela, _opcoes) in ARQUIVOS_FONTE.items():
    print(f"perfilando {tabela} ...")
    perfil += perfila(tabela)

df_perfil = spark.createDataFrame(
    perfil,
    "tabela string, coluna string, linhas long, vazios long, pct_vazios double, "
    "valores_distintos long, valor_minimo string, valor_maximo string",
)

salva_tabela(
    df_perfil,
    f"{BRONZE}.qualidade_perfilagem",
    comentario="Perfilagem coluna a coluna das tabelas Bronze (completude, cardinalidade, faixa de valores)",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Colunas com ausência de dados

# COMMAND ----------

display(
    spark.table(f"{BRONZE}.qualidade_perfilagem")
    .filter("vazios > 0")
    .orderBy(F.desc("pct_vazios"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Verificações dirigidas
# MAGIC
# MAGIC Cada verificação devolve **quantas ocorrências** do problema existem. `esperado` é o limite
# MAGIC aceitável: acima dele a Silver precisa tratar o caso explicitamente.

# COMMAND ----------

CHECKS = [
    # ----------------------------------------------------------------- unicidade
    ("Unicidade", "pedidos", "order_id duplicado",
     f"SELECT count(*) FROM (SELECT order_id FROM {BRONZE}.pedidos GROUP BY order_id HAVING count(*) > 1)", 0),
    ("Unicidade", "clientes", "customer_id duplicado",
     f"SELECT count(*) FROM (SELECT customer_id FROM {BRONZE}.clientes GROUP BY customer_id HAVING count(*) > 1)", 0),
    ("Unicidade", "itens_pedido", "chave (order_id, order_item_id) duplicada",
     f"SELECT count(*) FROM (SELECT order_id, order_item_id FROM {BRONZE}.itens_pedido GROUP BY 1,2 HAVING count(*) > 1)", 0),
    ("Unicidade", "avaliacoes", "review_id repetido em mais de uma linha",
     f"SELECT count(*) FROM (SELECT review_id FROM {BRONZE}.avaliacoes GROUP BY review_id HAVING count(*) > 1)", 0),
    ("Unicidade", "avaliacoes", "order_id com mais de uma avaliação",
     f"SELECT count(*) FROM (SELECT order_id FROM {BRONZE}.avaliacoes GROUP BY order_id HAVING count(*) > 1)", 0),
    ("Unicidade", "geolocalizacao", "linhas inteiramente duplicadas",
     f"SELECT count(*) - count(DISTINCT geolocation_zip_code_prefix || '|' || geolocation_lat || '|' || geolocation_lng || '|' || geolocation_city || '|' || geolocation_state) FROM {BRONZE}.geolocalizacao", 0),
    ("Unicidade", "produtos", "product_id duplicado",
     f"SELECT count(*) FROM (SELECT product_id FROM {BRONZE}.produtos GROUP BY product_id HAVING count(*) > 1)", 0),
    ("Unicidade", "vendedores", "seller_id duplicado",
     f"SELECT count(*) FROM (SELECT seller_id FROM {BRONZE}.vendedores GROUP BY seller_id HAVING count(*) > 1)", 0),

    # ------------------------------------------------------ integridade referencial
    ("Integridade referencial", "itens_pedido", "order_id inexistente em pedidos",
     f"SELECT count(*) FROM {BRONZE}.itens_pedido i LEFT ANTI JOIN {BRONZE}.pedidos p ON i.order_id = p.order_id", 0),
    ("Integridade referencial", "itens_pedido", "product_id inexistente em produtos",
     f"SELECT count(*) FROM {BRONZE}.itens_pedido i LEFT ANTI JOIN {BRONZE}.produtos pr ON i.product_id = pr.product_id", 0),
    ("Integridade referencial", "itens_pedido", "seller_id inexistente em vendedores",
     f"SELECT count(*) FROM {BRONZE}.itens_pedido i LEFT ANTI JOIN {BRONZE}.vendedores v ON i.seller_id = v.seller_id", 0),
    ("Integridade referencial", "pedidos", "customer_id inexistente em clientes",
     f"SELECT count(*) FROM {BRONZE}.pedidos p LEFT ANTI JOIN {BRONZE}.clientes c ON p.customer_id = c.customer_id", 0),
    ("Integridade referencial", "pedidos", "pedido sem nenhum item",
     f"SELECT count(*) FROM {BRONZE}.pedidos p LEFT ANTI JOIN {BRONZE}.itens_pedido i ON p.order_id = i.order_id", 0),
    ("Integridade referencial", "pedidos", "pedido sem nenhum pagamento",
     f"SELECT count(*) FROM {BRONZE}.pedidos p LEFT ANTI JOIN {BRONZE}.pagamentos pg ON p.order_id = pg.order_id", 0),
    ("Integridade referencial", "produtos", "categoria sem tradução cadastrada",
     f"SELECT count(DISTINCT product_category_name) FROM {BRONZE}.produtos p LEFT ANTI JOIN {BRONZE}.traducao_categoria t ON p.product_category_name = t.product_category_name WHERE p.product_category_name IS NOT NULL", 0),
    ("Integridade referencial", "clientes", "CEP sem coordenada em geolocalizacao",
     f"SELECT count(*) FROM {BRONZE}.clientes c LEFT ANTI JOIN {BRONZE}.geolocalizacao g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix", 0),

    # ---------------------------------------------------------------- completude
    ("Completude", "pedidos", "pedido entregue sem data de entrega ao cliente",
     f"SELECT count(*) FROM {BRONZE}.pedidos WHERE order_status = 'delivered' AND (order_delivered_customer_date IS NULL OR trim(order_delivered_customer_date) = '')", 0),
    ("Completude", "pedidos", "pedido sem data de aprovação",
     f"SELECT count(*) FROM {BRONZE}.pedidos WHERE order_approved_at IS NULL OR trim(order_approved_at) = ''", 0),
    ("Completude", "produtos", "produto sem categoria",
     f"SELECT count(*) FROM {BRONZE}.produtos WHERE product_category_name IS NULL OR trim(product_category_name) = ''", 0),
    ("Completude", "produtos", "produto sem peso/dimensões",
     f"SELECT count(*) FROM {BRONZE}.produtos WHERE product_weight_g IS NULL OR product_length_cm IS NULL", 0),

    # -------------------------------------------------------------- consistência
    ("Consistência", "pedidos", "pedido NÃO entregue com data de entrega preenchida",
     f"SELECT count(*) FROM {BRONZE}.pedidos WHERE order_status <> 'delivered' AND order_delivered_customer_date IS NOT NULL", 0),
    ("Consistência", "pedidos", "data de entrega anterior à compra",
     f"SELECT count(*) FROM {BRONZE}.pedidos WHERE timestamp(order_delivered_customer_date) < timestamp(order_purchase_timestamp)", 0),
    ("Consistência", "pedidos", "postagem à transportadora anterior à aprovação",
     f"SELECT count(*) FROM {BRONZE}.pedidos WHERE timestamp(order_delivered_carrier_date) < timestamp(order_approved_at)", 0),
    ("Consistência", "pagamentos", "payment_type = 'not_defined'",
     f"SELECT count(*) FROM {BRONZE}.pagamentos WHERE payment_type = 'not_defined'", 0),
    ("Consistência", "vendedores", "seller_city com sufixo de UF ou barra",
     f"SELECT count(*) FROM {BRONZE}.vendedores WHERE seller_city RLIKE '[/\\\\\\\\]' OR seller_city RLIKE ' (sp|rj|mg|df|pr|rs|sc|ba|go)$'", 0),
    ("Consistência", "vendedores", "seller_city preenchido com número",
     f"SELECT count(*) FROM {BRONZE}.vendedores WHERE seller_city RLIKE '^[0-9]+$'", 0),
    ("Consistência", "geolocalizacao", "variações de grafia para 'sao paulo'",
     f"SELECT count(DISTINCT geolocation_city) FROM {BRONZE}.geolocalizacao WHERE geolocation_city RLIKE '^s[aã]o? ?paulo$' OR geolocation_city RLIKE '^sa.o paulo$'", 1),

    # ------------------------------------------------------------------ acurácia
    ("Acurácia", "pagamentos", "payment_value menor ou igual a zero",
     f"SELECT count(*) FROM {BRONZE}.pagamentos WHERE double(payment_value) <= 0", 0),
    ("Acurácia", "pagamentos", "payment_installments igual a zero",
     f"SELECT count(*) FROM {BRONZE}.pagamentos WHERE int(payment_installments) = 0", 0),
    ("Acurácia", "itens_pedido", "price menor ou igual a zero",
     f"SELECT count(*) FROM {BRONZE}.itens_pedido WHERE double(price) <= 0", 0),
    ("Acurácia", "itens_pedido", "frete igual a zero",
     f"SELECT count(*) FROM {BRONZE}.itens_pedido WHERE double(freight_value) = 0", 0),
    ("Acurácia", "avaliacoes", "review_score fora da escala 1..5",
     f"SELECT count(*) FROM {BRONZE}.avaliacoes WHERE int(review_score) NOT BETWEEN 1 AND 5", 0),
    ("Acurácia", "geolocalizacao", "coordenada fora do território brasileiro",
     f"SELECT count(*) FROM {BRONZE}.geolocalizacao WHERE double(geolocation_lat) NOT BETWEEN {BR_LAT_MIN} AND {BR_LAT_MAX} OR double(geolocation_lng) NOT BETWEEN {BR_LNG_MIN} AND {BR_LNG_MAX}", 0),

    # ------------------------------------------------------------------ outliers
    ("Outliers", "itens_pedido", "preço de item acima de R$ 2.000",
     f"SELECT count(*) FROM {BRONZE}.itens_pedido WHERE double(price) > 2000", 0),
    ("Outliers", "itens_pedido", "frete acima de R$ 200",
     f"SELECT count(*) FROM {BRONZE}.itens_pedido WHERE double(freight_value) > 200", 0),
    ("Outliers", "pedidos", "entrega com mais de 90 dias",
     f"SELECT count(*) FROM {BRONZE}.pedidos WHERE datediff(timestamp(order_delivered_customer_date), timestamp(order_purchase_timestamp)) > 90", 0),
]

resultados = []
for dimensao, tabela, descricao, consulta, esperado in CHECKS:
    ocorrencias = spark.sql(consulta).collect()[0][0] or 0
    resultados.append((
        dimensao, tabela, descricao, int(ocorrencias), int(esperado),
        "OK" if ocorrencias <= esperado else "TRATAR NA SILVER",
    ))

df_checks = spark.createDataFrame(
    resultados,
    "dimensao string, tabela string, verificacao string, ocorrencias long, "
    "limite_aceitavel long, situacao string",
)

salva_tabela(
    df_checks,
    f"{BRONZE}.qualidade_checks",
    comentario="Resultado das verificações de qualidade executadas sobre a camada Bronze",
)

# COMMAND ----------

display(
    spark.table(f"{BRONZE}.qualidade_checks")
    .orderBy(F.col("situacao").desc(), F.col("ocorrencias").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo por dimensão de qualidade

# COMMAND ----------

display(
    spark.table(f"{BRONZE}.qualidade_checks")
    .groupBy("dimensao")
    .agg(
        F.count("*").alias("verificacoes"),
        F.sum(F.when(F.col("situacao") == "OK", 1).otherwise(0)).alias("sem_problema"),
        F.sum(F.when(F.col("situacao") != "OK", 1).otherwise(0)).alias("com_problema"),
        F.sum("ocorrencias").alias("total_ocorrencias"),
    )
    .orderBy(F.desc("com_problema"))
)
