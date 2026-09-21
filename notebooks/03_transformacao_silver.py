# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Limpeza e padronização → camada **Silver**
# MAGIC
# MAGIC Aqui o dado deixa de ser texto e passa a ter tipo, domínio e chave confiável.
# MAGIC Cada tratamento abaixo responde a um achado medido no notebook `02_qualidade_bronze`.
# MAGIC
# MAGIC | Problema medido na Bronze | Tratamento na Silver |
# MAGIC |---|---|
# MAGIC | Tudo é `STRING` | tipagem explícita: `timestamp`, `int`, `decimal(10,2)` |
# MAGIC | 559 pedidos com mais de uma avaliação | mantém apenas a avaliação mais recente por pedido |
# MAGIC | 261.831 linhas duplicadas em geolocalização | uma linha por CEP, com mediana das coordenadas |
# MAGIC | 42 coordenadas fora do Brasil | descartadas antes de calcular a mediana |
# MAGIC | Cidades com acento/encoding/sufixo de UF inconsistentes | `normaliza_cidade()` |
# MAGIC | 610 produtos sem categoria | categoria vira `nao_informado` (não se descarta a venda) |
# MAGIC | 2 categorias sem tradução | tradução recai para o nome em português |
# MAGIC | Datas fora de ordem (postagem antes da aprovação) | mantidas, porém **sinalizadas** por flag |
# MAGIC | `payment_installments = 0` | normalizado para 1 (pagamento à vista), com flag |
# MAGIC | Erro de grafia da fonte (`lenght`) | corrigido para `length` |
# MAGIC
# MAGIC **Regra adotada:** nada é silenciosamente jogado fora. Linha problemática que ainda tem valor
# MAGIC analítico permanece com uma coluna-bandeira (`flag_*`), para que a camada Gold decida
# MAGIC — de forma explícita e auditável — o que entra em cada métrica.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 `silver.geolocalizacao` — uma coordenada por CEP
# MAGIC
# MAGIC A Bronze traz 1.000.163 pontos de GPS para apenas 19.015 prefixos de CEP: é uma tabela de
# MAGIC *observações*, não de *localidades*. Para servir como dimensão geográfica ela precisa ter
# MAGIC uma linha por CEP. Usa-se a **mediana** (e não a média) das coordenadas porque a mediana não
# MAGIC se desloca com pontos absurdos, e a cidade/UF mais frequente como rótulo.

# COMMAND ----------

bronze_geo = spark.table(f"{BRONZE}.geolocalizacao")

geo_valida = (
    bronze_geo
    .select(
        F.col("geolocation_zip_code_prefix").cast("int").alias("cep_prefixo"),
        F.col("geolocation_lat").cast("double").alias("latitude"),
        F.col("geolocation_lng").cast("double").alias("longitude"),
        normaliza_cidade(F.col("geolocation_city")).alias("cidade"),
        F.upper(F.trim(F.col("geolocation_state"))).alias("uf"),
    )
    .filter(
        F.col("latitude").between(BR_LAT_MIN, BR_LAT_MAX)
        & F.col("longitude").between(BR_LNG_MIN, BR_LNG_MAX)
        & F.col("cep_prefixo").isNotNull()
    )
)

rotulo_mais_frequente = (
    geo_valida.groupBy("cep_prefixo", "cidade", "uf").count()
    .withColumn(
        "posicao",
        F.row_number().over(
            Window.partitionBy("cep_prefixo").orderBy(F.desc("count"), F.asc("cidade"))
        ),
    )
    .filter("posicao = 1")
    .select("cep_prefixo", "cidade", "uf")
)

silver_geo = (
    geo_valida.groupBy("cep_prefixo")
    .agg(
        F.round(F.percentile_approx("latitude", 0.5), 6).alias("latitude"),
        F.round(F.percentile_approx("longitude", 0.5), 6).alias("longitude"),
        F.count("*").alias("qtd_observacoes"),
    )
    .join(rotulo_mais_frequente, "cep_prefixo", "left")
    .withColumn("regiao", col_regiao(F.col("uf")))
    .select("cep_prefixo", "cidade", "uf", "regiao", "latitude", "longitude", "qtd_observacoes")
)

salva_tabela(
    silver_geo, f"{SILVER}.geolocalizacao",
    comentario="Silver - uma linha por prefixo de CEP com coordenada mediana, cidade/UF padronizadas",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 `silver.clientes` e `silver.vendedores`
# MAGIC
# MAGIC Atenção a um detalhe do modelo de origem: em Olist, `customer_id` é a chave **do pedido**
# MAGIC (muda a cada compra) e `customer_unique_id` é a chave **da pessoa**. Confundir as duas leva a
# MAGIC concluir que não existe cliente recorrente. As duas são preservadas.

# COMMAND ----------

silver_clientes = (
    spark.table(f"{BRONZE}.clientes")
    .select(
        F.col("customer_id").alias("cliente_id"),
        F.col("customer_unique_id").alias("cliente_unico_id"),
        F.col("customer_zip_code_prefix").cast("int").alias("cep_prefixo"),
        normaliza_cidade(F.col("customer_city")).alias("cidade"),
        F.upper(F.trim(F.col("customer_state"))).alias("uf"),
    )
    .withColumn("regiao", col_regiao(F.col("uf")))
)

salva_tabela(
    silver_clientes, f"{SILVER}.clientes",
    comentario="Silver - clientes com cidade padronizada e região IBGE derivada da UF",
)

silver_vendedores = (
    spark.table(f"{BRONZE}.vendedores")
    .select(
        F.col("seller_id").alias("vendedor_id"),
        F.col("seller_zip_code_prefix").cast("int").alias("cep_prefixo"),
        normaliza_cidade(F.col("seller_city")).alias("cidade"),
        F.upper(F.trim(F.col("seller_state"))).alias("uf"),
    )
    .withColumn("regiao", col_regiao(F.col("uf")))
    # 1 vendedor tem a cidade preenchida com um número; o CEP resolve o rótulo correto.
    .withColumn("flag_cidade_invalida", F.col("cidade").rlike("^[0-9]+$"))
)

salva_tabela(
    silver_vendedores, f"{SILVER}.vendedores",
    comentario="Silver - vendedores com cidade padronizada e região IBGE derivada da UF",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 `silver.produtos`
# MAGIC
# MAGIC Três correções: o erro de grafia da própria fonte (`lenght` → `length`), a categoria ausente
# MAGIC (610 produtos, que continuam vendendo e não podem sumir da análise) e a tradução faltante de
# MAGIC duas categorias (`pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`).

# COMMAND ----------

traducao = (
    spark.table(f"{BRONZE}.traducao_categoria")
    .select(
        F.trim(F.col("product_category_name")).alias("categoria"),
        F.trim(F.col("product_category_name_english")).alias("categoria_en"),
    )
)

silver_produtos = (
    spark.table(f"{BRONZE}.produtos")
    .select(
        F.col("product_id").alias("produto_id"),
        F.coalesce(
            F.when(F.trim(F.col("product_category_name")) != F.lit(""),
                   F.trim(F.col("product_category_name"))),
            F.lit("nao_informado"),
        ).alias("categoria"),
        F.col("product_name_lenght").cast("int").alias("tamanho_nome"),
        F.col("product_description_lenght").cast("int").alias("tamanho_descricao"),
        F.col("product_photos_qty").cast("int").alias("qtd_fotos"),
        F.col("product_weight_g").cast("double").alias("peso_g"),
        F.col("product_length_cm").cast("double").alias("comprimento_cm"),
        F.col("product_height_cm").cast("double").alias("altura_cm"),
        F.col("product_width_cm").cast("double").alias("largura_cm"),
    )
    .join(traducao, "categoria", "left")
    .withColumn("categoria_en", F.coalesce(F.col("categoria_en"), F.col("categoria")))
    .withColumn("volume_cm3", F.round(F.col("comprimento_cm") * F.col("altura_cm") * F.col("largura_cm"), 2))
    .withColumn("flag_categoria_ausente", F.col("categoria") == F.lit("nao_informado"))
    .withColumn("flag_dimensoes_ausentes", F.col("peso_g").isNull() | F.col("comprimento_cm").isNull())
)

salva_tabela(
    silver_produtos, f"{SILVER}.produtos",
    comentario="Silver - produtos tipados, com categoria traduzida e volume calculado",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 `silver.pedidos` — o coração do tratamento
# MAGIC
# MAGIC É aqui que nascem as métricas de prazo que sustentam as perguntas de negócio. O ciclo do
# MAGIC pedido tem quatro marcos, e cada intervalo entre eles é responsabilidade de um ator diferente:
# MAGIC
# MAGIC ```
# MAGIC compra ──► aprovação ──► postagem à transportadora ──► entrega ao cliente
# MAGIC        (pagamento)        (vendedor/manuseio)              (logística)
# MAGIC                                                   ▲
# MAGIC                                          prazo prometido ao cliente
# MAGIC ```
# MAGIC
# MAGIC `dias_atraso` é a diferença entre entrega real e prazo prometido: **negativo = adiantado**.

# COMMAND ----------

bronze_pedidos = spark.table(f"{BRONZE}.pedidos")

ts = lambda c: F.to_timestamp(F.col(c))

silver_pedidos = (
    bronze_pedidos
    .select(
        F.col("order_id").alias("pedido_id"),
        F.col("customer_id").alias("cliente_id"),
        F.lower(F.trim(F.col("order_status"))).alias("status"),
        ts("order_purchase_timestamp").alias("dt_compra"),
        ts("order_approved_at").alias("dt_aprovacao"),
        ts("order_delivered_carrier_date").alias("dt_postagem"),
        ts("order_delivered_customer_date").alias("dt_entrega"),
        ts("order_estimated_delivery_date").alias("dt_prazo_estimado"),
    )
    .withColumn("data_compra", F.to_date("dt_compra"))
    .withColumn("ano_mes_compra", F.date_format("dt_compra", "yyyy-MM"))
    .withColumn("flag_entregue", F.col("status") == F.lit("delivered"))
    .withColumn("flag_cancelado", F.col("status").isin("canceled", "unavailable"))
    # horas/dias entre marcos do ciclo
    .withColumn("horas_ate_aprovacao",
                F.round((F.col("dt_aprovacao").cast("long") - F.col("dt_compra").cast("long")) / 3600.0, 2))
    .withColumn("dias_ate_postagem",
                F.round((F.col("dt_postagem").cast("long") - F.col("dt_aprovacao").cast("long")) / 86400.0, 2))
    .withColumn("dias_transporte",
                F.round((F.col("dt_entrega").cast("long") - F.col("dt_postagem").cast("long")) / 86400.0, 2))
    .withColumn("dias_ate_entrega",
                F.round((F.col("dt_entrega").cast("long") - F.col("dt_compra").cast("long")) / 86400.0, 2))
    .withColumn("dias_prazo_prometido",
                F.round((F.col("dt_prazo_estimado").cast("long") - F.col("dt_compra").cast("long")) / 86400.0, 2))
    .withColumn("dias_atraso",
                F.round((F.col("dt_entrega").cast("long") - F.col("dt_prazo_estimado").cast("long")) / 86400.0, 2))
    .withColumn("flag_atrasado", F.when(F.col("dias_atraso").isNotNull(), F.col("dias_atraso") > 0))
    # sinalizações de inconsistência detectadas na Bronze
    .withColumn("flag_entregue_sem_data", F.col("flag_entregue") & F.col("dt_entrega").isNull())
    .withColumn("flag_nao_entregue_com_data", (~F.col("flag_entregue")) & F.col("dt_entrega").isNotNull())
    .withColumn("flag_sequencia_datas_invalida",
                (F.col("dt_postagem") < F.col("dt_aprovacao")) | (F.col("dt_entrega") < F.col("dt_postagem")))
)

salva_tabela(
    silver_pedidos, f"{SILVER}.pedidos",
    comentario="Silver - pedidos tipados com métricas de ciclo de entrega e sinalização de inconsistências",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 `silver.itens_pedido` e `silver.pagamentos`

# COMMAND ----------

silver_itens = (
    spark.table(f"{BRONZE}.itens_pedido")
    .select(
        F.col("order_id").alias("pedido_id"),
        F.col("order_item_id").cast("int").alias("item_seq"),
        F.col("product_id").alias("produto_id"),
        F.col("seller_id").alias("vendedor_id"),
        F.to_timestamp(F.col("shipping_limit_date")).alias("dt_limite_postagem"),
        F.col("price").cast("decimal(10,2)").alias("valor_produto"),
        F.col("freight_value").cast("decimal(10,2)").alias("valor_frete"),
    )
    .withColumn("valor_item", F.col("valor_produto") + F.col("valor_frete"))
    .withColumn("flag_frete_gratis", F.col("valor_frete") == 0)
)

salva_tabela(
    silver_itens, f"{SILVER}.itens_pedido",
    comentario="Silver - itens de pedido tipados, com valor total do item (produto + frete)",
)

silver_pagamentos = (
    spark.table(f"{BRONZE}.pagamentos")
    .select(
        F.col("order_id").alias("pedido_id"),
        F.col("payment_sequential").cast("int").alias("pagamento_seq"),
        F.lower(F.trim(F.col("payment_type"))).alias("meio_pagamento"),
        F.col("payment_installments").cast("int").alias("parcelas_origem"),
        F.col("payment_value").cast("decimal(10,2)").alias("valor_pago"),
    )
    # 0 parcelas não existe: a leitura correta é pagamento à vista.
    .withColumn("parcelas", F.when(F.col("parcelas_origem") <= 0, F.lit(1)).otherwise(F.col("parcelas_origem")))
    .withColumn("flag_parcelas_corrigidas", F.col("parcelas_origem") <= 0)
    .withColumn("flag_meio_indefinido", F.col("meio_pagamento") == F.lit("not_defined"))
    .withColumn("flag_valor_nulo", F.col("valor_pago") <= 0)
    .drop("parcelas_origem")
)

salva_tabela(
    silver_pagamentos, f"{SILVER}.pagamentos",
    comentario="Silver - pagamentos tipados, com parcelas normalizadas e sinalização de registros indefinidos",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 `silver.avaliacoes` — deduplicação para um voto por pedido
# MAGIC
# MAGIC A Bronze tem 100.000 avaliações para 99.441 pedidos: 559 pedidos receberam mais de uma.
# MAGIC Como a pergunta de negócio é "qual a nota **daquele pedido**", o grão precisa ser o pedido.
# MAGIC Critério de desempate: fica a avaliação **respondida por último**, que é a manifestação mais
# MAGIC recente do cliente; `review_id` serve de critério final para tornar o resultado determinístico.

# COMMAND ----------

base_avaliacoes = (
    spark.table(f"{BRONZE}.avaliacoes")
    .select(
        F.col("review_id").alias("avaliacao_id"),
        F.col("order_id").alias("pedido_id"),
        F.col("review_score").cast("int").alias("nota"),
        F.col("review_comment_title").alias("titulo_comentario"),
        F.col("review_comment_message").alias("comentario"),
        F.to_timestamp(F.col("review_creation_date")).alias("dt_envio_pesquisa"),
        F.to_timestamp(F.col("review_answer_timestamp")).alias("dt_resposta"),
    )
)

janela = Window.partitionBy("pedido_id").orderBy(F.desc("dt_resposta"), F.desc("avaliacao_id"))

silver_avaliacoes = (
    base_avaliacoes
    .withColumn("posicao", F.row_number().over(janela))
    .withColumn("qtd_avaliacoes_pedido", F.count("*").over(Window.partitionBy("pedido_id")))
    .filter("posicao = 1")
    .drop("posicao")
    .withColumn("flag_pedido_reavaliado", F.col("qtd_avaliacoes_pedido") > 1)
    .withColumn("tem_comentario",
                F.col("comentario").isNotNull() & (F.trim(F.col("comentario")) != F.lit("")))
    .withColumn("tamanho_comentario", F.length(F.trim(F.col("comentario"))))
    .withColumn("faixa_nota", F.when(F.col("nota") <= 2, "Detrator")
                               .when(F.col("nota") == 3, "Neutro")
                               .otherwise("Promotor"))
)

salva_tabela(
    silver_avaliacoes, f"{SILVER}.avaliacoes",
    comentario="Silver - uma avaliação por pedido (a mais recente), com classificação Detrator/Neutro/Promotor",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência: o grão ficou correto?

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'avaliacoes por pedido (max)'  AS verificacao,
           max(qtd)                       AS valor
    FROM (SELECT pedido_id, count(*) AS qtd FROM {SILVER}.avaliacoes GROUP BY pedido_id)
    UNION ALL SELECT 'pedidos distintos em silver.pedidos', count(DISTINCT pedido_id) FROM {SILVER}.pedidos
    UNION ALL SELECT 'pedidos com avaliacao',               count(*)                  FROM {SILVER}.avaliacoes
    UNION ALL SELECT 'ceps distintos em geolocalizacao',    count(*)                  FROM {SILVER}.geolocalizacao
""")) 

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {SILVER}"))
