# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Modelagem dimensional → camada **Gold**
# MAGIC
# MAGIC A Silver é fiel ao modelo transacional da Olist (9 tabelas normalizadas). Para responder às
# MAGIC perguntas de negócio sem um emaranhado de *joins* a cada consulta, a Gold reorganiza tudo em
# MAGIC um **esquema estrela com dois fatos** que compartilham as mesmas dimensões
# MAGIC (*conformed dimensions*):
# MAGIC
# MAGIC ```
# MAGIC                    dim_data ─────────────┬───────────── dim_cliente
# MAGIC                        │                 │                   │
# MAGIC                        ▼                 ▼                   ▼
# MAGIC   dim_produto ──► fato_item_pedido    fato_pedido  ◄──────────┘
# MAGIC        ▲          (grão: item)        (grão: pedido)
# MAGIC        │                 ▲
# MAGIC   dim_vendedor ──────────┘
# MAGIC ```
# MAGIC
# MAGIC **Por que dois fatos e não um?** Porque existem dois grãos legítimos e misturá-los produz
# MAGIC número errado. Preço e frete são atributos do *item*; prazo de entrega, nota da avaliação e
# MAGIC forma de pagamento são atributos do *pedido*. Somar o atraso em uma tabela de itens
# MAGIC contaria o mesmo atraso três vezes num pedido de três itens — e a média de nota por
# MAGIC categoria ficaria enviesada para pedidos grandes.
# MAGIC
# MAGIC **Sobre as chaves:** as dimensões usam a **chave natural** do negócio (`pedido_id`,
# MAGIC `produto_id`, …) em vez de chave substituta inteira. Em um Lakehouse não há ganho de
# MAGIC armazenamento relevante em trocar um hash de 32 caracteres por um inteiro, e manter a chave
# MAGIC de origem preserva a linhagem até a Bronze. A exceção é `dim_data`, que usa a chave
# MAGIC convencional `yyyyMMdd` como inteiro.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql.window import Window

pedidos = spark.table(f"{SILVER}.pedidos")
itens = spark.table(f"{SILVER}.itens_pedido")
pagamentos = spark.table(f"{SILVER}.pagamentos")
avaliacoes = spark.table(f"{SILVER}.avaliacoes")
clientes = spark.table(f"{SILVER}.clientes")
vendedores = spark.table(f"{SILVER}.vendedores")
produtos = spark.table(f"{SILVER}.produtos")
geo = spark.table(f"{SILVER}.geolocalizacao")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 `gold.dim_data`
# MAGIC
# MAGIC Calendário completo cobrindo o período do dataset. Ter uma dimensão de data (em vez de
# MAGIC extrair `month()` na hora da consulta) é o que permite agrupar por trimestre, dia da semana
# MAGIC ou fim de semana sem reescrever a lógica em cada análise — e revela meses sem venda, que um
# MAGIC `GROUP BY` sobre o fato esconderia.

# COMMAND ----------

limites = pedidos.agg(
    F.min(F.to_date("dt_compra")).alias("min_data"),
    F.greatest(F.max(F.to_date("dt_entrega")), F.max(F.to_date("dt_prazo_estimado"))).alias("max_data"),
).collect()[0]

dim_data = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{limites['min_data']}'), "
        f"to_date('{limites['max_data']}'), interval 1 day)) AS data"
    )
    .withColumn("data_sk", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("dia_semana_num", F.dayofweek("data"))
    .withColumn("nome_mes", F.element_at(
        F.array(*[F.lit(m) for m in ["janeiro", "fevereiro", "março", "abril", "maio", "junho",
                                     "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]]),
        F.col("mes")))
    .withColumn("nome_dia_semana", F.element_at(
        F.array(*[F.lit(d) for d in ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"]]),
        F.col("dia_semana_num")))
    .withColumn("flag_fim_de_semana", F.col("dia_semana_num").isin(1, 7))
    .select("data_sk", "data", "ano", "trimestre", "mes", "nome_mes", "ano_mes",
            "dia", "dia_semana_num", "nome_dia_semana", "flag_fim_de_semana")
)

salva_tabela(dim_data, f"{GOLD}.dim_data", comentario="Dimensão calendário diária cobrindo todo o período do dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 `gold.dim_cliente` e `gold.dim_vendedor`
# MAGIC
# MAGIC As duas dimensões recebem a coordenada do CEP vinda de `silver.geolocalizacao`. É esse
# MAGIC enriquecimento que torna possível medir a **distância real entre vendedor e comprador** no
# MAGIC fato de item — variável central para explicar o frete.
# MAGIC
# MAGIC `dim_cliente` também carrega a contagem de pedidos da *pessoa* (`cliente_unico_id`), o que
# MAGIC permite separar cliente novo de cliente recorrente sem recalcular isso em toda consulta.

# COMMAND ----------

pedidos_por_pessoa = (
    pedidos.join(clientes.select("cliente_id", "cliente_unico_id"), "cliente_id")
    .groupBy("cliente_unico_id")
    .agg(F.countDistinct("pedido_id").alias("qtd_pedidos_pessoa"))
)

dim_cliente = (
    clientes
    .join(geo.select(F.col("cep_prefixo"), F.col("latitude").alias("latitude"),
                     F.col("longitude").alias("longitude")), "cep_prefixo", "left")
    .join(pedidos_por_pessoa, "cliente_unico_id", "left")
    .withColumn("flag_cliente_recorrente", F.col("qtd_pedidos_pessoa") > 1)
    .withColumn("flag_sem_coordenada", F.col("latitude").isNull())
    .select("cliente_id", "cliente_unico_id", "cep_prefixo", "cidade", "uf", "regiao",
            "latitude", "longitude", "qtd_pedidos_pessoa",
            "flag_cliente_recorrente", "flag_sem_coordenada")
)

salva_tabela(dim_cliente, f"{GOLD}.dim_cliente",
             comentario="Dimensão cliente (grão: chave de compra), com geolocalização e marcação de recorrência")

dim_vendedor = (
    vendedores
    .join(geo.select("cep_prefixo", "latitude", "longitude"), "cep_prefixo", "left")
    .withColumn("flag_sem_coordenada", F.col("latitude").isNull())
    .select("vendedor_id", "cep_prefixo", "cidade", "uf", "regiao",
            "latitude", "longitude", "flag_sem_coordenada")
)

salva_tabela(dim_vendedor, f"{GOLD}.dim_vendedor",
             comentario="Dimensão vendedor com localização geográfica derivada do CEP")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 `gold.dim_produto`
# MAGIC
# MAGIC A cauda de 74 categorias é longa demais para um gráfico legível: as 15 maiores por receita
# MAGIC recebem nome próprio e o restante é agrupado em `outras`. A categoria original continua na
# MAGIC tabela — o agrupamento é uma conveniência de leitura, não uma perda de informação.

# COMMAND ----------

receita_por_categoria = (
    itens.join(produtos.select("produto_id", "categoria"), "produto_id")
    .groupBy("categoria").agg(F.sum("valor_produto").alias("receita"))
)
top_categorias = [r["categoria"] for r in receita_por_categoria.orderBy(F.desc("receita")).limit(15).collect()]

dim_produto = (
    produtos
    .withColumn("categoria_agrupada",
                F.when(F.col("categoria").isin(top_categorias), F.col("categoria")).otherwise(F.lit("outras")))
    .withColumn("faixa_peso",
                F.when(F.col("peso_g").isNull(), "nao informado")
                 .when(F.col("peso_g") <= 500, "ate 500g")
                 .when(F.col("peso_g") <= 2000, "501g a 2kg")
                 .when(F.col("peso_g") <= 10000, "2kg a 10kg")
                 .otherwise("acima de 10kg"))
    .select("produto_id", "categoria", "categoria_en", "categoria_agrupada",
            "peso_g", "comprimento_cm", "altura_cm", "largura_cm", "volume_cm3", "faixa_peso",
            "qtd_fotos", "tamanho_nome", "tamanho_descricao",
            "flag_categoria_ausente", "flag_dimensoes_ausentes")
)

salva_tabela(dim_produto, f"{GOLD}.dim_produto",
             comentario="Dimensão produto com categoria traduzida, agrupamento das 15 maiores e faixa de peso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 `gold.fato_pedido` — grão: **um pedido**
# MAGIC
# MAGIC Consolida três fontes que o modelo transacional mantém separadas: os itens (valores), os
# MAGIC pagamentos (como foi pago) e a avaliação (qual foi a nota). O meio de pagamento principal é
# MAGIC o de **maior valor** no pedido, já que um mesmo pedido pode combinar vouchers e cartão.

# COMMAND ----------

agg_itens = (
    itens.groupBy("pedido_id").agg(
        F.count("*").alias("qtd_itens"),
        F.countDistinct("produto_id").alias("qtd_produtos_distintos"),
        F.countDistinct("vendedor_id").alias("qtd_vendedores"),
        F.sum("valor_produto").cast("decimal(12,2)").alias("valor_produtos"),
        F.sum("valor_frete").cast("decimal(12,2)").alias("valor_frete"),
        F.sum("valor_item").cast("decimal(12,2)").alias("valor_total"),
    )
)

janela_pagamento = Window.partitionBy("pedido_id").orderBy(F.desc("valor_pago"), F.asc("meio_pagamento"))
meio_principal = (
    pagamentos.withColumn("posicao", F.row_number().over(janela_pagamento))
    .filter("posicao = 1")
    .select("pedido_id", F.col("meio_pagamento").alias("meio_pagamento_principal"))
)

agg_pagamentos = (
    pagamentos.groupBy("pedido_id").agg(
        F.sum("valor_pago").cast("decimal(12,2)").alias("valor_pago"),
        F.max("parcelas").alias("parcelas"),
        F.countDistinct("meio_pagamento").alias("qtd_meios_pagamento"),
    ).join(meio_principal, "pedido_id", "left")
)

# distância média entre o(s) vendedor(es) e o cliente do pedido
distancia_pedido = (
    itens.select("pedido_id", "vendedor_id")
    .join(pedidos.select("pedido_id", "cliente_id"), "pedido_id")
    .join(dim_cliente.select("cliente_id", F.col("latitude").alias("lat_cli"),
                             F.col("longitude").alias("lng_cli")), "cliente_id")
    .join(dim_vendedor.select("vendedor_id", F.col("latitude").alias("lat_ven"),
                              F.col("longitude").alias("lng_ven")), "vendedor_id")
    .withColumn("distancia_km", distancia_km(F.col("lat_ven"), F.col("lng_ven"),
                                             F.col("lat_cli"), F.col("lng_cli")))
    .groupBy("pedido_id").agg(F.round(F.avg("distancia_km"), 2).alias("distancia_km"))
)

fato_pedido = (
    pedidos
    .join(agg_itens, "pedido_id", "left")
    .join(agg_pagamentos, "pedido_id", "left")
    .join(distancia_pedido, "pedido_id", "left")
    .join(avaliacoes.select("pedido_id", "nota", "faixa_nota", "tem_comentario",
                            "tamanho_comentario", "flag_pedido_reavaliado"), "pedido_id", "left")
    .withColumn("data_compra_sk", F.date_format("dt_compra", "yyyyMMdd").cast("int"))
    .withColumn("data_entrega_sk", F.date_format("dt_entrega", "yyyyMMdd").cast("int"))
    .withColumn("pct_frete",
                F.when(F.col("valor_total") > 0,
                       F.round(100 * F.col("valor_frete") / F.col("valor_total"), 2)))
    .withColumn("faixa_atraso",
                F.when(F.col("dias_atraso").isNull(), "sem entrega registrada")
                 .when(F.col("dias_atraso") <= -10, "mais de 10 dias adiantado")
                 .when(F.col("dias_atraso") < 0, "adiantado")
                 .when(F.col("dias_atraso") == 0, "no prazo exato")
                 .when(F.col("dias_atraso") <= 7, "ate 7 dias de atraso")
                 .otherwise("mais de 7 dias de atraso"))
    .select(
        # chaves
        "pedido_id", "cliente_id", "data_compra_sk", "data_entrega_sk",
        # atributos degenerados
        "status", "data_compra", "ano_mes_compra",
        "flag_entregue", "flag_cancelado", "flag_atrasado", "faixa_atraso",
        # medidas de valor
        "qtd_itens", "qtd_produtos_distintos", "qtd_vendedores",
        "valor_produtos", "valor_frete", "valor_total", "pct_frete",
        "valor_pago", "parcelas", "qtd_meios_pagamento", "meio_pagamento_principal",
        # medidas de prazo
        "horas_ate_aprovacao", "dias_ate_postagem", "dias_transporte",
        "dias_ate_entrega", "dias_prazo_prometido", "dias_atraso", "distancia_km",
        # medidas de satisfação
        "nota", "faixa_nota", "tem_comentario", "tamanho_comentario",
        # sinalizações herdadas da Silver
        "flag_entregue_sem_data", "flag_nao_entregue_com_data",
        "flag_sequencia_datas_invalida", "flag_pedido_reavaliado",
    )
)

salva_tabela(fato_pedido, f"{GOLD}.fato_pedido",
             comentario="Fato no grão de pedido: valores, prazos do ciclo de entrega, pagamento e nota da avaliação")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 `gold.fato_item_pedido` — grão: **um item de um pedido**
# MAGIC
# MAGIC É o fato usado para tudo que é por produto, por vendedor ou por categoria: receita, frete
# MAGIC unitário e a distância entre quem vendeu e quem comprou.

# COMMAND ----------

fato_item = (
    itens
    .join(pedidos.select("pedido_id", "cliente_id", "dt_compra", "status", "flag_entregue",
                         "dias_atraso", "flag_atrasado"), "pedido_id")
    .join(dim_cliente.select("cliente_id", F.col("latitude").alias("lat_cli"),
                             F.col("longitude").alias("lng_cli"),
                             F.col("regiao").alias("regiao_cliente"),
                             F.col("uf").alias("uf_cliente")), "cliente_id", "left")
    .join(dim_vendedor.select("vendedor_id", F.col("latitude").alias("lat_ven"),
                              F.col("longitude").alias("lng_ven"),
                              F.col("regiao").alias("regiao_vendedor"),
                              F.col("uf").alias("uf_vendedor")), "vendedor_id", "left")
    .withColumn("distancia_km", distancia_km(F.col("lat_ven"), F.col("lng_ven"),
                                             F.col("lat_cli"), F.col("lng_cli")))
    .withColumn("data_compra_sk", F.date_format("dt_compra", "yyyyMMdd").cast("int"))
    .withColumn("pct_frete",
                F.when(F.col("valor_item") > 0,
                       F.round(100 * F.col("valor_frete") / F.col("valor_item"), 2)))
    .withColumn("flag_interestadual", F.col("uf_cliente") != F.col("uf_vendedor"))
    .select("pedido_id", "item_seq", "produto_id", "vendedor_id", "cliente_id", "data_compra_sk",
            "dt_limite_postagem", "valor_produto", "valor_frete", "valor_item", "pct_frete",
            "distancia_km", "uf_cliente", "regiao_cliente", "uf_vendedor", "regiao_vendedor",
            "flag_interestadual", "status", "flag_entregue", "dias_atraso", "flag_atrasado")
)

salva_tabela(fato_item, f"{GOLD}.fato_item_pedido",
             comentario="Fato no grão de item de pedido: receita, frete, distância vendedor-cliente e recorte geográfico")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Chaves estrangeiras declaradas no Unity Catalog
# MAGIC
# MAGIC As constraints não são obrigatórias no Databricks e não são impostas em tempo de escrita,
# MAGIC mas documentam o relacionamento entre as tabelas e alimentam o diagrama de modelo do
# MAGIC Catalog Explorer — deixando o esquema estrela visível para quem nunca leu este notebook.

# COMMAND ----------

constraints = [
    f"ALTER TABLE {GOLD}.dim_data          ALTER COLUMN data_sk        SET NOT NULL",
    f"ALTER TABLE {GOLD}.dim_cliente       ALTER COLUMN cliente_id     SET NOT NULL",
    f"ALTER TABLE {GOLD}.dim_vendedor      ALTER COLUMN vendedor_id    SET NOT NULL",
    f"ALTER TABLE {GOLD}.dim_produto       ALTER COLUMN produto_id     SET NOT NULL",
    f"ALTER TABLE {GOLD}.fato_pedido       ALTER COLUMN pedido_id      SET NOT NULL",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ALTER COLUMN pedido_id      SET NOT NULL",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ALTER COLUMN item_seq       SET NOT NULL",
    f"ALTER TABLE {GOLD}.dim_data          ADD CONSTRAINT pk_dim_data     PRIMARY KEY (data_sk)",
    f"ALTER TABLE {GOLD}.dim_cliente       ADD CONSTRAINT pk_dim_cliente  PRIMARY KEY (cliente_id)",
    f"ALTER TABLE {GOLD}.dim_vendedor      ADD CONSTRAINT pk_dim_vendedor PRIMARY KEY (vendedor_id)",
    f"ALTER TABLE {GOLD}.dim_produto       ADD CONSTRAINT pk_dim_produto  PRIMARY KEY (produto_id)",
    f"ALTER TABLE {GOLD}.fato_pedido       ADD CONSTRAINT pk_fato_pedido  PRIMARY KEY (pedido_id)",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ADD CONSTRAINT pk_fato_item    PRIMARY KEY (pedido_id, item_seq)",
    f"ALTER TABLE {GOLD}.fato_pedido       ADD CONSTRAINT fk_pedido_cliente FOREIGN KEY (cliente_id) REFERENCES {GOLD}.dim_cliente",
    f"ALTER TABLE {GOLD}.fato_pedido       ADD CONSTRAINT fk_pedido_data    FOREIGN KEY (data_compra_sk) REFERENCES {GOLD}.dim_data",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ADD CONSTRAINT fk_item_pedido    FOREIGN KEY (pedido_id) REFERENCES {GOLD}.fato_pedido",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ADD CONSTRAINT fk_item_produto   FOREIGN KEY (produto_id) REFERENCES {GOLD}.dim_produto",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ADD CONSTRAINT fk_item_vendedor  FOREIGN KEY (vendedor_id) REFERENCES {GOLD}.dim_vendedor",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ADD CONSTRAINT fk_item_cliente   FOREIGN KEY (cliente_id) REFERENCES {GOLD}.dim_cliente",
    f"ALTER TABLE {GOLD}.fato_item_pedido  ADD CONSTRAINT fk_item_data      FOREIGN KEY (data_compra_sk) REFERENCES {GOLD}.dim_data",
]

for comando in constraints:
    try:
        spark.sql(comando)
        print(f"  ✔ {comando.split('ADD CONSTRAINT')[-1].split('ALTER COLUMN')[-1].strip()[:70]}")
    except Exception as erro:  # constraint já existente ou não suportada no ambiente
        print(f"  · ignorado ({type(erro).__name__}): {comando[:80]}")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {GOLD}"))
