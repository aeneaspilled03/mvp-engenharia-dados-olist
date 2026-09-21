# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Testes de qualidade **pós-carga** (Gold)
# MAGIC
# MAGIC O notebook 02 mediu a qualidade do dado que *entrou*. Este mede a qualidade do dado que
# MAGIC *saiu* — que é uma pergunta diferente: um pipeline pode partir de uma base limpa e ainda
# MAGIC assim produzir número errado, por um join que duplicou linhas ou um filtro que comeu registros.
# MAGIC
# MAGIC Três famílias de teste:
# MAGIC
# MAGIC 1. **Grão e unicidade** — cada fato tem a chave que promete ter?
# MAGIC 2. **Integridade referencial** — todo fato encontra sua dimensão?
# MAGIC 3. **Reconciliação Bronze → Gold** — a receita e a contagem sobreviveram intactas ao pipeline?
# MAGIC
# MAGIC O resultado vai para `gold.qualidade_pos_carga`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

TESTES = [
    # ------------------------------------------------------------ grão / unicidade
    ("Grão", "fato_pedido: uma linha por pedido",
     f"SELECT count(*) FROM (SELECT pedido_id FROM {GOLD}.fato_pedido GROUP BY pedido_id HAVING count(*) > 1)", 0),
    ("Grão", "fato_item_pedido: uma linha por (pedido, item)",
     f"SELECT count(*) FROM (SELECT pedido_id, item_seq FROM {GOLD}.fato_item_pedido GROUP BY 1,2 HAVING count(*) > 1)", 0),
    ("Grão", "dim_cliente: cliente_id único",
     f"SELECT count(*) FROM (SELECT cliente_id FROM {GOLD}.dim_cliente GROUP BY cliente_id HAVING count(*) > 1)", 0),
    ("Grão", "dim_produto: produto_id único",
     f"SELECT count(*) FROM (SELECT produto_id FROM {GOLD}.dim_produto GROUP BY produto_id HAVING count(*) > 1)", 0),
    ("Grão", "dim_vendedor: vendedor_id único",
     f"SELECT count(*) FROM (SELECT vendedor_id FROM {GOLD}.dim_vendedor GROUP BY vendedor_id HAVING count(*) > 1)", 0),
    ("Grão", "dim_data: data_sk único",
     f"SELECT count(*) FROM (SELECT data_sk FROM {GOLD}.dim_data GROUP BY data_sk HAVING count(*) > 1)", 0),

    # -------------------------------------------------------- integridade referencial
    ("Integridade", "fato_pedido → dim_cliente",
     f"SELECT count(*) FROM {GOLD}.fato_pedido f LEFT ANTI JOIN {GOLD}.dim_cliente d ON f.cliente_id = d.cliente_id", 0),
    ("Integridade", "fato_pedido → dim_data",
     f"SELECT count(*) FROM {GOLD}.fato_pedido f LEFT ANTI JOIN {GOLD}.dim_data d ON f.data_compra_sk = d.data_sk", 0),
    ("Integridade", "fato_item_pedido → dim_produto",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido f LEFT ANTI JOIN {GOLD}.dim_produto d ON f.produto_id = d.produto_id", 0),
    ("Integridade", "fato_item_pedido → dim_vendedor",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido f LEFT ANTI JOIN {GOLD}.dim_vendedor d ON f.vendedor_id = d.vendedor_id", 0),
    ("Integridade", "fato_item_pedido → fato_pedido",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido f LEFT ANTI JOIN {GOLD}.fato_pedido p ON f.pedido_id = p.pedido_id", 0),

    # --------------------------------------------------------------- regras de negócio
    ("Regra de negócio", "nota fora da escala 1..5",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE nota IS NOT NULL AND nota NOT BETWEEN 1 AND 5", 0),
    ("Regra de negócio", "valor de item negativo",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido WHERE valor_produto < 0 OR valor_frete < 0", 0),
    ("Regra de negócio", "pedido entregue sem data de entrega que não foi sinalizado",
     f"SELECT count(*) FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NULL AND NOT flag_entregue_sem_data", 0),
    ("Regra de negócio", "distância vendedor-cliente acima de 6.000 km",
     f"SELECT count(*) FROM {GOLD}.fato_item_pedido WHERE distancia_km > 6000", 0),
]

resultados = []
for familia, descricao, consulta, esperado in TESTES:
    valor = spark.sql(consulta).collect()[0][0] or 0
    resultados.append((familia, descricao, int(valor), int(esperado),
                       "PASSOU" if valor <= esperado else "FALHOU"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliação Bronze → Gold
# MAGIC
# MAGIC O teste mais importante do conjunto. Se a receita total da Bronze e a da Gold divergirem,
# MAGIC alguma transformação perdeu ou duplicou dinheiro — e nenhuma análise adiante vale nada.

# COMMAND ----------

reconciliacoes = [
    ("Reconciliação", "pedidos: contagem Bronze = contagem Gold",
     f"SELECT (SELECT count(*) FROM {BRONZE}.pedidos) - (SELECT count(*) FROM {GOLD}.fato_pedido)", 0),
    ("Reconciliação", "itens: contagem Bronze = contagem Gold",
     f"SELECT (SELECT count(*) FROM {BRONZE}.itens_pedido) - (SELECT count(*) FROM {GOLD}.fato_item_pedido)", 0),
    ("Reconciliação", "receita de produtos: diferença em centavos Bronze x Gold",
     f"""SELECT abs(round(
              (SELECT sum(double(price)) FROM {BRONZE}.itens_pedido)
            - (SELECT sum(valor_produto)  FROM {GOLD}.fato_item_pedido), 2) * 100)""", 1),
    ("Reconciliação", "valor de frete: diferença em centavos Bronze x Gold",
     f"""SELECT abs(round(
              (SELECT sum(double(freight_value)) FROM {BRONZE}.itens_pedido)
            - (SELECT sum(valor_frete)     FROM {GOLD}.fato_item_pedido), 2) * 100)""", 1),
    ("Reconciliação", "clientes: contagem Bronze = contagem dim_cliente",
     f"SELECT (SELECT count(*) FROM {BRONZE}.clientes) - (SELECT count(*) FROM {GOLD}.dim_cliente)", 0),
    ("Reconciliação", "avaliações: pedidos avaliados na Gold <= pedidos na Bronze",
     f"SELECT greatest(0, (SELECT count(*) FROM {GOLD}.fato_pedido WHERE nota IS NOT NULL) - (SELECT count(DISTINCT order_id) FROM {BRONZE}.avaliacoes))", 0),
]

for familia, descricao, consulta, esperado in reconciliacoes:
    valor = spark.sql(consulta).collect()[0][0] or 0
    resultados.append((familia, descricao, int(abs(valor)), int(esperado),
                       "PASSOU" if abs(valor) <= esperado else "FALHOU"))

df_testes = spark.createDataFrame(
    resultados, "familia string, teste string, resultado long, limite long, situacao string"
)

salva_tabela(df_testes, f"{GOLD}.qualidade_pos_carga",
             comentario="Testes de grão, integridade referencial, regras de negócio e reconciliação Bronze→Gold")

display(df_testes.orderBy(F.col("situacao").desc(), "familia"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Veredito

# COMMAND ----------

falhas = spark.table(f"{GOLD}.qualidade_pos_carga").filter("situacao = 'FALHOU'").count()
total = spark.table(f"{GOLD}.qualidade_pos_carga").count()
print(f"{total - falhas} de {total} testes passaram.")
if falhas:
    display(spark.table(f"{GOLD}.qualidade_pos_carga").filter("situacao = 'FALHOU'"))
else:
    print("Modelo Gold íntegro: grão, integridade referencial e reconciliação financeira OK.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cobertura dos dados que alimentam as análises
# MAGIC
# MAGIC Nem todo pedido serve para toda pergunta. Esta tabela deixa explícito **quanto** da base
# MAGIC sustenta cada recorte — é o que permite dizer, depois, se uma conclusão vale para o dataset
# MAGIC inteiro ou só para um pedaço dele.

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'pedidos totais'                                AS recorte, count(*) AS pedidos,
           round(100.0 * count(*) / (SELECT count(*) FROM {GOLD}.fato_pedido), 1) AS pct
      FROM {GOLD}.fato_pedido
    UNION ALL SELECT 'entregues', count(*), round(100.0*count(*)/(SELECT count(*) FROM {GOLD}.fato_pedido),1)
      FROM {GOLD}.fato_pedido WHERE flag_entregue
    UNION ALL SELECT 'entregues com data de entrega', count(*), round(100.0*count(*)/(SELECT count(*) FROM {GOLD}.fato_pedido),1)
      FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NOT NULL
    UNION ALL SELECT 'com nota de avaliação', count(*), round(100.0*count(*)/(SELECT count(*) FROM {GOLD}.fato_pedido),1)
      FROM {GOLD}.fato_pedido WHERE nota IS NOT NULL
    UNION ALL SELECT 'entregues + nota (base das perguntas de prazo x satisfação)', count(*), round(100.0*count(*)/(SELECT count(*) FROM {GOLD}.fato_pedido),1)
      FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_atraso IS NOT NULL AND nota IS NOT NULL
    UNION ALL SELECT 'com distância vendedor-cliente calculada', count(*), round(100.0*count(*)/(SELECT count(*) FROM {GOLD}.fato_pedido),1)
      FROM {GOLD}.fato_pedido WHERE distancia_km IS NOT NULL
    ORDER BY pedidos DESC
"""))
