# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Análise — respondendo às perguntas de negócio
# MAGIC
# MAGIC Todas as consultas rodam **exclusivamente sobre a camada Gold**. Nenhuma faz join com Bronze
# MAGIC ou Silver: se o modelo dimensional foi bem construído, ele basta. É esse o teste final do
# MAGIC pipeline.
# MAGIC
# MAGIC **Recorte padrão:** perguntas de prazo e satisfação usam apenas pedidos com status
# MAGIC `delivered`, data de entrega registrada e nota de avaliação. Perguntas de receita usam todos
# MAGIC os itens vendidos. Cada consulta declara seu filtro.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

def responde(titulo, consulta, linhas=40):
    print("\n" + "═" * 100)
    print(titulo)
    print("═" * 100)
    df = spark.sql(consulta)
    display(df, linhas)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · O atraso derruba a nota? Em que intensidade?

# COMMAND ----------

responde("P1.a — Nota média e proporção de detratores por faixa de atraso", f"""
SELECT faixa_atraso,
       count(*)                                                              AS pedidos,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)                    AS pct_pedidos,
       round(avg(nota), 2)                                                   AS nota_media,
       round(100.0 * sum(CASE WHEN nota <= 2 THEN 1 ELSE 0 END) / count(*), 1) AS pct_detratores,
       round(100.0 * sum(CASE WHEN nota  = 5 THEN 1 ELSE 0 END) / count(*), 1) AS pct_nota_5
  FROM {GOLD}.fato_pedido
 WHERE flag_entregue AND nota IS NOT NULL AND dias_atraso IS NOT NULL
 GROUP BY faixa_atraso
 ORDER BY CASE faixa_atraso
            WHEN 'mais de 10 dias adiantado' THEN 1 WHEN 'adiantado' THEN 2
            WHEN 'no prazo exato' THEN 3 WHEN 'ate 7 dias de atraso' THEN 4
            ELSE 5 END
""")

# COMMAND ----------

responde("P1.b — Pontual x atrasado, e a correlação entre dias de atraso e nota", f"""
SELECT CASE WHEN dias_atraso > 0 THEN 'entregue com atraso' ELSE 'entregue no prazo' END AS situacao,
       count(*)                                                                AS pedidos,
       round(avg(nota), 2)                                                     AS nota_media,
       round(100.0 * sum(CASE WHEN nota <= 2 THEN 1 ELSE 0 END) / count(*), 1) AS pct_detratores,
       round(avg(dias_atraso), 1)                                              AS media_dias_atraso
  FROM {GOLD}.fato_pedido
 WHERE flag_entregue AND nota IS NOT NULL AND dias_atraso IS NOT NULL
 GROUP BY 1 ORDER BY 1
""")

responde("P1.c — Correlação de Pearson entre atraso (dias) e nota (1-5)", f"""
SELECT round(corr(dias_atraso, nota), 3)      AS corr_atraso_nota,
       round(corr(dias_ate_entrega, nota), 3) AS corr_tempo_entrega_nota,
       round(corr(distancia_km, nota), 3)     AS corr_distancia_nota,
       count(*)                               AS base
  FROM {GOLD}.fato_pedido
 WHERE flag_entregue AND nota IS NOT NULL AND dias_atraso IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Em que etapa do ciclo o tempo é perdido?

# COMMAND ----------

responde("P2.a — Duração média de cada etapa do ciclo do pedido", f"""
SELECT 'todos os entregues' AS recorte,
       count(*)                            AS pedidos,
       round(avg(horas_ate_aprovacao), 1)  AS horas_ate_aprovacao,
       round(avg(dias_ate_postagem), 2)    AS dias_aprovacao_ate_postagem,
       round(avg(dias_transporte), 2)      AS dias_em_transporte,
       round(avg(dias_ate_entrega), 2)     AS dias_totais_ate_entrega
  FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NOT NULL
UNION ALL
SELECT CASE WHEN dias_atraso > 0 THEN 'entregues com atraso' ELSE 'entregues no prazo' END,
       count(*), round(avg(horas_ate_aprovacao), 1), round(avg(dias_ate_postagem), 2),
       round(avg(dias_transporte), 2), round(avg(dias_ate_entrega), 2)
  FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NOT NULL
 GROUP BY 1
 ORDER BY 1
""")

responde("P2.b — Mediana e cauda de cada etapa (percentis)", f"""
SELECT round(percentile_approx(dias_ate_postagem, 0.5), 2)  AS mediana_ate_postagem,
       round(percentile_approx(dias_ate_postagem, 0.9), 2)  AS p90_ate_postagem,
       round(percentile_approx(dias_transporte,   0.5), 2)  AS mediana_transporte,
       round(percentile_approx(dias_transporte,   0.9), 2)  AS p90_transporte,
       round(percentile_approx(dias_ate_entrega,  0.5), 2)  AS mediana_total,
       round(percentile_approx(dias_ate_entrega,  0.9), 2)  AS p90_total
  FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · O prazo prometido ao cliente é conservador?

# COMMAND ----------

responde("P3.a — Prazo prometido x prazo realizado", f"""
SELECT count(*)                                                                  AS pedidos,
       round(avg(dias_prazo_prometido), 1)                                       AS media_prazo_prometido,
       round(avg(dias_ate_entrega), 1)                                           AS media_entrega_real,
       round(avg(-dias_atraso), 1)                                               AS media_dias_de_folga,
       round(percentile_approx(-dias_atraso, 0.5), 1)                            AS mediana_dias_de_folga,
       round(100.0 * sum(CASE WHEN dias_atraso <= 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_dentro_do_prazo
  FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_atraso IS NOT NULL
""")

responde("P3.b — A folga do prazo prometido varia por região do cliente?", f"""
SELECT c.regiao,
       count(*)                                                                    AS pedidos,
       round(avg(f.dias_prazo_prometido), 1)                                       AS prazo_prometido,
       round(avg(f.dias_ate_entrega), 1)                                           AS entrega_real,
       round(avg(-f.dias_atraso), 1)                                               AS folga_media_dias,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados
  FROM {GOLD}.fato_pedido f
  JOIN {GOLD}.dim_cliente c ON f.cliente_id = c.cliente_id
 WHERE f.flag_entregue AND f.dias_atraso IS NOT NULL AND c.regiao IS NOT NULL
 GROUP BY c.regiao ORDER BY folga_media_dias DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · O desempenho é igual em todo o país?

# COMMAND ----------

responde("P4.a — Entrega e satisfação por região do cliente", f"""
SELECT c.regiao,
       count(*)                                                                     AS pedidos,
       round(avg(f.dias_ate_entrega), 1)                                            AS dias_entrega,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados,
       round(avg(f.nota), 2)                                                        AS nota_media,
       round(avg(f.valor_frete), 2)                                                 AS frete_medio,
       round(avg(f.distancia_km), 0)                                                AS distancia_media_km
  FROM {GOLD}.fato_pedido f
  JOIN {GOLD}.dim_cliente c ON f.cliente_id = c.cliente_id
 WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL AND c.regiao IS NOT NULL
 GROUP BY c.regiao ORDER BY pct_atrasados DESC
""")

responde("P4.b — As 10 UFs com pior e melhor desempenho de prazo", f"""
WITH base AS (
  SELECT c.uf, c.regiao,
         count(*)                                                                     AS pedidos,
         round(avg(f.dias_ate_entrega), 1)                                            AS dias_entrega,
         round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados,
         round(avg(f.nota), 2)                                                        AS nota_media,
         round(avg(f.valor_frete), 2)                                                 AS frete_medio
    FROM {GOLD}.fato_pedido f
    JOIN {GOLD}.dim_cliente c ON f.cliente_id = c.cliente_id
   WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL AND c.uf IS NOT NULL
   GROUP BY c.uf, c.regiao HAVING count(*) >= 100
)
SELECT * FROM (
  SELECT 'pior prazo' AS grupo, * FROM base ORDER BY dias_entrega DESC LIMIT 10
) UNION ALL SELECT * FROM (
  SELECT 'melhor prazo' AS grupo, * FROM base ORDER BY dias_entrega ASC LIMIT 10
) ORDER BY grupo DESC, dias_entrega DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · O que explica o custo do frete?

# COMMAND ----------

responde("P5.a — Participação do frete no valor do pedido, por região do cliente", f"""
SELECT regiao_cliente,
       count(*)                                        AS itens,
       round(avg(valor_produto), 2)                    AS produto_medio,
       round(avg(valor_frete), 2)                      AS frete_medio,
       round(100.0 * sum(valor_frete) / sum(valor_item), 1) AS pct_frete_no_total,
       round(avg(distancia_km), 0)                     AS distancia_media_km
  FROM {GOLD}.fato_item_pedido
 WHERE regiao_cliente IS NOT NULL
 GROUP BY regiao_cliente ORDER BY pct_frete_no_total DESC
""")

responde("P5.b — Frete por faixa de distância entre vendedor e comprador", f"""
SELECT CASE WHEN distancia_km <    50 THEN 'a) ate 50 km'
            WHEN distancia_km <   200 THEN 'b) 50 a 200 km'
            WHEN distancia_km <   500 THEN 'c) 200 a 500 km'
            WHEN distancia_km <  1000 THEN 'd) 500 a 1.000 km'
            WHEN distancia_km <  2000 THEN 'e) 1.000 a 2.000 km'
            ELSE                            'f) acima de 2.000 km' END AS faixa_distancia,
       count(*)                                             AS itens,
       round(avg(valor_frete), 2)                           AS frete_medio,
       round(100.0 * sum(valor_frete) / sum(valor_item), 1) AS pct_frete_no_total,
       round(avg(dias_atraso), 2)                           AS media_dias_atraso
  FROM {GOLD}.fato_item_pedido
 WHERE distancia_km IS NOT NULL
 GROUP BY 1 ORDER BY 1
""")

responde("P5.c — Correlação do frete com distância, peso e valor do produto", f"""
SELECT round(corr(i.valor_frete, i.distancia_km), 3) AS corr_frete_distancia,
       round(corr(i.valor_frete, p.peso_g), 3)       AS corr_frete_peso,
       round(corr(i.valor_frete, p.volume_cm3), 3)   AS corr_frete_volume,
       round(corr(i.valor_frete, i.valor_produto), 3) AS corr_frete_valor_produto,
       count(*)                                      AS base
  FROM {GOLD}.fato_item_pedido i
  JOIN {GOLD}.dim_produto p ON i.produto_id = p.produto_id
 WHERE i.distancia_km IS NOT NULL AND p.peso_g IS NOT NULL
""")

responde("P5.d — Frete interestadual x dentro do mesmo estado", f"""
SELECT CASE WHEN flag_interestadual THEN 'entre estados diferentes' ELSE 'mesmo estado' END AS tipo,
       count(*)                                             AS itens,
       round(avg(valor_frete), 2)                           AS frete_medio,
       round(100.0 * sum(valor_frete) / sum(valor_item), 1) AS pct_frete_no_total,
       round(avg(distancia_km), 0)                          AS distancia_media_km
  FROM {GOLD}.fato_item_pedido
 WHERE flag_interestadual IS NOT NULL
 GROUP BY 1 ORDER BY frete_medio DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 · Quais categorias sustentam a receita e quais corroem a satisfação?

# COMMAND ----------

responde("P6.a — As 15 maiores categorias por receita, com nota e pontualidade", f"""
SELECT p.categoria,
       count(*)                                                                      AS itens_vendidos,
       round(sum(i.valor_produto), 2)                                                AS receita,
       round(100.0 * sum(i.valor_produto) / sum(sum(i.valor_produto)) OVER (), 1)    AS pct_receita,
       round(avg(i.valor_produto), 2)                                                AS ticket_medio_item,
       round(100.0 * sum(i.valor_frete) / sum(i.valor_item), 1)                      AS pct_frete,
       round(avg(f.nota), 2)                                                         AS nota_media,
       round(100.0 * sum(CASE WHEN i.flag_atrasado THEN 1 ELSE 0 END) / count(*), 1) AS pct_atrasados
  FROM {GOLD}.fato_item_pedido i
  JOIN {GOLD}.dim_produto p ON i.produto_id = p.produto_id
  JOIN {GOLD}.fato_pedido  f ON i.pedido_id = f.pedido_id
 GROUP BY p.categoria
 ORDER BY receita DESC LIMIT 15
""")

responde("P6.b — Categorias relevantes com a pior nota média (mínimo 500 itens)", f"""
SELECT p.categoria,
       count(*)                                                                      AS itens_vendidos,
       round(sum(i.valor_produto), 2)                                                AS receita,
       round(avg(f.nota), 2)                                                         AS nota_media,
       round(100.0 * sum(CASE WHEN f.nota <= 2 THEN 1 ELSE 0 END) / count(*), 1)     AS pct_detratores,
       round(100.0 * sum(CASE WHEN i.flag_atrasado THEN 1 ELSE 0 END) / count(*), 1) AS pct_atrasados,
       round(avg(i.distancia_km), 0)                                                 AS distancia_media_km
  FROM {GOLD}.fato_item_pedido i
  JOIN {GOLD}.dim_produto p ON i.produto_id = p.produto_id
  JOIN {GOLD}.fato_pedido  f ON i.pedido_id = f.pedido_id
 WHERE f.nota IS NOT NULL
 GROUP BY p.categoria HAVING count(*) >= 500
 ORDER BY nota_media ASC LIMIT 10
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P7 · Quão concentrada é a receita entre os vendedores?

# COMMAND ----------

responde("P7.a — Curva de Pareto: quantos vendedores fazem a receita", f"""
WITH por_vendedor AS (
  SELECT vendedor_id, sum(valor_produto) AS receita
    FROM {GOLD}.fato_item_pedido GROUP BY vendedor_id
), acumulado AS (
  SELECT vendedor_id, receita,
         row_number() OVER (ORDER BY receita DESC)                     AS posicao,
         sum(receita) OVER (ORDER BY receita DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receita_acumulada,
         sum(receita) OVER ()                                          AS receita_total,
         count(*)     OVER ()                                          AS total_vendedores
    FROM por_vendedor
)
SELECT faixa,
       max(posicao)                                              AS vendedores_ate_a_faixa,
       round(100.0 * max(posicao) / max(total_vendedores), 1)    AS pct_dos_vendedores
  FROM (
    SELECT *, CASE WHEN receita_acumulada / receita_total <= 0.50 THEN '50% da receita'
                   WHEN receita_acumulada / receita_total <= 0.80 THEN '80% da receita'
                   WHEN receita_acumulada / receita_total <= 0.90 THEN '90% da receita'
                   ELSE '100% da receita' END AS faixa
      FROM acumulado
  ) GROUP BY faixa ORDER BY vendedores_ate_a_faixa
""")

responde("P7.b — Os 10 maiores vendedores: receita, prazo e nota", f"""
SELECT i.vendedor_id, v.uf, v.cidade,
       count(DISTINCT i.pedido_id)                                                   AS pedidos,
       round(sum(i.valor_produto), 2)                                                AS receita,
       round(avg(f.nota), 2)                                                         AS nota_media,
       round(100.0 * sum(CASE WHEN i.flag_atrasado THEN 1 ELSE 0 END) / count(*), 1) AS pct_atrasados,
       round(avg(f.dias_ate_postagem), 2)                                            AS dias_ate_postagem
  FROM {GOLD}.fato_item_pedido i
  JOIN {GOLD}.dim_vendedor v ON i.vendedor_id = v.vendedor_id
  JOIN {GOLD}.fato_pedido  f ON i.pedido_id  = f.pedido_id
 GROUP BY i.vendedor_id, v.uf, v.cidade
 ORDER BY receita DESC LIMIT 10
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P8 · Meio de pagamento e parcelamento

# COMMAND ----------

responde("P8.a — Mix de meios de pagamento e ticket médio", f"""
SELECT meio_pagamento_principal                                        AS meio_pagamento,
       count(*)                                                        AS pedidos,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)              AS pct_pedidos,
       round(avg(valor_total), 2)                                      AS ticket_medio,
       round(avg(parcelas), 2)                                         AS parcelas_medias,
       round(avg(nota), 2)                                             AS nota_media
  FROM {GOLD}.fato_pedido
 WHERE meio_pagamento_principal IS NOT NULL
 GROUP BY meio_pagamento_principal ORDER BY pedidos DESC
""")

responde("P8.b — Parcelamento x ticket médio", f"""
SELECT CASE WHEN parcelas = 1 THEN 'a) a vista'
            WHEN parcelas <= 3 THEN 'b) 2 a 3x'
            WHEN parcelas <= 6 THEN 'c) 4 a 6x'
            WHEN parcelas <= 10 THEN 'd) 7 a 10x'
            ELSE 'e) 11x ou mais' END       AS faixa_parcelamento,
       count(*)                             AS pedidos,
       round(avg(valor_total), 2)           AS ticket_medio,
       round(avg(nota), 2)                  AS nota_media
  FROM {GOLD}.fato_pedido WHERE parcelas IS NOT NULL
 GROUP BY 1 ORDER BY 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P9 · O atraso afasta o cliente? (pergunta parcialmente respondível)

# COMMAND ----------

responde("P9.a — Quanto de recompra existe na base", f"""
SELECT count(DISTINCT cliente_unico_id)                                     AS pessoas,
       sum(CASE WHEN qtd_pedidos_pessoa > 1 THEN 1 ELSE 0 END)              AS pessoas_recorrentes,
       round(100.0 * sum(CASE WHEN qtd_pedidos_pessoa > 1 THEN 1 ELSE 0 END)
             / count(DISTINCT cliente_unico_id), 2)                         AS pct_recorrentes
  FROM (SELECT DISTINCT cliente_unico_id, qtd_pedidos_pessoa FROM {GOLD}.dim_cliente)
""")

responde("P9.b — Taxa de recompra após um primeiro pedido pontual x atrasado", f"""
WITH pedidos_pessoa AS (
  SELECT c.cliente_unico_id, f.pedido_id, f.dt_compra_ordem, f.dias_atraso,
         row_number() OVER (PARTITION BY c.cliente_unico_id ORDER BY f.dt_compra_ordem) AS ordem,
         count(*)     OVER (PARTITION BY c.cliente_unico_id)                            AS total_pedidos
    FROM (SELECT pedido_id, cliente_id, dias_atraso, data_compra AS dt_compra_ordem
            FROM {GOLD}.fato_pedido WHERE flag_entregue AND dias_atraso IS NOT NULL) f
    JOIN {GOLD}.dim_cliente c ON f.cliente_id = c.cliente_id
)
SELECT CASE WHEN dias_atraso > 0 THEN 'primeiro pedido atrasou' ELSE 'primeiro pedido no prazo' END AS situacao,
       count(*)                                                                  AS clientes,
       sum(CASE WHEN total_pedidos > 1 THEN 1 ELSE 0 END)                        AS voltaram_a_comprar,
       round(100.0 * sum(CASE WHEN total_pedidos > 1 THEN 1 ELSE 0 END)/count(*), 2) AS taxa_recompra_pct
  FROM pedidos_pessoa WHERE ordem = 1
 GROUP BY 1 ORDER BY 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Série temporal de apoio (contexto para todas as perguntas)

# COMMAND ----------

responde("Evolução mensal: volume, prazo, atraso e nota", f"""
SELECT d.ano_mes,
       count(*)                                                                      AS pedidos,
       round(sum(f.valor_total), 2)                                                  AS receita,
       round(avg(f.dias_ate_entrega), 1)                                             AS dias_entrega,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados,
       round(avg(f.nota), 2)                                                         AS nota_media
  FROM {GOLD}.fato_pedido f
  JOIN {GOLD}.dim_data d ON f.data_compra_sk = d.data_sk
 WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL
 GROUP BY d.ano_mes ORDER BY d.ano_mes
""", linhas=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gráficos
# MAGIC
# MAGIC Quatro figuras que sintetizam as respostas. São salvas no Volume
# MAGIC `gold.relatorios` e reaproveitadas na documentação do repositório.
# MAGIC
# MAGIC Convenções adotadas: uma medida por painel (nunca dois eixos y no mesmo gráfico),
# MAGIC valor escrito diretamente na marca em vez de legenda, grade discreta e cor usada apenas
# MAGIC quando carrega significado — azul para a série, vermelho reservado para o estado "atrasado".

# COMMAND ----------

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DIR_IMG = f"{DIR_SAIDA}/img"
os.makedirs(DIR_IMG, exist_ok=True)

SUPERFICIE, TINTA, TINTA_2 = "#fcfcfb", "#0b0b0b", "#52514e"
AZUL, VERMELHO, GRADE = "#2a78d6", "#d03b3b", "#e5e4e0"

plt.rcParams.update({
    "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE,
    "savefig.facecolor": SUPERFICIE, "text.color": TINTA,
    "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
    "font.size": 9, "axes.titlesize": 10.5, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.titlepad": 10,
})


def limpa(ax, eixo_valor="x"):
    for lado in ("top", "right", "left", "bottom"):
        ax.spines[lado].set_visible(False)
    ax.grid(axis=eixo_valor, color=GRADE, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)


def barras_h(ax, rotulos, valores, cores, formato="{:.2f}", titulo="", folga=1.18):
    valores = [float(v) for v in valores]
    y = range(len(rotulos))
    ax.barh(y, valores, height=0.62, color=cores)
    ax.set_yticks(list(y), rotulos)
    ax.invert_yaxis()
    ax.set_xlim(0, max(valores) * folga)
    for i, v in enumerate(valores):
        ax.text(v + max(valores) * 0.02, i, formato.format(v), va="center", fontsize=8.5, color=TINTA)
    ax.set_title(titulo)
    limpa(ax)
    ax.set_xticklabels([])


def salva(fig, nome):
    caminho = f"{DIR_IMG}/{nome}"
    fig.savefig(caminho, dpi=160, bbox_inches="tight")
    print(f"  ✔ {caminho}")
    return caminho

# COMMAND ----------

# ---------------------------------------------------- G1: atraso x satisfação
dados = spark.sql(f"""
SELECT faixa_atraso, round(avg(nota), 2) AS nota_media,
       round(100.0 * sum(CASE WHEN nota <= 2 THEN 1 ELSE 0 END)/count(*), 1) AS pct_detratores
  FROM {GOLD}.fato_pedido
 WHERE flag_entregue AND nota IS NOT NULL AND dias_atraso IS NOT NULL
 GROUP BY faixa_atraso
 ORDER BY CASE faixa_atraso WHEN 'mais de 10 dias adiantado' THEN 1 WHEN 'adiantado' THEN 2
            WHEN 'no prazo exato' THEN 3 WHEN 'ate 7 dias de atraso' THEN 4 ELSE 5 END
""").toPandas()

rotulos = [r.replace("ate", "até").replace("mais de", "+") for r in dados.faixa_atraso]
cores = [VERMELHO if "atraso" in r else AZUL for r in dados.faixa_atraso]

fig, eixos = plt.subplots(1, 2, figsize=(11, 3.4))
barras_h(eixos[0], rotulos, list(dados.nota_media), cores, "{:.2f}", "Nota média da avaliação (1 a 5)")
barras_h(eixos[1], rotulos, list(dados.pct_detratores), cores, "{:.1f}%", "Pedidos com nota 1 ou 2")
eixos[1].set_yticklabels([])
fig.suptitle("P1 · O que o atraso faz com a avaliação do cliente", x=0.007, ha="left",
             fontsize=12, fontweight="bold")
fig.text(0.007, -0.06, "Base: 96.470 pedidos entregues com data de entrega e avaliação registradas.",
         fontsize=8, color=TINTA_2)
fig.tight_layout(rect=[0, 0, 1, 0.9])
salva(fig, "g1_atraso_satisfacao.png")
plt.show()

# COMMAND ----------

# ------------------------------------------------------------ G2: por região
dados = spark.sql(f"""
SELECT c.regiao, round(avg(f.dias_ate_entrega), 1) AS dias_entrega,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados
  FROM {GOLD}.fato_pedido f JOIN {GOLD}.dim_cliente c ON f.cliente_id = c.cliente_id
 WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL AND c.regiao IS NOT NULL
 GROUP BY c.regiao ORDER BY dias_entrega DESC
""").toPandas()

fig, eixos = plt.subplots(1, 2, figsize=(11, 3.1))
barras_h(eixos[0], list(dados.regiao), list(dados.dias_entrega), [AZUL] * len(dados),
         "{:.1f} dias", "Tempo médio entre a compra e a entrega")
barras_h(eixos[1], list(dados.regiao), list(dados.pct_atrasados), [VERMELHO] * len(dados),
         "{:.1f}%", "Pedidos entregues depois do prazo prometido")
eixos[1].set_yticklabels([])
fig.suptitle("P4 · O mesmo marketplace entrega de formas diferentes pelo país", x=0.007,
             ha="left", fontsize=12, fontweight="bold")
fig.tight_layout(rect=[0, 0, 1, 0.9])
salva(fig, "g2_desempenho_regiao.png")
plt.show()

# COMMAND ----------

# ------------------------------------------------- G3: frete x distância
dados = spark.sql(f"""
SELECT CASE WHEN distancia_km <   50 THEN 'até 50 km'
            WHEN distancia_km <  200 THEN '50 a 200 km'
            WHEN distancia_km <  500 THEN '200 a 500 km'
            WHEN distancia_km < 1000 THEN '500 a 1.000 km'
            WHEN distancia_km < 2000 THEN '1.000 a 2.000 km'
            ELSE                          'acima de 2.000 km' END AS faixa,
       CASE WHEN distancia_km < 50 THEN 1 WHEN distancia_km < 200 THEN 2
            WHEN distancia_km < 500 THEN 3 WHEN distancia_km < 1000 THEN 4
            WHEN distancia_km < 2000 THEN 5 ELSE 6 END            AS ordem,
       round(avg(valor_frete), 2)                                 AS frete_medio,
       round(100.0 * sum(valor_frete) / sum(valor_item), 1)       AS pct_frete
  FROM {GOLD}.fato_item_pedido WHERE distancia_km IS NOT NULL
 GROUP BY 1, 2 ORDER BY ordem
""").toPandas()

fig, eixos = plt.subplots(1, 2, figsize=(11, 3.4))
barras_h(eixos[0], list(dados.faixa), list(dados.frete_medio), [AZUL] * len(dados),
         "R$ {:.2f}", "Frete médio por item")
barras_h(eixos[1], list(dados.faixa), list(dados.pct_frete), [AZUL] * len(dados),
         "{:.1f}%", "Quanto do que o cliente paga é frete")
eixos[1].set_yticklabels([])
fig.suptitle("P5 · Distância entre vendedor e comprador x custo do frete", x=0.007,
             ha="left", fontsize=12, fontweight="bold")
fig.tight_layout(rect=[0, 0, 1, 0.9])
salva(fig, "g3_frete_distancia.png")
plt.show()

# COMMAND ----------

# ----------------------------------------------------- G4: evolução mensal
dados = spark.sql(f"""
SELECT d.ano_mes, count(*) AS pedidos,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados,
       round(avg(f.nota), 2) AS nota_media
  FROM {GOLD}.fato_pedido f JOIN {GOLD}.dim_data d ON f.data_compra_sk = d.data_sk
 WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL
 GROUP BY d.ano_mes HAVING count(*) >= 100 ORDER BY d.ano_mes
""").toPandas()

fig, eixos = plt.subplots(2, 1, figsize=(11, 5.2), sharex=True)
eixos[0].plot(dados.ano_mes, dados.pct_atrasados, color=VERMELHO, linewidth=2,
              marker="o", markersize=4)
eixos[0].set_title("Pedidos entregues com atraso (% do mês)")
eixos[1].plot(dados.ano_mes, dados.nota_media, color=AZUL, linewidth=2, marker="o", markersize=4)
eixos[1].set_title("Nota média da avaliação (1 a 5)")
for ax in eixos:
    limpa(ax, eixo_valor="y")
eixos[1].set_ylim(3.5, 4.5)
plt.setp(eixos[1].get_xticklabels(), rotation=60, ha="right", fontsize=8)

pico = dados.loc[dados.pct_atrasados.idxmax()]
eixos[0].annotate(f"{pico.ano_mes}: {pico.pct_atrasados}%",
                  xy=(pico.ano_mes, pico.pct_atrasados), xytext=(-8, 10),
                  textcoords="offset points", fontsize=8.5, color=TINTA, ha="right")

fig.suptitle("Evolução mensal · quando a operação atrasa, a nota cai no mesmo mês",
             x=0.007, ha="left", fontsize=12, fontweight="bold")
fig.tight_layout(rect=[0, 0, 1, 0.94])
salva(fig, "g4_evolucao_mensal.png")
plt.show()
