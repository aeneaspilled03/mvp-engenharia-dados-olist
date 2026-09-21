-- =====================================================================
-- MVP · Engenharia de Dados — consultas de análise (camada Gold)
-- Gerado a partir do notebook notebooks/06_analise_perguntas.py
-- Executável diretamente no editor SQL do Databricks.
-- =====================================================================

-- ----------------------------------------------------------------------------
-- P1.a — Nota média e proporção de detratores por faixa de atraso
-- ----------------------------------------------------------------------------
SELECT faixa_atraso,
       count(*)                                                              AS pedidos,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)                    AS pct_pedidos,
       round(avg(nota), 2)                                                   AS nota_media,
       round(100.0 * sum(CASE WHEN nota <= 2 THEN 1 ELSE 0 END) / count(*), 1) AS pct_detratores,
       round(100.0 * sum(CASE WHEN nota  = 5 THEN 1 ELSE 0 END) / count(*), 1) AS pct_nota_5
  FROM olist_mvp.gold.fato_pedido
 WHERE flag_entregue AND nota IS NOT NULL AND dias_atraso IS NOT NULL
 GROUP BY faixa_atraso
 ORDER BY CASE faixa_atraso
            WHEN 'mais de 10 dias adiantado' THEN 1 WHEN 'adiantado' THEN 2
            WHEN 'no prazo exato' THEN 3 WHEN 'ate 7 dias de atraso' THEN 4
            ELSE 5 END;

-- ----------------------------------------------------------------------------
-- P1.b — Pontual x atrasado, e a correlação entre dias de atraso e nota
-- ----------------------------------------------------------------------------
SELECT CASE WHEN dias_atraso > 0 THEN 'entregue com atraso' ELSE 'entregue no prazo' END AS situacao,
       count(*)                                                                AS pedidos,
       round(avg(nota), 2)                                                     AS nota_media,
       round(100.0 * sum(CASE WHEN nota <= 2 THEN 1 ELSE 0 END) / count(*), 1) AS pct_detratores,
       round(avg(dias_atraso), 1)                                              AS media_dias_atraso
  FROM olist_mvp.gold.fato_pedido
 WHERE flag_entregue AND nota IS NOT NULL AND dias_atraso IS NOT NULL
 GROUP BY 1 ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P1.c — Correlação de Pearson entre atraso (dias) e nota (1-5)
-- ----------------------------------------------------------------------------
SELECT round(corr(dias_atraso, nota), 3)      AS corr_atraso_nota,
       round(corr(dias_ate_entrega, nota), 3) AS corr_tempo_entrega_nota,
       round(corr(distancia_km, nota), 3)     AS corr_distancia_nota,
       count(*)                               AS base
  FROM olist_mvp.gold.fato_pedido
 WHERE flag_entregue AND nota IS NOT NULL AND dias_atraso IS NOT NULL;

-- ----------------------------------------------------------------------------
-- P2.a — Duração média de cada etapa do ciclo do pedido
-- ----------------------------------------------------------------------------
SELECT 'todos os entregues' AS recorte,
       count(*)                            AS pedidos,
       round(avg(horas_ate_aprovacao), 1)  AS horas_ate_aprovacao,
       round(avg(dias_ate_postagem), 2)    AS dias_aprovacao_ate_postagem,
       round(avg(dias_transporte), 2)      AS dias_em_transporte,
       round(avg(dias_ate_entrega), 2)     AS dias_totais_ate_entrega
  FROM olist_mvp.gold.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NOT NULL
UNION ALL
SELECT CASE WHEN dias_atraso > 0 THEN 'entregues com atraso' ELSE 'entregues no prazo' END,
       count(*), round(avg(horas_ate_aprovacao), 1), round(avg(dias_ate_postagem), 2),
       round(avg(dias_transporte), 2), round(avg(dias_ate_entrega), 2)
  FROM olist_mvp.gold.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NOT NULL
 GROUP BY 1
 ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P2.b — Mediana e cauda de cada etapa (percentis)
-- ----------------------------------------------------------------------------
SELECT round(percentile_approx(dias_ate_postagem, 0.5), 2)  AS mediana_ate_postagem,
       round(percentile_approx(dias_ate_postagem, 0.9), 2)  AS p90_ate_postagem,
       round(percentile_approx(dias_transporte,   0.5), 2)  AS mediana_transporte,
       round(percentile_approx(dias_transporte,   0.9), 2)  AS p90_transporte,
       round(percentile_approx(dias_ate_entrega,  0.5), 2)  AS mediana_total,
       round(percentile_approx(dias_ate_entrega,  0.9), 2)  AS p90_total
  FROM olist_mvp.gold.fato_pedido WHERE flag_entregue AND dias_ate_entrega IS NOT NULL;

-- ----------------------------------------------------------------------------
-- P3.a — Prazo prometido x prazo realizado
-- ----------------------------------------------------------------------------
SELECT count(*)                                                                  AS pedidos,
       round(avg(dias_prazo_prometido), 1)                                       AS media_prazo_prometido,
       round(avg(dias_ate_entrega), 1)                                           AS media_entrega_real,
       round(avg(-dias_atraso), 1)                                               AS media_dias_de_folga,
       round(percentile_approx(-dias_atraso, 0.5), 1)                            AS mediana_dias_de_folga,
       round(100.0 * sum(CASE WHEN dias_atraso <= 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_dentro_do_prazo
  FROM olist_mvp.gold.fato_pedido WHERE flag_entregue AND dias_atraso IS NOT NULL;

-- ----------------------------------------------------------------------------
-- P3.b — A folga do prazo prometido varia por região do cliente?
-- ----------------------------------------------------------------------------
SELECT c.regiao,
       count(*)                                                                    AS pedidos,
       round(avg(f.dias_prazo_prometido), 1)                                       AS prazo_prometido,
       round(avg(f.dias_ate_entrega), 1)                                           AS entrega_real,
       round(avg(-f.dias_atraso), 1)                                               AS folga_media_dias,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados
  FROM olist_mvp.gold.fato_pedido f
  JOIN olist_mvp.gold.dim_cliente c ON f.cliente_id = c.cliente_id
 WHERE f.flag_entregue AND f.dias_atraso IS NOT NULL AND c.regiao IS NOT NULL
 GROUP BY c.regiao ORDER BY folga_media_dias DESC;

-- ----------------------------------------------------------------------------
-- P4.a — Entrega e satisfação por região do cliente
-- ----------------------------------------------------------------------------
SELECT c.regiao,
       count(*)                                                                     AS pedidos,
       round(avg(f.dias_ate_entrega), 1)                                            AS dias_entrega,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados,
       round(avg(f.nota), 2)                                                        AS nota_media,
       round(avg(f.valor_frete), 2)                                                 AS frete_medio,
       round(avg(f.distancia_km), 0)                                                AS distancia_media_km
  FROM olist_mvp.gold.fato_pedido f
  JOIN olist_mvp.gold.dim_cliente c ON f.cliente_id = c.cliente_id
 WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL AND c.regiao IS NOT NULL
 GROUP BY c.regiao ORDER BY pct_atrasados DESC;

-- ----------------------------------------------------------------------------
-- P4.b — As 10 UFs com pior e melhor desempenho de prazo
-- ----------------------------------------------------------------------------
WITH base AS (
  SELECT c.uf, c.regiao,
         count(*)                                                                     AS pedidos,
         round(avg(f.dias_ate_entrega), 1)                                            AS dias_entrega,
         round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados,
         round(avg(f.nota), 2)                                                        AS nota_media,
         round(avg(f.valor_frete), 2)                                                 AS frete_medio
    FROM olist_mvp.gold.fato_pedido f
    JOIN olist_mvp.gold.dim_cliente c ON f.cliente_id = c.cliente_id
   WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL AND c.uf IS NOT NULL
   GROUP BY c.uf, c.regiao HAVING count(*) >= 100
)
SELECT * FROM (
  SELECT 'pior prazo' AS grupo, * FROM base ORDER BY dias_entrega DESC LIMIT 10
) UNION ALL SELECT * FROM (
  SELECT 'melhor prazo' AS grupo, * FROM base ORDER BY dias_entrega ASC LIMIT 10
) ORDER BY grupo DESC, dias_entrega DESC;

-- ----------------------------------------------------------------------------
-- P5.a — Participação do frete no valor do pedido, por região do cliente
-- ----------------------------------------------------------------------------
SELECT regiao_cliente,
       count(*)                                        AS itens,
       round(avg(valor_produto), 2)                    AS produto_medio,
       round(avg(valor_frete), 2)                      AS frete_medio,
       round(100.0 * sum(valor_frete) / sum(valor_item), 1) AS pct_frete_no_total,
       round(avg(distancia_km), 0)                     AS distancia_media_km
  FROM olist_mvp.gold.fato_item_pedido
 WHERE regiao_cliente IS NOT NULL
 GROUP BY regiao_cliente ORDER BY pct_frete_no_total DESC;

-- ----------------------------------------------------------------------------
-- P5.b — Frete por faixa de distância entre vendedor e comprador
-- ----------------------------------------------------------------------------
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
  FROM olist_mvp.gold.fato_item_pedido
 WHERE distancia_km IS NOT NULL
 GROUP BY 1 ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P5.c — Correlação do frete com distância, peso e valor do produto
-- ----------------------------------------------------------------------------
SELECT round(corr(i.valor_frete, i.distancia_km), 3) AS corr_frete_distancia,
       round(corr(i.valor_frete, p.peso_g), 3)       AS corr_frete_peso,
       round(corr(i.valor_frete, p.volume_cm3), 3)   AS corr_frete_volume,
       round(corr(i.valor_frete, i.valor_produto), 3) AS corr_frete_valor_produto,
       count(*)                                      AS base
  FROM olist_mvp.gold.fato_item_pedido i
  JOIN olist_mvp.gold.dim_produto p ON i.produto_id = p.produto_id
 WHERE i.distancia_km IS NOT NULL AND p.peso_g IS NOT NULL;

-- ----------------------------------------------------------------------------
-- P5.d — Frete interestadual x dentro do mesmo estado
-- ----------------------------------------------------------------------------
SELECT CASE WHEN flag_interestadual THEN 'entre estados diferentes' ELSE 'mesmo estado' END AS tipo,
       count(*)                                             AS itens,
       round(avg(valor_frete), 2)                           AS frete_medio,
       round(100.0 * sum(valor_frete) / sum(valor_item), 1) AS pct_frete_no_total,
       round(avg(distancia_km), 0)                          AS distancia_media_km
  FROM olist_mvp.gold.fato_item_pedido
 WHERE flag_interestadual IS NOT NULL
 GROUP BY 1 ORDER BY frete_medio DESC;

-- ----------------------------------------------------------------------------
-- P6.a — As 15 maiores categorias por receita, com nota e pontualidade
-- ----------------------------------------------------------------------------
SELECT p.categoria,
       count(*)                                                                      AS itens_vendidos,
       round(sum(i.valor_produto), 2)                                                AS receita,
       round(100.0 * sum(i.valor_produto) / sum(sum(i.valor_produto)) OVER (), 1)    AS pct_receita,
       round(avg(i.valor_produto), 2)                                                AS ticket_medio_item,
       round(100.0 * sum(i.valor_frete) / sum(i.valor_item), 1)                      AS pct_frete,
       round(avg(f.nota), 2)                                                         AS nota_media,
       round(100.0 * sum(CASE WHEN i.flag_atrasado THEN 1 ELSE 0 END) / count(*), 1) AS pct_atrasados
  FROM olist_mvp.gold.fato_item_pedido i
  JOIN olist_mvp.gold.dim_produto p ON i.produto_id = p.produto_id
  JOIN olist_mvp.gold.fato_pedido  f ON i.pedido_id = f.pedido_id
 GROUP BY p.categoria
 ORDER BY receita DESC LIMIT 15;

-- ----------------------------------------------------------------------------
-- P6.b — Categorias relevantes com a pior nota média (mínimo 500 itens)
-- ----------------------------------------------------------------------------
SELECT p.categoria,
       count(*)                                                                      AS itens_vendidos,
       round(sum(i.valor_produto), 2)                                                AS receita,
       round(avg(f.nota), 2)                                                         AS nota_media,
       round(100.0 * sum(CASE WHEN f.nota <= 2 THEN 1 ELSE 0 END) / count(*), 1)     AS pct_detratores,
       round(100.0 * sum(CASE WHEN i.flag_atrasado THEN 1 ELSE 0 END) / count(*), 1) AS pct_atrasados,
       round(avg(i.distancia_km), 0)                                                 AS distancia_media_km
  FROM olist_mvp.gold.fato_item_pedido i
  JOIN olist_mvp.gold.dim_produto p ON i.produto_id = p.produto_id
  JOIN olist_mvp.gold.fato_pedido  f ON i.pedido_id = f.pedido_id
 WHERE f.nota IS NOT NULL
 GROUP BY p.categoria HAVING count(*) >= 500
 ORDER BY nota_media ASC LIMIT 10;

-- ----------------------------------------------------------------------------
-- P7.a — Curva de Pareto: quantos vendedores fazem a receita
-- ----------------------------------------------------------------------------
WITH por_vendedor AS (
  SELECT vendedor_id, sum(valor_produto) AS receita
    FROM olist_mvp.gold.fato_item_pedido GROUP BY vendedor_id
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
  ) GROUP BY faixa ORDER BY vendedores_ate_a_faixa;

-- ----------------------------------------------------------------------------
-- P7.b — Os 10 maiores vendedores: receita, prazo e nota
-- ----------------------------------------------------------------------------
SELECT i.vendedor_id, v.uf, v.cidade,
       count(DISTINCT i.pedido_id)                                                   AS pedidos,
       round(sum(i.valor_produto), 2)                                                AS receita,
       round(avg(f.nota), 2)                                                         AS nota_media,
       round(100.0 * sum(CASE WHEN i.flag_atrasado THEN 1 ELSE 0 END) / count(*), 1) AS pct_atrasados,
       round(avg(f.dias_ate_postagem), 2)                                            AS dias_ate_postagem
  FROM olist_mvp.gold.fato_item_pedido i
  JOIN olist_mvp.gold.dim_vendedor v ON i.vendedor_id = v.vendedor_id
  JOIN olist_mvp.gold.fato_pedido  f ON i.pedido_id  = f.pedido_id
 GROUP BY i.vendedor_id, v.uf, v.cidade
 ORDER BY receita DESC LIMIT 10;

-- ----------------------------------------------------------------------------
-- P8.a — Mix de meios de pagamento e ticket médio
-- ----------------------------------------------------------------------------
SELECT meio_pagamento_principal                                        AS meio_pagamento,
       count(*)                                                        AS pedidos,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)              AS pct_pedidos,
       round(avg(valor_total), 2)                                      AS ticket_medio,
       round(avg(parcelas), 2)                                         AS parcelas_medias,
       round(avg(nota), 2)                                             AS nota_media
  FROM olist_mvp.gold.fato_pedido
 WHERE meio_pagamento_principal IS NOT NULL
 GROUP BY meio_pagamento_principal ORDER BY pedidos DESC;

-- ----------------------------------------------------------------------------
-- P8.b — Parcelamento x ticket médio
-- ----------------------------------------------------------------------------
SELECT CASE WHEN parcelas = 1 THEN 'a) a vista'
            WHEN parcelas <= 3 THEN 'b) 2 a 3x'
            WHEN parcelas <= 6 THEN 'c) 4 a 6x'
            WHEN parcelas <= 10 THEN 'd) 7 a 10x'
            ELSE 'e) 11x ou mais' END       AS faixa_parcelamento,
       count(*)                             AS pedidos,
       round(avg(valor_total), 2)           AS ticket_medio,
       round(avg(nota), 2)                  AS nota_media
  FROM olist_mvp.gold.fato_pedido WHERE parcelas IS NOT NULL
 GROUP BY 1 ORDER BY 1;

-- ----------------------------------------------------------------------------
-- P9.a — Quanto de recompra existe na base
-- ----------------------------------------------------------------------------
SELECT count(DISTINCT cliente_unico_id)                                     AS pessoas,
       sum(CASE WHEN qtd_pedidos_pessoa > 1 THEN 1 ELSE 0 END)              AS pessoas_recorrentes,
       round(100.0 * sum(CASE WHEN qtd_pedidos_pessoa > 1 THEN 1 ELSE 0 END)
             / count(DISTINCT cliente_unico_id), 2)                         AS pct_recorrentes
  FROM (SELECT DISTINCT cliente_unico_id, qtd_pedidos_pessoa FROM olist_mvp.gold.dim_cliente);

-- ----------------------------------------------------------------------------
-- P9.b — Taxa de recompra após um primeiro pedido pontual x atrasado
-- ----------------------------------------------------------------------------
WITH pedidos_pessoa AS (
  SELECT c.cliente_unico_id, f.pedido_id, f.dt_compra_ordem, f.dias_atraso,
         row_number() OVER (PARTITION BY c.cliente_unico_id ORDER BY f.dt_compra_ordem) AS ordem,
         count(*)     OVER (PARTITION BY c.cliente_unico_id)                            AS total_pedidos
    FROM (SELECT pedido_id, cliente_id, dias_atraso, data_compra AS dt_compra_ordem
            FROM olist_mvp.gold.fato_pedido WHERE flag_entregue AND dias_atraso IS NOT NULL) f
    JOIN olist_mvp.gold.dim_cliente c ON f.cliente_id = c.cliente_id
)
SELECT CASE WHEN dias_atraso > 0 THEN 'primeiro pedido atrasou' ELSE 'primeiro pedido no prazo' END AS situacao,
       count(*)                                                                  AS clientes,
       sum(CASE WHEN total_pedidos > 1 THEN 1 ELSE 0 END)                        AS voltaram_a_comprar,
       round(100.0 * sum(CASE WHEN total_pedidos > 1 THEN 1 ELSE 0 END)/count(*), 2) AS taxa_recompra_pct
  FROM pedidos_pessoa WHERE ordem = 1
 GROUP BY 1 ORDER BY 1;

-- ----------------------------------------------------------------------------
-- Evolução mensal: volume, prazo, atraso e nota
-- ----------------------------------------------------------------------------
SELECT d.ano_mes,
       count(*)                                                                      AS pedidos,
       round(sum(f.valor_total), 2)                                                  AS receita,
       round(avg(f.dias_ate_entrega), 1)                                             AS dias_entrega,
       round(100.0 * sum(CASE WHEN f.dias_atraso > 0 THEN 1 ELSE 0 END)/count(*), 1) AS pct_atrasados,
       round(avg(f.nota), 2)                                                         AS nota_media
  FROM olist_mvp.gold.fato_pedido f
  JOIN olist_mvp.gold.dim_data d ON f.data_compra_sk = d.data_sk
 WHERE f.flag_entregue AND f.dias_ate_entrega IS NOT NULL
 GROUP BY d.ano_mes ORDER BY d.ano_mes;
