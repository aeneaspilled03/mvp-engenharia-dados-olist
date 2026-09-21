# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Catálogo de Dados
# MAGIC
# MAGIC Dado sem documentação vira caixa-preta: ninguém sabe o que cada campo significa, que valores
# MAGIC são válidos, de onde vieram. Este notebook resolve isso a partir de **uma única fonte de
# MAGIC verdade** — o dicionário `CATALOGO` abaixo — e a usa para três coisas ao mesmo tempo:
# MAGIC
# MAGIC 1. **Grava as descrições no Unity Catalog** (`COMMENT ON` em tabelas e colunas), de modo que
# MAGIC    elas apareçam no Catalog Explorer, no autocomplete do editor SQL e para qualquer
# MAGIC    ferramenta que leia o metastore.
# MAGIC 2. **Publica a tabela `gold.catalogo_de_dados`**, consultável por SQL.
# MAGIC 3. **Exporta `catalogo_de_dados.md`**, versionado junto com o código no repositório.
# MAGIC
# MAGIC Antes de tudo isso, o notebook **valida** o dicionário contra o schema real das tabelas: se
# MAGIC uma coluna foi criada e não documentada (ou documentada e não existe mais), a execução falha.
# MAGIC É o que impede o catálogo de envelhecer em silêncio enquanto o pipeline evolui.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

CATALOGO = {
    # ══════════════════════════════════════════════════════════════════ SILVER
    f"{SILVER}.geolocalizacao": {
        "descricao": "Uma linha por prefixo de CEP (5 primeiros dígitos), com coordenada geográfica "
                     "consolidada. Origem: bronze.geolocalizacao (1.000.163 observações de GPS), "
                     "reduzida pela mediana das coordenadas válidas de cada CEP.",
        "colunas": {
            "cep_prefixo": "Cinco primeiros dígitos do CEP. Chave primária. Domínio: 1.001 a 99.990.",
            "cidade": "Município padronizado (minúsculas, sem acento, sem sufixo de UF). Rótulo mais frequente entre as observações do CEP.",
            "uf": "Sigla da unidade federativa. Domínio: as 27 UFs brasileiras.",
            "regiao": "Região do IBGE derivada da UF. Domínio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul.",
            "latitude": "Latitude mediana das observações do CEP. Domínio: -33,75 a 5,28 (território brasileiro).",
            "longitude": "Longitude mediana das observações do CEP. Domínio: -73,99 a -34,79.",
            "qtd_observacoes": "Quantas linhas da Bronze foram consolidadas neste CEP. Serve como medida de confiança da coordenada.",
        },
    },
    f"{SILVER}.clientes": {
        "descricao": "Clientes no grão de compra. Origem: bronze.clientes, com cidade padronizada e "
                     "região derivada da UF.",
        "colunas": {
            "cliente_id": "Identificador do cliente NAQUELA compra — muda a cada pedido. Chave primária. Hash de 32 caracteres.",
            "cliente_unico_id": "Identificador da pessoa, estável entre pedidos. É a chave que permite medir recorrência.",
            "cep_prefixo": "Prefixo de CEP do cliente. Chave estrangeira para silver.geolocalizacao.",
            "cidade": "Município do cliente, padronizado.",
            "uf": "Sigla da UF do cliente. Domínio: 27 UFs.",
            "regiao": "Região do IBGE derivada da UF.",
        },
    },
    f"{SILVER}.vendedores": {
        "descricao": "Vendedores (lojistas) cadastrados no marketplace. Origem: bronze.vendedores.",
        "colunas": {
            "vendedor_id": "Identificador do vendedor. Chave primária. Hash de 32 caracteres.",
            "cep_prefixo": "Prefixo de CEP do vendedor. Chave estrangeira para silver.geolocalizacao.",
            "cidade": "Município do vendedor, padronizado (remove sufixos como ' / sp' e nomes com acentuação inconsistente).",
            "uf": "Sigla da UF do vendedor. Domínio: 23 UFs presentes no dataset.",
            "regiao": "Região do IBGE derivada da UF.",
            "flag_cidade_invalida": "Verdadeiro quando o campo cidade da origem vinha preenchido com um número (1 ocorrência).",
        },
    },
    f"{SILVER}.produtos": {
        "descricao": "Catálogo de produtos. Origem: bronze.produtos enriquecida com "
                     "bronze.traducao_categoria; corrige o erro de grafia da fonte (lenght → length).",
        "colunas": {
            "produto_id": "Identificador do produto. Chave primária.",
            "categoria": "Categoria em português. Domínio: 73 categorias + 'nao_informado' para os 610 produtos sem categoria na origem.",
            "categoria_en": "Categoria em inglês. Origem: bronze.traducao_categoria; nas 2 categorias sem tradução repete o nome em português.",
            "tamanho_nome": "Número de caracteres do nome do produto no anúncio. Nulo para produtos sem cadastro completo.",
            "tamanho_descricao": "Número de caracteres da descrição do anúncio.",
            "qtd_fotos": "Quantidade de fotos no anúncio. Domínio: 1 a 20.",
            "peso_g": "Peso do produto em gramas. Domínio observado: 0 a 40.425. Nulo em 2 produtos.",
            "comprimento_cm": "Comprimento da embalagem em centímetros.",
            "altura_cm": "Altura da embalagem em centímetros.",
            "largura_cm": "Largura da embalagem em centímetros.",
            "volume_cm3": "Volume calculado: comprimento × altura × largura. Derivado nesta camada.",
            "flag_categoria_ausente": "Verdadeiro quando a categoria não vinha preenchida na origem.",
            "flag_dimensoes_ausentes": "Verdadeiro quando peso ou dimensões não vieram preenchidos.",
        },
    },
    f"{SILVER}.pedidos": {
        "descricao": "Pedidos com os quatro marcos do ciclo (compra, aprovação, postagem, entrega) "
                     "tipados e as métricas de prazo derivadas. Origem: bronze.pedidos.",
        "colunas": {
            "pedido_id": "Identificador do pedido. Chave primária.",
            "cliente_id": "Chave estrangeira para silver.clientes.",
            "status": "Situação do pedido. Domínio: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved.",
            "dt_compra": "Data e hora em que o pedido foi realizado.",
            "dt_aprovacao": "Data e hora da aprovação do pagamento. Nulo em 160 pedidos.",
            "dt_postagem": "Data e hora da entrega à transportadora. Nulo em 1.783 pedidos.",
            "dt_entrega": "Data e hora da entrega ao cliente. Nulo em 2.965 pedidos (não entregues + 8 entregues sem registro).",
            "dt_prazo_estimado": "Prazo de entrega prometido ao cliente no momento da compra.",
            "data_compra": "Data (sem hora) da compra. Usada para ligar à dimensão calendário.",
            "ano_mes_compra": "Competência da compra no formato yyyy-MM.",
            "flag_entregue": "Verdadeiro quando status = 'delivered'.",
            "flag_cancelado": "Verdadeiro quando status é 'canceled' ou 'unavailable'.",
            "horas_ate_aprovacao": "Horas entre a compra e a aprovação do pagamento.",
            "dias_ate_postagem": "Dias entre a aprovação e a postagem à transportadora. Mede o tempo de manuseio do vendedor.",
            "dias_transporte": "Dias entre a postagem e a entrega ao cliente. Mede o tempo da logística.",
            "dias_ate_entrega": "Dias entre a compra e a entrega — o prazo que o cliente efetivamente percebe.",
            "dias_prazo_prometido": "Dias entre a compra e o prazo prometido.",
            "dias_atraso": "Dias entre a entrega real e o prazo prometido. NEGATIVO significa entrega adiantada.",
            "flag_atrasado": "Verdadeiro quando dias_atraso > 0. Nulo quando não há entrega registrada.",
            "flag_entregue_sem_data": "Verdadeiro nos 8 pedidos marcados como entregues sem data de entrega.",
            "flag_nao_entregue_com_data": "Verdadeiro nos 6 pedidos com data de entrega apesar de status diferente de 'delivered'.",
            "flag_sequencia_datas_invalida": "Verdadeiro quando os marcos estão fora de ordem cronológica (1.359 pedidos).",
        },
    },
    f"{SILVER}.itens_pedido": {
        "descricao": "Itens que compõem cada pedido. Um pedido com três produtos gera três linhas. "
                     "Origem: bronze.itens_pedido.",
        "colunas": {
            "pedido_id": "Chave estrangeira para silver.pedidos. Parte da chave primária composta.",
            "item_seq": "Número sequencial do item dentro do pedido. Parte da chave primária composta. Domínio: 1 a 21.",
            "produto_id": "Chave estrangeira para silver.produtos.",
            "vendedor_id": "Chave estrangeira para silver.vendedores.",
            "dt_limite_postagem": "Prazo contratual que o vendedor tem para postar o item.",
            "valor_produto": "Preço do produto em reais. Domínio: 0,85 a 6.735,00.",
            "valor_frete": "Valor do frete rateado para o item, em reais. Domínio: 0,00 a 409,68.",
            "valor_item": "valor_produto + valor_frete. Derivado nesta camada.",
            "flag_frete_gratis": "Verdadeiro quando o frete do item é zero (383 itens).",
        },
    },
    f"{SILVER}.pagamentos": {
        "descricao": "Transações de pagamento. Um pedido pode ter várias (voucher + cartão, por "
                     "exemplo). Origem: bronze.pagamentos.",
        "colunas": {
            "pedido_id": "Chave estrangeira para silver.pedidos. Parte da chave primária composta.",
            "pagamento_seq": "Sequencial da transação dentro do pedido. Parte da chave primária composta.",
            "meio_pagamento": "Meio utilizado. Domínio: credit_card, boleto, voucher, debit_card, not_defined.",
            "valor_pago": "Valor da transação em reais.",
            "parcelas": "Número de parcelas, já normalizado (0 na origem passa a 1). Domínio: 1 a 24.",
            "flag_parcelas_corrigidas": "Verdadeiro quando a origem trazia 0 parcelas (2 transações).",
            "flag_meio_indefinido": "Verdadeiro quando o meio é 'not_defined' (3 transações).",
            "flag_valor_nulo": "Verdadeiro quando o valor pago é zero ou negativo (9 transações).",
        },
    },
    f"{SILVER}.avaliacoes": {
        "descricao": "Uma avaliação por pedido. Origem: bronze.avaliacoes (100.000 linhas), "
                     "deduplicada mantendo a resposta mais recente de cada pedido.",
        "colunas": {
            "avaliacao_id": "Identificador da avaliação na origem. Não é único: o mesmo id aparece em pedidos diferentes.",
            "pedido_id": "Chave estrangeira para silver.pedidos. Chave primária desta tabela após a deduplicação.",
            "nota": "Nota dada pelo cliente. Domínio: 1 a 5.",
            "titulo_comentario": "Título livre do comentário. Ausente em 88% das avaliações.",
            "comentario": "Texto livre do comentário. Ausente em 58% das avaliações.",
            "dt_envio_pesquisa": "Data em que a pesquisa de satisfação foi enviada ao cliente.",
            "dt_resposta": "Data e hora da resposta do cliente. Critério de desempate na deduplicação.",
            "qtd_avaliacoes_pedido": "Quantas avaliações o pedido recebeu na origem. Domínio: 1 a 3.",
            "flag_pedido_reavaliado": "Verdadeiro quando o pedido tinha mais de uma avaliação na origem.",
            "tem_comentario": "Verdadeiro quando existe texto no comentário.",
            "tamanho_comentario": "Número de caracteres do comentário.",
            "faixa_nota": "Classificação da nota. Domínio: Detrator (1-2), Neutro (3), Promotor (4-5).",
        },
    },

    # ════════════════════════════════════════════════════════════════════ GOLD
    f"{GOLD}.dim_data": {
        "descricao": "Dimensão calendário, um registro por dia entre a primeira compra e a última "
                     "data prevista de entrega. Gerada por sequência de datas.",
        "colunas": {
            "data_sk": "Chave substituta no formato yyyyMMdd. Chave primária.",
            "data": "Data civil.",
            "ano": "Ano. Domínio: 2016 a 2018.",
            "trimestre": "Trimestre do ano. Domínio: 1 a 4.",
            "mes": "Mês do ano. Domínio: 1 a 12.",
            "nome_mes": "Nome do mês por extenso, em português.",
            "ano_mes": "Competência no formato yyyy-MM. Usada nas séries mensais.",
            "dia": "Dia do mês. Domínio: 1 a 31.",
            "dia_semana_num": "Dia da semana. Domínio: 1 (domingo) a 7 (sábado).",
            "nome_dia_semana": "Nome do dia da semana, em português.",
            "flag_fim_de_semana": "Verdadeiro para sábado e domingo.",
        },
    },
    f"{GOLD}.dim_cliente": {
        "descricao": "Dimensão cliente no grão de compra. Origem: silver.clientes enriquecida com "
                     "coordenadas de silver.geolocalizacao e com a contagem de pedidos da pessoa.",
        "colunas": {
            "cliente_id": "Chave primária. Identificador do cliente naquela compra.",
            "cliente_unico_id": "Identificador da pessoa, estável entre pedidos. 96.096 pessoas distintas.",
            "cep_prefixo": "Prefixo de CEP do cliente.",
            "cidade": "Município do cliente, padronizado.",
            "uf": "UF do cliente. Domínio: 27 UFs.",
            "regiao": "Região do IBGE. Domínio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul.",
            "latitude": "Latitude do CEP do cliente. Junção com silver.geolocalizacao. Nula em 278 clientes.",
            "longitude": "Longitude do CEP do cliente.",
            "qtd_pedidos_pessoa": "Total de pedidos feitos pela pessoa (cliente_unico_id) em toda a base. Domínio: 1 a 17.",
            "flag_cliente_recorrente": "Verdadeiro quando a pessoa fez mais de um pedido (3,12% das pessoas).",
            "flag_sem_coordenada": "Verdadeiro quando o CEP não tem coordenada na base de geolocalização.",
        },
    },
    f"{GOLD}.dim_vendedor": {
        "descricao": "Dimensão vendedor. Origem: silver.vendedores enriquecida com coordenadas de "
                     "silver.geolocalizacao.",
        "colunas": {
            "vendedor_id": "Chave primária. Identificador do vendedor.",
            "cep_prefixo": "Prefixo de CEP do vendedor.",
            "cidade": "Município do vendedor, padronizado.",
            "uf": "UF do vendedor. Domínio: 23 UFs presentes.",
            "regiao": "Região do IBGE do vendedor.",
            "latitude": "Latitude do CEP do vendedor. Nula em 7 vendedores.",
            "longitude": "Longitude do CEP do vendedor.",
            "flag_sem_coordenada": "Verdadeiro quando o CEP do vendedor não tem coordenada.",
        },
    },
    f"{GOLD}.dim_produto": {
        "descricao": "Dimensão produto. Origem: silver.produtos, com agrupamento das 15 maiores "
                     "categorias por receita e faixa de peso para leitura em gráficos.",
        "colunas": {
            "produto_id": "Chave primária. Identificador do produto.",
            "categoria": "Categoria em português. Domínio: 73 categorias + 'nao_informado'.",
            "categoria_en": "Categoria em inglês.",
            "categoria_agrupada": "As 15 maiores categorias por receita; todas as demais viram 'outras'. Derivado nesta camada.",
            "peso_g": "Peso do produto em gramas.",
            "comprimento_cm": "Comprimento da embalagem em centímetros.",
            "altura_cm": "Altura da embalagem em centímetros.",
            "largura_cm": "Largura da embalagem em centímetros.",
            "volume_cm3": "Volume da embalagem em centímetros cúbicos.",
            "faixa_peso": "Faixa de peso. Domínio: ate 500g, 501g a 2kg, 2kg a 10kg, acima de 10kg, nao informado.",
            "qtd_fotos": "Quantidade de fotos no anúncio.",
            "tamanho_nome": "Número de caracteres do nome do produto.",
            "tamanho_descricao": "Número de caracteres da descrição.",
            "flag_categoria_ausente": "Verdadeiro para os 610 produtos sem categoria na origem.",
            "flag_dimensoes_ausentes": "Verdadeiro quando peso ou dimensões estão ausentes.",
        },
    },
    f"{GOLD}.fato_pedido": {
        "descricao": "Tabela fato no grão de UM PEDIDO. Consolida itens (valores), pagamentos e "
                     "avaliação. É a tabela das perguntas de prazo, satisfação e pagamento. "
                     "Origem: silver.pedidos + agregações de silver.itens_pedido, "
                     "silver.pagamentos e silver.avaliacoes.",
        "colunas": {
            "pedido_id": "Chave primária. Identificador do pedido.",
            "cliente_id": "Chave estrangeira para dim_cliente.",
            "data_compra_sk": "Chave estrangeira para dim_data (data da compra).",
            "data_entrega_sk": "Chave estrangeira para dim_data (data da entrega). Nula quando não houve entrega.",
            "status": "Situação do pedido. Atributo degenerado. Domínio: 8 valores, sendo 'delivered' 97% dos casos.",
            "data_compra": "Data da compra, replicada no fato para consultas diretas.",
            "ano_mes_compra": "Competência da compra no formato yyyy-MM.",
            "flag_entregue": "Verdadeiro quando o pedido foi entregue.",
            "flag_cancelado": "Verdadeiro para pedidos cancelados ou indisponíveis.",
            "flag_atrasado": "Verdadeiro quando a entrega passou do prazo prometido.",
            "faixa_atraso": "Classificação do desvio de prazo. Domínio: mais de 10 dias adiantado, adiantado, no prazo exato, ate 7 dias de atraso, mais de 7 dias de atraso, sem entrega registrada.",
            "qtd_itens": "Quantidade de itens no pedido. Domínio: 1 a 21. Nula nos 775 pedidos sem item.",
            "qtd_produtos_distintos": "Quantidade de produtos diferentes no pedido.",
            "qtd_vendedores": "Quantidade de vendedores distintos que atenderam o pedido.",
            "valor_produtos": "Soma do preço dos produtos do pedido, em reais.",
            "valor_frete": "Soma do frete dos itens do pedido, em reais.",
            "valor_total": "valor_produtos + valor_frete.",
            "pct_frete": "Participação do frete no valor total do pedido, em pontos percentuais.",
            "valor_pago": "Soma das transações de pagamento do pedido, em reais.",
            "parcelas": "Maior número de parcelas entre as transações do pedido. Domínio: 1 a 24.",
            "qtd_meios_pagamento": "Quantidade de meios de pagamento distintos usados no pedido.",
            "meio_pagamento_principal": "Meio de pagamento da transação de maior valor do pedido. Domínio: credit_card, boleto, voucher, debit_card, not_defined.",
            "horas_ate_aprovacao": "Horas entre a compra e a aprovação do pagamento.",
            "dias_ate_postagem": "Dias entre a aprovação e a postagem (tempo do vendedor).",
            "dias_transporte": "Dias entre a postagem e a entrega (tempo da logística).",
            "dias_ate_entrega": "Dias entre a compra e a entrega (tempo percebido pelo cliente).",
            "dias_prazo_prometido": "Dias entre a compra e o prazo prometido no checkout.",
            "dias_atraso": "Entrega real menos prazo prometido, em dias. Negativo = adiantado.",
            "distancia_km": "Distância média (Haversine) entre os vendedores do pedido e o cliente, em quilômetros. Derivada das coordenadas de dim_cliente e dim_vendedor.",
            "nota": "Nota da avaliação do pedido. Domínio: 1 a 5.",
            "faixa_nota": "Classificação da nota. Domínio: Detrator, Neutro, Promotor.",
            "tem_comentario": "Verdadeiro quando o cliente escreveu um comentário.",
            "tamanho_comentario": "Número de caracteres do comentário.",
            "flag_entregue_sem_data": "Pedido marcado como entregue sem data de entrega (8 casos).",
            "flag_nao_entregue_com_data": "Pedido com data de entrega apesar de status diferente de entregue (6 casos).",
            "flag_sequencia_datas_invalida": "Marcos do ciclo fora de ordem cronológica (1.359 casos).",
            "flag_pedido_reavaliado": "Pedido que recebeu mais de uma avaliação na origem.",
        },
    },
    f"{GOLD}.fato_item_pedido": {
        "descricao": "Tabela fato no grão de UM ITEM DE UM PEDIDO. É a tabela das perguntas de "
                     "receita, frete, produto e vendedor. Origem: silver.itens_pedido enriquecida "
                     "com silver.pedidos, dim_cliente e dim_vendedor.",
        "colunas": {
            "pedido_id": "Parte da chave primária composta. Chave estrangeira para fato_pedido.",
            "item_seq": "Parte da chave primária composta. Sequencial do item no pedido.",
            "produto_id": "Chave estrangeira para dim_produto.",
            "vendedor_id": "Chave estrangeira para dim_vendedor.",
            "cliente_id": "Chave estrangeira para dim_cliente.",
            "data_compra_sk": "Chave estrangeira para dim_data.",
            "dt_limite_postagem": "Prazo contratual do vendedor para postar o item.",
            "valor_produto": "Preço do produto, em reais.",
            "valor_frete": "Frete atribuído ao item, em reais.",
            "valor_item": "valor_produto + valor_frete.",
            "pct_frete": "Participação do frete no valor do item, em pontos percentuais.",
            "distancia_km": "Distância em linha reta (Haversine) entre o CEP do vendedor e o do cliente. Nula quando algum dos CEPs não tem coordenada.",
            "uf_cliente": "UF de destino.",
            "regiao_cliente": "Região de destino.",
            "uf_vendedor": "UF de origem.",
            "regiao_vendedor": "Região de origem.",
            "flag_interestadual": "Verdadeiro quando origem e destino estão em UFs diferentes (64% dos itens).",
            "status": "Situação do pedido ao qual o item pertence.",
            "flag_entregue": "Verdadeiro quando o pedido foi entregue.",
            "dias_atraso": "Atraso do pedido ao qual o item pertence, em dias.",
            "flag_atrasado": "Verdadeiro quando o pedido do item foi entregue com atraso.",
        },
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Validação: o catálogo cobre exatamente o que existe?

# COMMAND ----------

problemas = []
for tabela, meta in CATALOGO.items():
    colunas_reais = set(spark.table(tabela).columns)
    colunas_doc = set(meta["colunas"])
    for c in sorted(colunas_reais - colunas_doc):
        problemas.append((tabela, c, "existe na tabela e NÃO está no catálogo"))
    for c in sorted(colunas_doc - colunas_reais):
        problemas.append((tabela, c, "está no catálogo e NÃO existe na tabela"))

if problemas:
    display(spark.createDataFrame(problemas, "tabela string, coluna string, problema string"))
    raise AssertionError(f"Catálogo desatualizado: {len(problemas)} divergência(s).")

total_colunas = sum(len(m["colunas"]) for m in CATALOGO.values())
print(f"Catálogo consistente: {len(CATALOGO)} tabelas e {total_colunas} colunas documentadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Gravação dos comentários no Unity Catalog

# COMMAND ----------

for tabela, meta in CATALOGO.items():
    texto = meta["descricao"].replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{texto}'")
    documenta_colunas(tabela, meta["colunas"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Publicação da tabela `gold.catalogo_de_dados`

# COMMAND ----------

NAO_SAO_CHAVE = {"cliente_unico_id", "avaliacao_id"}

CHAVES = {
    f"{SILVER}.geolocalizacao": ["cep_prefixo"],
    f"{SILVER}.clientes": ["cliente_id"],
    f"{SILVER}.vendedores": ["vendedor_id"],
    f"{SILVER}.produtos": ["produto_id"],
    f"{SILVER}.pedidos": ["pedido_id"],
    f"{SILVER}.itens_pedido": ["pedido_id", "item_seq"],
    f"{SILVER}.pagamentos": ["pedido_id", "pagamento_seq"],
    f"{SILVER}.avaliacoes": ["pedido_id"],
    f"{GOLD}.dim_data": ["data_sk"],
    f"{GOLD}.dim_cliente": ["cliente_id"],
    f"{GOLD}.dim_vendedor": ["vendedor_id"],
    f"{GOLD}.dim_produto": ["produto_id"],
    f"{GOLD}.fato_pedido": ["pedido_id"],
    f"{GOLD}.fato_item_pedido": ["pedido_id", "item_seq"],
}

linhas = []
for tabela, meta in CATALOGO.items():
    camada = tabela.split(".")[1]
    nome_curto = tabela.split(".")[-1]
    schema = spark.table(tabela).schema
    tipos = {campo.name: campo.dataType.simpleString() for campo in schema}
    for ordem, campo in enumerate(schema, start=1):
        nome = campo.name
        if nome in CHAVES.get(tabela, []):
            papel = "chave primária"
        elif nome in NAO_SAO_CHAVE:
            papel = "atributo"
        elif nome.endswith("_sk") or nome.endswith("_id"):
            papel = "chave estrangeira"
        elif nome.startswith("flag_"):
            papel = "indicador"
        else:
            papel = "atributo"
        linhas.append((camada, nome_curto, ordem, nome, tipos[nome], papel,
                       meta["colunas"][nome], meta["descricao"]))

df_catalogo = spark.createDataFrame(
    linhas,
    "camada string, tabela string, ordem int, coluna string, tipo string, papel string, "
    "descricao_coluna string, descricao_tabela string",
)

salva_tabela(df_catalogo, f"{GOLD}.catalogo_de_dados",
             comentario="Catálogo de dados das camadas Silver e Gold: tipo, papel e descrição de cada coluna")

display(df_catalogo.filter(f"camada = '{SCHEMA_GOLD}'").orderBy("tabela", "ordem"), 120)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Exportação do catálogo em Markdown

# COMMAND ----------

import os

os.makedirs(DIR_SAIDA, exist_ok=True)
caminho_md = f"{DIR_SAIDA}/catalogo_de_dados.md"

registros = [linha.asDict() for linha in df_catalogo.orderBy("camada", "tabela", "ordem").collect()]

with open(caminho_md, "w", encoding="utf-8") as saida:
    saida.write("# Catálogo de Dados\n\n")
    saida.write("> Arquivo **gerado automaticamente** pelo notebook `07_catalogo_de_dados`.\n"
                "> A mesma fonte alimenta os comentários das tabelas e colunas no Unity Catalog\n"
                "> e a tabela `gold.catalogo_de_dados`. Não edite à mão.\n\n")
    saida.write(f"Camadas documentadas: **{len(CATALOGO)} tabelas**, "
                f"**{len(registros)} colunas**.\n\n")

    tabela_atual = None
    for registro in registros:
        chave = (registro["camada"], registro["tabela"])
        if chave != tabela_atual:
            tabela_atual = chave
            saida.write(f"\n## `{registro['camada']}.{registro['tabela']}`\n\n")
            saida.write(f"{registro['descricao_tabela']}\n\n")
            saida.write("| # | Coluna | Tipo | Papel | Descrição e domínio |\n")
            saida.write("|---|---|---|---|---|\n")
        saida.write(f"| {registro['ordem']} | `{registro['coluna']}` | `{registro['tipo']}` | "
                    f"{registro['papel']} | {registro['descricao_coluna']} |\n")

print(f"  ✔ {caminho_md} ({len(registros)} linhas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.5 Conferência: os comentários chegaram ao Unity Catalog?

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {GOLD}.fato_pedido"), 60)
