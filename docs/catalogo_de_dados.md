# Catálogo de Dados

> Arquivo **gerado automaticamente** pelo notebook `07_catalogo_de_dados`.
> A mesma fonte alimenta os comentários das tabelas e colunas no Unity Catalog
> e a tabela `gold.catalogo_de_dados`. Não edite à mão.

Camadas documentadas: **14 tabelas**, **186 colunas**.


## `gold.dim_cliente`

Dimensão cliente no grão de compra. Origem: silver.clientes enriquecida com coordenadas de silver.geolocalizacao e com a contagem de pedidos da pessoa.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `cliente_id` | `string` | chave primária | Chave primária. Identificador do cliente naquela compra. |
| 2 | `cliente_unico_id` | `string` | chave estrangeira | Identificador da pessoa, estável entre pedidos. 96.096 pessoas distintas. |
| 3 | `cep_prefixo` | `int` | atributo | Prefixo de CEP do cliente. |
| 4 | `cidade` | `string` | atributo | Município do cliente, padronizado. |
| 5 | `uf` | `string` | atributo | UF do cliente. Domínio: 27 UFs. |
| 6 | `regiao` | `string` | atributo | Região do IBGE. Domínio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul. |
| 7 | `latitude` | `double` | atributo | Latitude do CEP do cliente. Junção com silver.geolocalizacao. Nula em 278 clientes. |
| 8 | `longitude` | `double` | atributo | Longitude do CEP do cliente. |
| 9 | `qtd_pedidos_pessoa` | `bigint` | atributo | Total de pedidos feitos pela pessoa (cliente_unico_id) em toda a base. Domínio: 1 a 17. |
| 10 | `flag_cliente_recorrente` | `boolean` | indicador | Verdadeiro quando a pessoa fez mais de um pedido (3,12% das pessoas). |
| 11 | `flag_sem_coordenada` | `boolean` | indicador | Verdadeiro quando o CEP não tem coordenada na base de geolocalização. |

## `gold.dim_data`

Dimensão calendário, um registro por dia entre a primeira compra e a última data prevista de entrega. Gerada por sequência de datas.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `data_sk` | `int` | chave primária | Chave substituta no formato yyyyMMdd. Chave primária. |
| 2 | `data` | `date` | atributo | Data civil. |
| 3 | `ano` | `int` | atributo | Ano. Domínio: 2016 a 2018. |
| 4 | `trimestre` | `int` | atributo | Trimestre do ano. Domínio: 1 a 4. |
| 5 | `mes` | `int` | atributo | Mês do ano. Domínio: 1 a 12. |
| 6 | `nome_mes` | `string` | atributo | Nome do mês por extenso, em português. |
| 7 | `ano_mes` | `string` | atributo | Competência no formato yyyy-MM. Usada nas séries mensais. |
| 8 | `dia` | `int` | atributo | Dia do mês. Domínio: 1 a 31. |
| 9 | `dia_semana_num` | `int` | atributo | Dia da semana. Domínio: 1 (domingo) a 7 (sábado). |
| 10 | `nome_dia_semana` | `string` | atributo | Nome do dia da semana, em português. |
| 11 | `flag_fim_de_semana` | `boolean` | indicador | Verdadeiro para sábado e domingo. |

## `gold.dim_produto`

Dimensão produto. Origem: silver.produtos, com agrupamento das 15 maiores categorias por receita e faixa de peso para leitura em gráficos.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `produto_id` | `string` | chave primária | Chave primária. Identificador do produto. |
| 2 | `categoria` | `string` | atributo | Categoria em português. Domínio: 73 categorias + 'nao_informado'. |
| 3 | `categoria_en` | `string` | atributo | Categoria em inglês. |
| 4 | `categoria_agrupada` | `string` | atributo | As 15 maiores categorias por receita; todas as demais viram 'outras'. Derivado nesta camada. |
| 5 | `peso_g` | `double` | atributo | Peso do produto em gramas. |
| 6 | `comprimento_cm` | `double` | atributo | Comprimento da embalagem em centímetros. |
| 7 | `altura_cm` | `double` | atributo | Altura da embalagem em centímetros. |
| 8 | `largura_cm` | `double` | atributo | Largura da embalagem em centímetros. |
| 9 | `volume_cm3` | `double` | atributo | Volume da embalagem em centímetros cúbicos. |
| 10 | `faixa_peso` | `string` | atributo | Faixa de peso. Domínio: ate 500g, 501g a 2kg, 2kg a 10kg, acima de 10kg, nao informado. |
| 11 | `qtd_fotos` | `int` | atributo | Quantidade de fotos no anúncio. |
| 12 | `tamanho_nome` | `int` | atributo | Número de caracteres do nome do produto. |
| 13 | `tamanho_descricao` | `int` | atributo | Número de caracteres da descrição. |
| 14 | `flag_categoria_ausente` | `boolean` | indicador | Verdadeiro para os 610 produtos sem categoria na origem. |
| 15 | `flag_dimensoes_ausentes` | `boolean` | indicador | Verdadeiro quando peso ou dimensões estão ausentes. |

## `gold.dim_vendedor`

Dimensão vendedor. Origem: silver.vendedores enriquecida com coordenadas de silver.geolocalizacao.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `vendedor_id` | `string` | chave primária | Chave primária. Identificador do vendedor. |
| 2 | `cep_prefixo` | `int` | atributo | Prefixo de CEP do vendedor. |
| 3 | `cidade` | `string` | atributo | Município do vendedor, padronizado. |
| 4 | `uf` | `string` | atributo | UF do vendedor. Domínio: 23 UFs presentes. |
| 5 | `regiao` | `string` | atributo | Região do IBGE do vendedor. |
| 6 | `latitude` | `double` | atributo | Latitude do CEP do vendedor. Nula em 7 vendedores. |
| 7 | `longitude` | `double` | atributo | Longitude do CEP do vendedor. |
| 8 | `flag_sem_coordenada` | `boolean` | indicador | Verdadeiro quando o CEP do vendedor não tem coordenada. |

## `gold.fato_item_pedido`

Tabela fato no grão de UM ITEM DE UM PEDIDO. É a tabela das perguntas de receita, frete, produto e vendedor. Origem: silver.itens_pedido enriquecida com silver.pedidos, dim_cliente e dim_vendedor.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `pedido_id` | `string` | chave primária | Parte da chave primária composta. Chave estrangeira para fato_pedido. |
| 2 | `item_seq` | `int` | chave primária | Parte da chave primária composta. Sequencial do item no pedido. |
| 3 | `produto_id` | `string` | chave estrangeira | Chave estrangeira para dim_produto. |
| 4 | `vendedor_id` | `string` | chave estrangeira | Chave estrangeira para dim_vendedor. |
| 5 | `cliente_id` | `string` | chave estrangeira | Chave estrangeira para dim_cliente. |
| 6 | `data_compra_sk` | `int` | chave estrangeira | Chave estrangeira para dim_data. |
| 7 | `dt_limite_postagem` | `timestamp` | atributo | Prazo contratual do vendedor para postar o item. |
| 8 | `valor_produto` | `decimal(10,2)` | atributo | Preço do produto, em reais. |
| 9 | `valor_frete` | `decimal(10,2)` | atributo | Frete atribuído ao item, em reais. |
| 10 | `valor_item` | `decimal(11,2)` | atributo | valor_produto + valor_frete. |
| 11 | `pct_frete` | `decimal(17,2)` | atributo | Participação do frete no valor do item, em pontos percentuais. |
| 12 | `distancia_km` | `double` | atributo | Distância em linha reta (Haversine) entre o CEP do vendedor e o do cliente. Nula quando algum dos CEPs não tem coordenada. |
| 13 | `uf_cliente` | `string` | atributo | UF de destino. |
| 14 | `regiao_cliente` | `string` | atributo | Região de destino. |
| 15 | `uf_vendedor` | `string` | atributo | UF de origem. |
| 16 | `regiao_vendedor` | `string` | atributo | Região de origem. |
| 17 | `flag_interestadual` | `boolean` | indicador | Verdadeiro quando origem e destino estão em UFs diferentes (64% dos itens). |
| 18 | `status` | `string` | atributo | Situação do pedido ao qual o item pertence. |
| 19 | `flag_entregue` | `boolean` | indicador | Verdadeiro quando o pedido foi entregue. |
| 20 | `dias_atraso` | `double` | atributo | Atraso do pedido ao qual o item pertence, em dias. |
| 21 | `flag_atrasado` | `boolean` | indicador | Verdadeiro quando o pedido do item foi entregue com atraso. |

## `gold.fato_pedido`

Tabela fato no grão de UM PEDIDO. Consolida itens (valores), pagamentos e avaliação. É a tabela das perguntas de prazo, satisfação e pagamento. Origem: silver.pedidos + agregações de silver.itens_pedido, silver.pagamentos e silver.avaliacoes.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `pedido_id` | `string` | chave primária | Chave primária. Identificador do pedido. |
| 2 | `cliente_id` | `string` | chave estrangeira | Chave estrangeira para dim_cliente. |
| 3 | `data_compra_sk` | `int` | chave estrangeira | Chave estrangeira para dim_data (data da compra). |
| 4 | `data_entrega_sk` | `int` | chave estrangeira | Chave estrangeira para dim_data (data da entrega). Nula quando não houve entrega. |
| 5 | `status` | `string` | atributo | Situação do pedido. Atributo degenerado. Domínio: 8 valores, sendo 'delivered' 97% dos casos. |
| 6 | `data_compra` | `date` | atributo | Data da compra, replicada no fato para consultas diretas. |
| 7 | `ano_mes_compra` | `string` | atributo | Competência da compra no formato yyyy-MM. |
| 8 | `flag_entregue` | `boolean` | indicador | Verdadeiro quando o pedido foi entregue. |
| 9 | `flag_cancelado` | `boolean` | indicador | Verdadeiro para pedidos cancelados ou indisponíveis. |
| 10 | `flag_atrasado` | `boolean` | indicador | Verdadeiro quando a entrega passou do prazo prometido. |
| 11 | `faixa_atraso` | `string` | atributo | Classificação do desvio de prazo. Domínio: mais de 10 dias adiantado, adiantado, no prazo exato, ate 7 dias de atraso, mais de 7 dias de atraso, sem entrega registrada. |
| 12 | `qtd_itens` | `bigint` | atributo | Quantidade de itens no pedido. Domínio: 1 a 21. Nula nos 775 pedidos sem item. |
| 13 | `qtd_produtos_distintos` | `bigint` | atributo | Quantidade de produtos diferentes no pedido. |
| 14 | `qtd_vendedores` | `bigint` | atributo | Quantidade de vendedores distintos que atenderam o pedido. |
| 15 | `valor_produtos` | `decimal(12,2)` | atributo | Soma do preço dos produtos do pedido, em reais. |
| 16 | `valor_frete` | `decimal(12,2)` | atributo | Soma do frete dos itens do pedido, em reais. |
| 17 | `valor_total` | `decimal(12,2)` | atributo | valor_produtos + valor_frete. |
| 18 | `pct_frete` | `decimal(19,2)` | atributo | Participação do frete no valor total do pedido, em pontos percentuais. |
| 19 | `valor_pago` | `decimal(12,2)` | atributo | Soma das transações de pagamento do pedido, em reais. |
| 20 | `parcelas` | `int` | atributo | Maior número de parcelas entre as transações do pedido. Domínio: 1 a 24. |
| 21 | `qtd_meios_pagamento` | `bigint` | atributo | Quantidade de meios de pagamento distintos usados no pedido. |
| 22 | `meio_pagamento_principal` | `string` | atributo | Meio de pagamento da transação de maior valor do pedido. Domínio: credit_card, boleto, voucher, debit_card, not_defined. |
| 23 | `horas_ate_aprovacao` | `double` | atributo | Horas entre a compra e a aprovação do pagamento. |
| 24 | `dias_ate_postagem` | `double` | atributo | Dias entre a aprovação e a postagem (tempo do vendedor). |
| 25 | `dias_transporte` | `double` | atributo | Dias entre a postagem e a entrega (tempo da logística). |
| 26 | `dias_ate_entrega` | `double` | atributo | Dias entre a compra e a entrega (tempo percebido pelo cliente). |
| 27 | `dias_prazo_prometido` | `double` | atributo | Dias entre a compra e o prazo prometido no checkout. |
| 28 | `dias_atraso` | `double` | atributo | Entrega real menos prazo prometido, em dias. Negativo = adiantado. |
| 29 | `distancia_km` | `double` | atributo | Distância média (Haversine) entre os vendedores do pedido e o cliente, em quilômetros. Derivada das coordenadas de dim_cliente e dim_vendedor. |
| 30 | `nota` | `int` | atributo | Nota da avaliação do pedido. Domínio: 1 a 5. |
| 31 | `faixa_nota` | `string` | atributo | Classificação da nota. Domínio: Detrator, Neutro, Promotor. |
| 32 | `tem_comentario` | `boolean` | atributo | Verdadeiro quando o cliente escreveu um comentário. |
| 33 | `tamanho_comentario` | `int` | atributo | Número de caracteres do comentário. |
| 34 | `flag_entregue_sem_data` | `boolean` | indicador | Pedido marcado como entregue sem data de entrega (8 casos). |
| 35 | `flag_nao_entregue_com_data` | `boolean` | indicador | Pedido com data de entrega apesar de status diferente de entregue (6 casos). |
| 36 | `flag_sequencia_datas_invalida` | `boolean` | indicador | Marcos do ciclo fora de ordem cronológica (1.359 casos). |
| 37 | `flag_pedido_reavaliado` | `boolean` | indicador | Pedido que recebeu mais de uma avaliação na origem. |

## `silver.avaliacoes`

Uma avaliação por pedido. Origem: bronze.avaliacoes (100.000 linhas), deduplicada mantendo a resposta mais recente de cada pedido.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `avaliacao_id` | `string` | chave estrangeira | Identificador da avaliação na origem. Não é único: o mesmo id aparece em pedidos diferentes. |
| 2 | `pedido_id` | `string` | chave primária | Chave estrangeira para silver.pedidos. Chave primária desta tabela após a deduplicação. |
| 3 | `nota` | `int` | atributo | Nota dada pelo cliente. Domínio: 1 a 5. |
| 4 | `titulo_comentario` | `string` | atributo | Título livre do comentário. Ausente em 88% das avaliações. |
| 5 | `comentario` | `string` | atributo | Texto livre do comentário. Ausente em 58% das avaliações. |
| 6 | `dt_envio_pesquisa` | `timestamp` | atributo | Data em que a pesquisa de satisfação foi enviada ao cliente. |
| 7 | `dt_resposta` | `timestamp` | atributo | Data e hora da resposta do cliente. Critério de desempate na deduplicação. |
| 8 | `qtd_avaliacoes_pedido` | `bigint` | atributo | Quantas avaliações o pedido recebeu na origem. Domínio: 1 a 3. |
| 9 | `flag_pedido_reavaliado` | `boolean` | indicador | Verdadeiro quando o pedido tinha mais de uma avaliação na origem. |
| 10 | `tem_comentario` | `boolean` | atributo | Verdadeiro quando existe texto no comentário. |
| 11 | `tamanho_comentario` | `int` | atributo | Número de caracteres do comentário. |
| 12 | `faixa_nota` | `string` | atributo | Classificação da nota. Domínio: Detrator (1-2), Neutro (3), Promotor (4-5). |

## `silver.clientes`

Clientes no grão de compra. Origem: bronze.clientes, com cidade padronizada e região derivada da UF.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `cliente_id` | `string` | chave primária | Identificador do cliente NAQUELA compra — muda a cada pedido. Chave primária. Hash de 32 caracteres. |
| 2 | `cliente_unico_id` | `string` | chave estrangeira | Identificador da pessoa, estável entre pedidos. É a chave que permite medir recorrência. |
| 3 | `cep_prefixo` | `int` | atributo | Prefixo de CEP do cliente. Chave estrangeira para silver.geolocalizacao. |
| 4 | `cidade` | `string` | atributo | Município do cliente, padronizado. |
| 5 | `uf` | `string` | atributo | Sigla da UF do cliente. Domínio: 27 UFs. |
| 6 | `regiao` | `string` | atributo | Região do IBGE derivada da UF. |

## `silver.geolocalizacao`

Uma linha por prefixo de CEP (5 primeiros dígitos), com coordenada geográfica consolidada. Origem: bronze.geolocalizacao (1.000.163 observações de GPS), reduzida pela mediana das coordenadas válidas de cada CEP.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `cep_prefixo` | `int` | chave primária | Cinco primeiros dígitos do CEP. Chave primária. Domínio: 1.001 a 99.990. |
| 2 | `cidade` | `string` | atributo | Município padronizado (minúsculas, sem acento, sem sufixo de UF). Rótulo mais frequente entre as observações do CEP. |
| 3 | `uf` | `string` | atributo | Sigla da unidade federativa. Domínio: as 27 UFs brasileiras. |
| 4 | `regiao` | `string` | atributo | Região do IBGE derivada da UF. Domínio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul. |
| 5 | `latitude` | `double` | atributo | Latitude mediana das observações do CEP. Domínio: -33,75 a 5,28 (território brasileiro). |
| 6 | `longitude` | `double` | atributo | Longitude mediana das observações do CEP. Domínio: -73,99 a -34,79. |
| 7 | `qtd_observacoes` | `bigint` | atributo | Quantas linhas da Bronze foram consolidadas neste CEP. Serve como medida de confiança da coordenada. |

## `silver.itens_pedido`

Itens que compõem cada pedido. Um pedido com três produtos gera três linhas. Origem: bronze.itens_pedido.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `pedido_id` | `string` | chave primária | Chave estrangeira para silver.pedidos. Parte da chave primária composta. |
| 2 | `item_seq` | `int` | chave primária | Número sequencial do item dentro do pedido. Parte da chave primária composta. Domínio: 1 a 21. |
| 3 | `produto_id` | `string` | chave estrangeira | Chave estrangeira para silver.produtos. |
| 4 | `vendedor_id` | `string` | chave estrangeira | Chave estrangeira para silver.vendedores. |
| 5 | `dt_limite_postagem` | `timestamp` | atributo | Prazo contratual que o vendedor tem para postar o item. |
| 6 | `valor_produto` | `decimal(10,2)` | atributo | Preço do produto em reais. Domínio: 0,85 a 6.735,00. |
| 7 | `valor_frete` | `decimal(10,2)` | atributo | Valor do frete rateado para o item, em reais. Domínio: 0,00 a 409,68. |
| 8 | `valor_item` | `decimal(11,2)` | atributo | valor_produto + valor_frete. Derivado nesta camada. |
| 9 | `flag_frete_gratis` | `boolean` | indicador | Verdadeiro quando o frete do item é zero (383 itens). |

## `silver.pagamentos`

Transações de pagamento. Um pedido pode ter várias (voucher + cartão, por exemplo). Origem: bronze.pagamentos.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `pedido_id` | `string` | chave primária | Chave estrangeira para silver.pedidos. Parte da chave primária composta. |
| 2 | `pagamento_seq` | `int` | chave primária | Sequencial da transação dentro do pedido. Parte da chave primária composta. |
| 3 | `meio_pagamento` | `string` | atributo | Meio utilizado. Domínio: credit_card, boleto, voucher, debit_card, not_defined. |
| 4 | `valor_pago` | `decimal(10,2)` | atributo | Valor da transação em reais. |
| 5 | `parcelas` | `int` | atributo | Número de parcelas, já normalizado (0 na origem passa a 1). Domínio: 1 a 24. |
| 6 | `flag_parcelas_corrigidas` | `boolean` | indicador | Verdadeiro quando a origem trazia 0 parcelas (2 transações). |
| 7 | `flag_meio_indefinido` | `boolean` | indicador | Verdadeiro quando o meio é 'not_defined' (3 transações). |
| 8 | `flag_valor_nulo` | `boolean` | indicador | Verdadeiro quando o valor pago é zero ou negativo (9 transações). |

## `silver.pedidos`

Pedidos com os quatro marcos do ciclo (compra, aprovação, postagem, entrega) tipados e as métricas de prazo derivadas. Origem: bronze.pedidos.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `pedido_id` | `string` | chave primária | Identificador do pedido. Chave primária. |
| 2 | `cliente_id` | `string` | chave estrangeira | Chave estrangeira para silver.clientes. |
| 3 | `status` | `string` | atributo | Situação do pedido. Domínio: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved. |
| 4 | `dt_compra` | `timestamp` | atributo | Data e hora em que o pedido foi realizado. |
| 5 | `dt_aprovacao` | `timestamp` | atributo | Data e hora da aprovação do pagamento. Nulo em 160 pedidos. |
| 6 | `dt_postagem` | `timestamp` | atributo | Data e hora da entrega à transportadora. Nulo em 1.783 pedidos. |
| 7 | `dt_entrega` | `timestamp` | atributo | Data e hora da entrega ao cliente. Nulo em 2.965 pedidos (não entregues + 8 entregues sem registro). |
| 8 | `dt_prazo_estimado` | `timestamp` | atributo | Prazo de entrega prometido ao cliente no momento da compra. |
| 9 | `data_compra` | `date` | atributo | Data (sem hora) da compra. Usada para ligar à dimensão calendário. |
| 10 | `ano_mes_compra` | `string` | atributo | Competência da compra no formato yyyy-MM. |
| 11 | `flag_entregue` | `boolean` | indicador | Verdadeiro quando status = 'delivered'. |
| 12 | `flag_cancelado` | `boolean` | indicador | Verdadeiro quando status é 'canceled' ou 'unavailable'. |
| 13 | `horas_ate_aprovacao` | `double` | atributo | Horas entre a compra e a aprovação do pagamento. |
| 14 | `dias_ate_postagem` | `double` | atributo | Dias entre a aprovação e a postagem à transportadora. Mede o tempo de manuseio do vendedor. |
| 15 | `dias_transporte` | `double` | atributo | Dias entre a postagem e a entrega ao cliente. Mede o tempo da logística. |
| 16 | `dias_ate_entrega` | `double` | atributo | Dias entre a compra e a entrega — o prazo que o cliente efetivamente percebe. |
| 17 | `dias_prazo_prometido` | `double` | atributo | Dias entre a compra e o prazo prometido. |
| 18 | `dias_atraso` | `double` | atributo | Dias entre a entrega real e o prazo prometido. NEGATIVO significa entrega adiantada. |
| 19 | `flag_atrasado` | `boolean` | indicador | Verdadeiro quando dias_atraso > 0. Nulo quando não há entrega registrada. |
| 20 | `flag_entregue_sem_data` | `boolean` | indicador | Verdadeiro nos 8 pedidos marcados como entregues sem data de entrega. |
| 21 | `flag_nao_entregue_com_data` | `boolean` | indicador | Verdadeiro nos 6 pedidos com data de entrega apesar de status diferente de 'delivered'. |
| 22 | `flag_sequencia_datas_invalida` | `boolean` | indicador | Verdadeiro quando os marcos estão fora de ordem cronológica (1.359 pedidos). |

## `silver.produtos`

Catálogo de produtos. Origem: bronze.produtos enriquecida com bronze.traducao_categoria; corrige o erro de grafia da fonte (lenght → length).

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `categoria` | `string` | atributo | Categoria em português. Domínio: 73 categorias + 'nao_informado' para os 610 produtos sem categoria na origem. |
| 2 | `produto_id` | `string` | chave primária | Identificador do produto. Chave primária. |
| 3 | `tamanho_nome` | `int` | atributo | Número de caracteres do nome do produto no anúncio. Nulo para produtos sem cadastro completo. |
| 4 | `tamanho_descricao` | `int` | atributo | Número de caracteres da descrição do anúncio. |
| 5 | `qtd_fotos` | `int` | atributo | Quantidade de fotos no anúncio. Domínio: 1 a 20. |
| 6 | `peso_g` | `double` | atributo | Peso do produto em gramas. Domínio observado: 0 a 40.425. Nulo em 2 produtos. |
| 7 | `comprimento_cm` | `double` | atributo | Comprimento da embalagem em centímetros. |
| 8 | `altura_cm` | `double` | atributo | Altura da embalagem em centímetros. |
| 9 | `largura_cm` | `double` | atributo | Largura da embalagem em centímetros. |
| 10 | `categoria_en` | `string` | atributo | Categoria em inglês. Origem: bronze.traducao_categoria; nas 2 categorias sem tradução repete o nome em português. |
| 11 | `volume_cm3` | `double` | atributo | Volume calculado: comprimento × altura × largura. Derivado nesta camada. |
| 12 | `flag_categoria_ausente` | `boolean` | indicador | Verdadeiro quando a categoria não vinha preenchida na origem. |
| 13 | `flag_dimensoes_ausentes` | `boolean` | indicador | Verdadeiro quando peso ou dimensões não vieram preenchidos. |

## `silver.vendedores`

Vendedores (lojistas) cadastrados no marketplace. Origem: bronze.vendedores.

| # | Coluna | Tipo | Papel | Descrição e domínio |
|---|---|---|---|---|
| 1 | `vendedor_id` | `string` | chave primária | Identificador do vendedor. Chave primária. Hash de 32 caracteres. |
| 2 | `cep_prefixo` | `int` | atributo | Prefixo de CEP do vendedor. Chave estrangeira para silver.geolocalizacao. |
| 3 | `cidade` | `string` | atributo | Município do vendedor, padronizado (remove sufixos como ' / sp' e nomes com acentuação inconsistente). |
| 4 | `uf` | `string` | atributo | Sigla da UF do vendedor. Domínio: 23 UFs presentes no dataset. |
| 5 | `regiao` | `string` | atributo | Região do IBGE derivada da UF. |
| 6 | `flag_cidade_invalida` | `boolean` | indicador | Verdadeiro quando o campo cidade da origem vinha preenchido com um número (1 ocorrência). |
