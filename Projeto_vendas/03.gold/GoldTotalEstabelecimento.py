# Databricks notebook source
# MAGIC %md
# MAGIC ## Informações Gerais
# MAGIC
# MAGIC | Informações     | Detalhes                            |
# MAGIC |-----------------|-------------------------------------|
# MAGIC | Nome Tabela     | gold.venda_total_estabelecimento |
# MAGIC | Origem          | silver.pedidos |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC
# MAGIC | Data       | Desenvolvido por | Motivo                    |
# MAGIC |------------|------------------|---------------------------|
# MAGIC | 06/06/2025 | Natã Felipe    | Criação do notebook  |
# MAGIC | 07/06/2025 | Natã Felipe    | Criação GoldTotalEstabelecimentos  |
# MAGIC | 07/06/2025 | Natã Felipe    | GoldTotalEstabelecimento Finalizada |
# MAGIC

# COMMAND ----------

# Nome do esquema e tabela do catálogo
database = "gold"
tabela = "venda_total_estabelecimentos"

# COMMAND ----------

# funções
# Função que aplica os comentários na tabela
def adicionaComentariosTabela(a,b,c,d):
    spark.sql(f"COMMENT ON TABLE {a}.{b} IS '{c}'")
    for key, value in d.items():
        sqlaux = f"ALTER TABLE {a}.{b} CHANGE COLUMN {key} COMMENT '{value}'"
        spark.sql(sqlaux)

# COMMAND ----------

# Comentario Tabela
comentario_tabela = 'Esta tabela é uma entidade corporativa que contém a relação dos pedidos'

lista_comentario_colunas = {
    'ID_ESTABELECIMENTO' : 'Id do estabelecimento.',
    'QUANTIDADE_TOTAL'         : 'Quantidade Total.',
    'VALOR_TOTAL'              : 'Valor Total Vendido.',
    'NOME_ESTABELECIMENTO': 'Nome do estabelecimento.',
}

# COMMAND ----------

df_pedidos = spark.sql(
f"""
WITH TUDO AS (
SELECT
    ID_ESTABELECIMENTO,
    NOME_ESTABELECIMENTO,
    PRODUTO,
    SUM(QUANTIDADE) AS QUANTIDADE_POR_PRODUTO,
    (PRECO * QUANTIDADE) AS VALOR_PRODUTO
FROM SILVER.PEDIDOS
GROUP BY ALL
)
SELECT
    ID_ESTABELECIMENTO,
    NOME_ESTABELECIMENTO,
    SUM(QUANTIDADE_POR_PRODUTO) AS QUANTIDADE_TOTAL, 
    SUM(VALOR_PRODUTO) AS VALOR_TOTAL
FROM TUDO
GROUP BY
    ID_ESTABELECIMENTO,
    NOME_ESTABELECIMENTO
ORDER BY VALOR_TOTAL DESC
LIMIT 5
"""
)


# COMMAND ----------

df_pedidos.display()

# COMMAND ----------

df_pedidos.write \
    .format('delta') \
    .mode('overwrite') \
    .clusterBy("ID_ESTABELECIMENTO" ) \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')
adicionaComentariosTabela(database, tabela, comentario_tabela, lista_comentario_colunas) 
print('Dados gravados com suceeso')  

# COMMAND ----------

spark.sql(f"OPTIMIZE {database}.{tabela}")
print(f"Processo de otimização finalizado!")