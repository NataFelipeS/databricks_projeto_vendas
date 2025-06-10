# Databricks notebook source
# MAGIC %md
# MAGIC ## Informações Gerais
# MAGIC
# MAGIC | Informações     | Detalhes                            |
# MAGIC |-----------------|-------------------------------------|
# MAGIC | Nome Tabela     | silver.estabelecimentos |
# MAGIC | Origem          | bronze.pedidos / bronze.estabelecimentos |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC
# MAGIC | Data       | Desenvolvido por | Motivo                    |
# MAGIC |------------|------------------|---------------------------|
# MAGIC | 06/06/2025 | Natã Felipe    | Ingestão bronze e criação notebook silver  |
# MAGIC

# COMMAND ----------

# funções
# Função que aplica os comentários na tabela
def adicionaComentariosTabela(a,b,c,d):
    spark.sql(f"COMMENT ON TABLE {a}.{b} IS '{c}'")
    for key, value in d.items():
        sqlaux = f"ALTER TABLE {a}.{b} CHANGE COLUMN {key} COMMENT '{value}'"
        spark.sql(sqlaux)


# COMMAND ----------

database = 'silver'
tabela = 'pedidos'

# COMMAND ----------

df_pedidos = spark.sql(
f"""
SELECT 
    BE.LOCAL AS NOME_ESTABELECIMENTO,
    BE.EMAIL AS EMAIL,
    CAST(BE.ESTABELECIMENTOID AS INT) AS ID_ESTABELECIMENTO,
    BE.TELEFONE AS TELEFONE,
    CAST(BP.PEDIDOID AS INT) AS ID_PEDIDO,
    BP.PRODUTO AS PRODUTO,
    CAST(BP.QUANTIDADE_VENDIDA AS INT) AS QUANTIDADE,
    CAST(BP.PRECO_UNITARIO AS DECIMAL(20,2)) AS PRECO,
    CAST(BP.DATA_VENDA AS DATE) AS DATA_PEDIDO
FROM bronze.estabelecimento be
    INNER JOIN BRONZE.PEDIDOS BP ON BP.ESTABELECIMENTOID = BE.ESTABELECIMENTOID
"""
)

# COMMAND ----------

# Comentario Tabela
comentario_tabela = 'Esta tabela é uma entidade corporativa que contém a relação dos pedidos'

lista_comentario_colunas = {
    'ID_PEDIDO'          : 'Id do pedido.',
    'ID_ESTABELECIMENTO' : 'Id do estabelecimento.',
    'PRODUTO'            : 'Nome do produto.',
    'QUANTIDADE'         : 'Quantidade do pedido.',
    'PRECO'              : 'Preço do produto unitário.',
    'NOME_ESTABELECIMENTO': 'Nome do estabelecimento.',
    'EMAIL'              : 'E-mail do estabelecimento.',
    'TELEFONE'           : 'Telefone do estabelecimento.',
    'DATA_PEDIDO'        : 'Data do pedido.'
}

# COMMAND ----------

df_pedidos.write \
    .format('delta') \
    .mode('overwrite') \
    .clusterBy("ID_PEDIDO", "ID_ESTABELECIMENTO" ) \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')
adicionaComentariosTabela(database, tabela, comentario_tabela, lista_comentario_colunas) 
print('Dados gravados com sucesso')  

# COMMAND ----------

spark.sql(f"OPTIMIZE {database}.{tabela}")
print(f"Processo de otimização finalizado!")