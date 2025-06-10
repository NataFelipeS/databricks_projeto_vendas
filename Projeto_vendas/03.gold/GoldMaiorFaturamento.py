# Databricks notebook source
# MAGIC %md
# MAGIC ## Informações Gerais
# MAGIC
# MAGIC | Informações     | Detalhes                            |
# MAGIC |-----------------|-------------------------------------|
# MAGIC | Nome Tabela     | gold.maior_faturamento |
# MAGIC | Origem          | silver.pedidos |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC
# MAGIC | Data       | Desenvolvido por | Motivo                    |
# MAGIC |------------|------------------|---------------------------|
# MAGIC | 09/06/2025 | Natã Felipe    | Criação do notebook  |
# MAGIC | 09/06/2025 | Natã Felipe    | GoldMaiorFaturamento Finalizada  |
# MAGIC

# COMMAND ----------

database = "gold"
tabela = "maior_faturamento"

# COMMAND ----------

def adicionaComentariosTabela(a,b,c,d):
    spark.sql(f"COMMENT ON TABLE {a}.{b} IS '{c}'")
    for key, value in d.items():
        sqlaux = f"ALTER TABLE {a}.{b} CHANGE COLUMN {key} COMMENT '{value}'"
        spark.sql(sqlaux)

# COMMAND ----------

comentario_tabela = 'Esta tabela é uma entidade corporativa que contém a relação dos pedidos'

lista_comentario_colunas = {
    'PRODUTO' : 'Nome do produto.',
    'FATURAMENTO'         : 'Valor total vendido do produto.',
}

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.pedidos

# COMMAND ----------

df_mais_vendidos = spark.sql(f"""
SELECT
    PRODUTO,
    SUM(QUANTIDADE * PRECO) AS FATURAMENTO
FROM SILVER.PEDIDOS
GROUP BY 
    PRODUTO
ORDER BY FATURAMENTO DESC
"""
)

# COMMAND ----------

df_mais_vendidos.display()

# COMMAND ----------

df_mais_vendidos.write \
    .format('delta') \
    .mode('overwrite') \
    .clusterBy("PRODUTO" ) \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')
adicionaComentariosTabela(database, tabela, comentario_tabela, lista_comentario_colunas) 
print('Dados gravados com suceeso')  

# COMMAND ----------

spark.sql(f"OPTIMIZE {database}.{tabela}")
print(f"Processo de otimização finalizado!")