# Databricks notebook source
database = 'bronze'
tabela = 'pedidos'

caminho_arquivo = 'dbfs:/FileStore/tables/projeto_vendas/pedidos.csv'

# COMMAND ----------

df = spark.read.format('csv').option("header", True).load(caminho_arquivo)

# COMMAND ----------

df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')
print('Dados gravados com sucesso!')