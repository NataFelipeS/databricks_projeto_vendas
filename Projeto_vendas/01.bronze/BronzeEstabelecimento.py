# Databricks notebook source
# MAGIC %run /Projeto_Vendas/00.config/Configuracoes

# COMMAND ----------

# Importações
from pyspark.sql.functions import current_date

# COMMAND ----------

# Nome do database e tabela
database = 'bronze'
tabela = 'estabelecimento' 

# Caminho do Arquivo
caminho_do_arquivo = 'dbfs:/FileStore/tables/projeto_vendas/estabelecimentos.csv'

# COMMAND ----------

# Extract
df = spark.read.format('csv').option("header", True).load(caminho_do_arquivo)

# COMMAND ----------

df_com_data = df.withColumn("data_hoje", current_date())

# COMMAND ----------

df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')
print('Dados gravados com Sucesso')