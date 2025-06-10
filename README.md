🔷 Pipeline de Dados com Databricks — Arquitetura em Camadas (Bronze, Silver e Gold)
📌 Objetivo
Este projeto tem como objetivo a construção de uma pipeline de dados ponta a ponta utilizando o Databricks, com a aplicação da arquitetura em camadas (medalhão) — Bronze, Silver e Gold —, a fim de responder a três perguntas de negócio levantadas pela área comercial:

Quais são as 5 empresas que mais compraram nossos produtos?
Quais são os 5 produtos mais vendidos?
Quais são os 5 produtos que mais geraram faturamento?

🧱 Estrutura do Projeto
A organização do projeto segue a separação modular por pastas dentro do ambiente do Databricks:

/Projeto_vendas
├── 00.configuracoes
│   └── setup_paths, parametros, conexões, esquemas
├── 01.bronze
│   └── ingestão de dados brutos (CSV, Excel, JSON, etc.)
├── 02.silver
│   └── limpeza, normalização e padronização dos dados
├── 03.gold
│   └── transformação analítica e criação de métricas

⚙️ Tecnologias Utilizadas
Databricks (Spark + Delta Lake)
PySpark para transformação de dados
Delta Lake para versionamento e controle de dados
SQL para análises nas camadas Silver e Gold
Visualizações nativas do Databricks

📥 Ingestão (Camada Bronze)
Coleta de arquivos em formatos variados (CSV, Excel, JSON)
Armazenamento dos dados brutos sem modificações
Aplicação de schema estruturado durante a leitura


🧹 Transformação (Camada Silver)
Limpeza de dados duplicados, nulos ou inconsistentes
Conversão de tipos, padronização de nomes e datas
Criação de tabelas intermediárias normalizadas:
vendas, clientes, produtos, itens_venda

📊 Métricas e Respostas (Camada Gold)
A camada Gold foi projetada com o foco em responder às demandas de negócio, agregando e cruzando os dados para gerar insights prontos para consumo.

📈 Resultado Final
As análises estão disponíveis em notebooks interativos com visualizações no próprio Databricks, facilitando a tomada de decisão e a geração de relatórios.

📂 Organização do Código
Pasta	Conteúdo
00.configuracoes	Configuração de variáveis, paths e schemas
01.bronze	Ingestão de dados brutos
02.silver	Limpeza, estruturação e enriquecimento
03.gold	Métricas e respostas aos problemas de negócio, dashboards Visualizações e resultados finais

🧠 Lições Aprendidas
Importância da separação de camadas para rastreabilidade e governança
Uso de Delta Lake para versionamento seguro de dados
Como responder perguntas de negócio com pipelines estruturados
