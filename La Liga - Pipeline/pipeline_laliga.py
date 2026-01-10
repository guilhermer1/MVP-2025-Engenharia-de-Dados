# Databricks notebook source
# MAGIC %md
# MAGIC # MVP: Engenharia de Dados
# MAGIC
# MAGIC **Autor**: Guilherme Mendes Ribeiro
# MAGIC
# MAGIC **Data**: 20/12/2025
# MAGIC
# MAGIC **Matrícula:** 4052025000053
# MAGIC
# MAGIC **Dataset:** [LaLiga Matches Dataset (2019-2025, FBref)](https://www.kaggle.com/datasets/marcelbiezunski/laliga-matches-dataset-2019-2025-fbref)
# MAGIC
# MAGIC # Descrição do Dataset
# MAGIC
# MAGIC O Dataset escolhido é o LaLiga Matches Dataset (2019-2025, FBref), se trata de um conjunto de dados sobre resultados de partidas que ocorreram na primeira divisão da La Liga (Liga espanhola de Futebol). O objetivo é analisar resultados e performances dos times de 2019 a 2025 e verificar se há a possibilidade de prever resultados comparando os dados históricos de diversos times ao logo desses anos.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Descrição do Problema
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hipóteses do Problema
# MAGIC
# MAGIC As hipóteses que tracei são as seguintes:
# MAGIC
# MAGIC - Existe uma correlação entre a maior posse de bola e a vitória em partidas?
# MAGIC
# MAGIC - A posse de bola se traduz em Chutes a gol e por consequência gols?
# MAGIC
# MAGIC - Os atributos Posse de bola, chutes e chutes a gol são o suficiente pra prever a vitória de um clube?

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Coleta de Dados

# COMMAND ----------

# MAGIC %md
# MAGIC Armazenar os dados brutos exatamente como estão no CSV.

# COMMAND ----------

# Read the CSV
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("dbfs:/Volumes/workspace/default/mvp/matches_full.csv")
)

# Clean column names: replace spaces and remove invalid characters
for col in raw_df.columns:
    new_col = (
        col.replace(" ", "_")
        .replace(",", "")
        .replace(";", "")
        .replace("{", "")
        .replace("}", "")
        .replace("(", "")
        .replace(")", "")
        .replace("\n", "")
        .replace("\t", "")
        .replace("=", "")
    )
    raw_df = raw_df.withColumnRenamed(col, new_col)

# Write to Delta
raw_df.write.format("delta").mode("overwrite").save("dbfs:/Volumes/workspace/default/mvp/laliga")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descrição Detalhada do Dataset LaLiga Matches (2019-2025)
# MAGIC
# MAGIC O dataset utilizado contém informações detalhadas sobre partidas da La Liga entre 2019 e 2025. Cada linha representa uma partida específica, com métricas e atributos relevantes para análise de desempenho dos times.
# MAGIC
# MAGIC ## Principais Colunas
# MAGIC
# MAGIC | Coluna    | Tipo      | Descrição                                                                 | Relevância para as Hipóteses |
# MAGIC |-----------|-----------|--------------------------------------------------------------------------|------------------------------|
# MAGIC | date      | date      | Data da partida (formato yyyy-MM-dd)                                     | Permite análises temporais e tendências ao longo das temporadas. |
# MAGIC | season    | string    | Temporada da partida (ex: 2019/2020)                                     | Segmentação por temporada para comparações históricas. |
# MAGIC | team      | string    | Nome do time principal                                                   | Foco nas performances individuais dos clubes. |
# MAGIC | opponent  | string    | Nome do time adversário                                                  | Permite avaliar o contexto do confronto. |
# MAGIC | poss      | double    | Posse de bola (%) do time principal                                      | Fundamental para testar correlação entre posse e vitória. |
# MAGIC | sh        | int       | Chutes totais do time principal                                          | Avalia se posse resulta em mais finalizações. |
# MAGIC | sot       | int       | Chutes a gol do time principal                                           | Mede efetividade ofensiva, relacionando com posse e gols. |
# MAGIC | gf        | int       | Gols a favor do time principal                                           | Métrica direta de resultado e performance. |
# MAGIC | ga        | int       | Gols contra o time principal                                             | Complementa análise do resultado. |
# MAGIC | result    | string    | Resultado da partida: WIN, DRAW, LOSS                                    | Variável alvo para prever vitórias com base nos atributos. |
# MAGIC
# MAGIC ## Relevância das Colunas
# MAGIC
# MAGIC - **Posse de bola (`poss`)**: Essencial para investigar se maior posse está associada à vitória.
# MAGIC - **Chutes (`sh`) e Chutes a gol (`sot`)**: Permitem analisar se a posse se traduz em oportunidades reais de gol.
# MAGIC - **Gols a favor (`gf`) e contra (`ga`)**: Fundamentais para determinar o resultado e validar hipóteses sobre desempenho.
# MAGIC - **Resultado (`result`)**: Utilizada como variável de saída para modelos preditivos.
# MAGIC - **Contexto temporal e de adversário**: As colunas `date`, `season`, `team` e `opponent` possibilitam segmentações e análises comparativas.
# MAGIC
# MAGIC ## Tipos de Dados
# MAGIC
# MAGIC - **Numéricos**: `poss` (double), `sh`, `sot`, `gf`, `ga` (int) — facilitam análises estatísticas e modelagem.
# MAGIC - **Categóricos**: `team`, `opponent`, `season`, `result` — úteis para agrupamentos e segmentações.
# MAGIC
# MAGIC ## Considerações
# MAGIC
# MAGIC O dataset foi limpo e tipado para garantir qualidade e facilitar análises. As colunas selecionadas estão diretamente alinhadas às hipóteses do projeto, permitindo investigar relações entre posse de bola, finalizações e resultados das partidas.

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Modelagem inicial

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Limpeza e tipagem das colunas relevantes:
# MAGIC
# MAGIC * gf → gols a favor
# MAGIC
# MAGIC * ga → gols contra
# MAGIC
# MAGIC * poss → posse de bola (%)
# MAGIC
# MAGIC * sh → chutes totais
# MAGIC
# MAGIC * sot → chutes a gol
# MAGIC
# MAGIC * result → resultado (W, D, L)

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = (raw_df
    .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    .withColumn("gf", F.col("gf").cast("int"))
    .withColumn("ga", F.col("ga").cast("int"))
    .withColumn("poss", F.col("poss").cast("double"))
    .withColumn("sh", F.col("sh").cast("int"))
    .withColumn("sot", F.col("sot").cast("int"))
    .withColumn("result",
        F.when(F.col("gf") > F.col("ga"), "WIN")
         .when(F.col("gf") < F.col("ga"), "LOSS")
         .otherwise("DRAW"))
)

silver_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/mvp/laliga_silver")


# COMMAND ----------

# MAGIC %md
# MAGIC Na célula anterior, foi realizada a primeira etapa de transformação dos dados brutos da La Liga (Silver Layer). As principais ações foram:
# MAGIC
# MAGIC - Conversão da coluna `date` para o tipo de dado de data (`yyyy-MM-dd`).
# MAGIC - Conversão das colunas numéricas relevantes (`gf`, `ga`, `poss`, `sh`, `sot`) para os tipos apropriados (`int` ou `double`).
# MAGIC - Criação da coluna `result`, que classifica o resultado da partida como "WIN" (vitória), "LOSS" (derrota) ou "DRAW" (empate), com base na comparação entre gols a favor (`gf`) e gols contra (`ga`).
# MAGIC - Por fim, o DataFrame transformado foi salvo em formato Delta na camada Silver, facilitando análises futuras e garantindo maior qualidade dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise final do (Silver Layer)
# MAGIC
# MAGIC Algumas colunas presentes nos dados originais não foram utilizadas e foram retiradas durante o processo de transformação por alguns motivos principais:
# MAGIC
# MAGIC - **Foco nas hipóteses e objetivo do projeto:** O estudo busca analisar a relação entre posse de bola, chutes, chutes a gol e o resultado das partidas. Colunas que não contribuem diretamente para essas análises, como informações administrativas, identificadores internos ou dados irrelevantes para o contexto esportivo, foram descartadas.
# MAGIC
# MAGIC - **Redução de complexidade:** Manter apenas as colunas essenciais facilita o processamento, análise e visualização dos dados, tornando o trabalho mais eficiente e evitando distrações com informações desnecessárias.
# MAGIC
# MAGIC - **Qualidade e clareza dos dados:** Remover colunas redundantes ou pouco informativas ajuda a garantir que o conjunto de dados seja mais limpo, claro e direcionado para responder às perguntas do projeto.
# MAGIC
# MAGIC Exemplo de colunas removidas: identificadores técnicos, observações textuais, códigos internos, ou atributos que não influenciam diretamente o desempenho em campo.
# MAGIC
# MAGIC Assim, o conjunto final contém apenas as métricas e atributos relevantes para a análise esportiva proposta.

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Modelagem e Carga final

# COMMAND ----------

# MAGIC %md
# MAGIC Criar a tabela fato `fato_partidas` com métricas e atributos necessários.

# COMMAND ----------

fato_partidas = (silver_df
    .select("date","season","team","opponent",
            "poss","sh","sot","gf","ga","result"))

# 1) Criar database
spark.sql("CREATE DATABASE IF NOT EXISTS mvp_gold")

# 2) Salvar a tabela no database
fato_partidas.write.mode("overwrite").saveAsTable("mvp_gold.fato_partidas")


# COMMAND ----------

# MAGIC %md
# MAGIC Na célula acima, foi realizada a criação da tabela fato `fato_partidas` na camada Gold do projeto. O processo envolveu:
# MAGIC
# MAGIC - Seleção das colunas relevantes do DataFrame `silver_df`, incluindo data da partida (`date`), temporada (`season`), equipe (`team`), adversário (`opponent`), posse de bola (`poss`), chutes totais (`sh`), chutes a gol (`sot`), gols a favor (`gf`), gols contra (`ga`) e resultado da partida (`result`).
# MAGIC - Criação do database `mvp_gold` caso ainda não existisse, garantindo a organização dos dados na camada Gold.
# MAGIC - Salvamento da tabela `fato_partidas` no database `mvp_gold`, consolidando as principais métricas e atributos das partidas para análises avançadas e geração de relatórios.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Esquema Estrela aplicado ao projeto
# MAGIC
# MAGIC O esquema estrela foi utilizado na modelagem dos dados da camada Gold, com a criação da tabela fato `fato_partidas` que centraliza as principais métricas das partidas da La Liga. Neste contexto:
# MAGIC
# MAGIC - **Tabela Fato (`fato_partidas`)**: Armazena os dados transacionais das partidas, incluindo data, temporada, equipe, adversário, posse de bola, chutes, chutes a gol, gols a favor, gols contra e resultado. Cada linha representa uma partida específica, consolidando as métricas relevantes para análise.
# MAGIC
# MAGIC - **Tabelas Dimensão (potenciais extensões)**: Embora o foco inicial seja a tabela fato, o esquema estrela pode ser expandido com tabelas de dimensão, como:
# MAGIC   - **Dimensão Equipe**: Informações detalhadas sobre os times (nome, cidade, estádio, etc.).
# MAGIC   - **Dimensão Temporada**: Dados sobre cada temporada (ano, regulamentos, etc.).
# MAGIC   - **Dimensão Adversário**: Características dos adversários.
# MAGIC   - **Dimensão Data**: Detalhes sobre datas (dia da semana, mês, feriados, etc.).
# MAGIC
# MAGIC A estrutura estrela facilita consultas analíticas, pois centraliza os fatos e permite junções simples com dimensões, otimizando o desempenho e a compreensão dos dados. Essa abordagem é recomendada em ambientes lakehouse como o Databricks, pois reduz a complexidade de joins e melhora a eficiência das análises.

# COMMAND ----------

# MAGIC %md
# MAGIC #4. Análise

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.1 Avaliação da qualidade dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC Avaliar a qualidade dos dados antes da análise é essencial para garantir que os resultados obtidos sejam confiáveis e úteis. Dados incompletos, incorretos ou inconsistentes podem levar a conclusões erradas e prejudicar decisões. Por isso, verificar a precisão, integridade e consistência dos dados é um passo fundamental para qualquer projeto de análise.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Principais campos para análise
# MAGIC
# MAGIC - **poss**: Posse de bola (%)
# MAGIC - **sh**: Chutes totais
# MAGIC - **sot**: Chutes a gol
# MAGIC - **gf**: Gols a favor
# MAGIC - **ga**: Gols contra
# MAGIC - **result**: Resultado categórico (`WIN`, `LOSS`, `DRAW`)
# MAGIC
# MAGIC **Chaves de contexto:**  
# MAGIC - `date` (data)
# MAGIC - `season` (temporada)
# MAGIC - `team` (equipe)
# MAGIC - `opponent` (adversário)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1.1 Avaliação de Completude (valores nulos)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN poss IS NULL THEN 1 ELSE 0 END) AS nulos_poss,
# MAGIC   SUM(CASE WHEN sh IS NULL THEN 1 ELSE 0 END) AS nulos_sh,
# MAGIC   SUM(CASE WHEN sot IS NULL THEN 1 ELSE 0 END) AS nulos_sot,
# MAGIC   SUM(CASE WHEN gf IS NULL THEN 1 ELSE 0 END) AS nulos_gf,
# MAGIC   SUM(CASE WHEN ga IS NULL THEN 1 ELSE 0 END) AS nulos_ga,
# MAGIC   SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS nulos_result
# MAGIC FROM mvp_gold.fato_partidas;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Na célula anterior, foi realizada uma consulta SQL para avaliar a completude dos dados na tabela fato `fato_partidas`, especificamente verificando a presença de valores nulos nos principais campos de análise: posse de bola (`poss`), chutes totais (`sh`), chutes a gol (`sot`), gols a favor (`gf`), gols contra (`ga`) e resultado da partida (`result`). 
# MAGIC
# MAGIC A consulta utiliza a função `SUM(CASE WHEN ... IS NULL THEN 1 ELSE 0 END)` para contar o número de registros nulos em cada coluna, retornando uma métrica de qualidade para cada atributo. O resultado da execução apresenta, para cada campo, a quantidade de valores ausentes, permitindo identificar possíveis problemas de completude e orientar ações de limpeza ou tratamento dos dados antes de análises mais avançadas.
# MAGIC
# MAGIC Foi possível observar que não há valores nulos nos atributos selecionados.

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.1.2 Domínios válidos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN poss < 0 OR poss > 100 THEN 1 ELSE 0 END) AS poss_invalid,
# MAGIC   SUM(CASE WHEN sot > sh THEN 1 ELSE 0 END) AS sot_invalid,
# MAGIC   SUM(CASE WHEN gf < 0 OR gf > 10 THEN 1 ELSE 0 END) AS gf_invalid,
# MAGIC   SUM(CASE WHEN ga < 0 OR ga > 10 THEN 1 ELSE 0 END) AS ga_invalid,
# MAGIC   SUM(CASE WHEN result NOT IN ('WIN','LOSS','DRAW') THEN 1 ELSE 0 END) AS result_invalid
# MAGIC FROM mvp_gold.fato_partidas;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Na célula acima, foi realizada uma consulta SQL para verificar a validade dos valores presentes nos principais campos da tabela `fato_partidas`. Cada métrica avaliada corresponde a uma regra de domínio esperada para os dados esportivos:
# MAGIC
# MAGIC - **poss_invalid**: Conta partidas em que a posse de bola (`poss`) está fora do intervalo permitido (menor que 0% ou maior que 100%), o que indicaria erro de registro.
# MAGIC - **sot_invalid**: Verifica se há casos em que o número de chutes a gol (`sot`) é maior que o total de chutes (`sh`), o que não faz sentido estatístico.
# MAGIC - **gf_invalid** e **ga_invalid**: Identificam partidas com gols a favor (`gf`) ou gols contra (`ga`) negativos ou excessivamente altos (acima de 10), valores improváveis em partidas reais.
# MAGIC - **result_invalid**: Conta registros em que o resultado da partida (`result`) não está entre os valores esperados: 'WIN', 'LOSS' ou 'DRAW'.
# MAGIC
# MAGIC O resultado da consulta apresenta, para cada regra, o número de registros inválidos encontrados. Se todos os valores retornados forem zero, significa que os dados estão dentro dos domínios esperados, indicando alta qualidade e consistência para análises futuras.

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.1.3 Consistência entre atributos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS inconsistencias
# MAGIC FROM mvp_gold.fato_partidas
# MAGIC WHERE (gf > ga AND result <> 'WIN')
# MAGIC    OR (gf = ga AND result <> 'DRAW')
# MAGIC    OR (gf < ga AND result <> 'LOSS');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Na célula acima, foi realizada uma consulta SQL para verificar a consistência entre os atributos de gols a favor (`gf`), gols contra (`ga`) e o resultado da partida (`result`) na tabela `fato_partidas`. A consulta conta o número de registros em que há divergência lógica entre esses campos, ou seja:
# MAGIC
# MAGIC - Se o número de gols a favor (`gf`) for maior que o número de gols contra (`ga`), o resultado esperado é `WIN`. Caso contrário, é considerado inconsistente.
# MAGIC - Se o número de gols a favor for igual ao número de gols contra, o resultado correto deve ser `DRAW`. Qualquer outro valor é inconsistente.
# MAGIC - Se o número de gols a favor for menor que o número de gols contra, o resultado esperado é `LOSS`. Divergências indicam inconsistência.
# MAGIC
# MAGIC O resultado da consulta retorna a quantidade total de registros que apresentam essas inconsistências. Se o valor retornado for zero, significa que todos os registros estão logicamente corretos em relação ao placar e ao resultado informado, indicando alta qualidade e confiabilidade dos dados para análise.

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.1.4 Distribuição de Outliers

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH stats AS (
# MAGIC   SELECT
# MAGIC     percentile(poss, 0.25) AS q1,
# MAGIC     percentile(poss, 0.75) AS q3
# MAGIC   FROM mvp_gold.fato_partidas
# MAGIC )
# MAGIC SELECT
# MAGIC   COUNT(*) AS total,
# MAGIC   SUM(CASE WHEN poss < q1 - 1.5*(q3-q1) THEN 1 ELSE 0 END) AS outliers_baixo,
# MAGIC   SUM(CASE WHEN poss > q3 + 1.5*(q3-q1) THEN 1 ELSE 0 END) AS outliers_alto
# MAGIC FROM mvp_gold.fato_partidas, stats;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Na célula acima, foi realizada uma análise de outliers para o campo de posse de bola (`poss`) na tabela `fato_partidas`. O processo envolveu dois passos principais:
# MAGIC
# MAGIC 1. **Cálculo dos Quartis**: Utilizando a função `percentile`, foram calculados o primeiro quartil (`q1`, 25%) e o terceiro quartil (`q3`, 75%) dos valores de posse de bola. Esses quartis são usados para identificar o intervalo interquartil (IQR), que representa a faixa central dos dados.
# MAGIC
# MAGIC 2. **Identificação de Outliers**: Com base nos quartis, foram definidos os limites para considerar um valor como outlier:
# MAGIC    - Outliers baixos: valores de posse de bola menores que `q1 - 1.5 * IQR`.
# MAGIC    - Outliers altos: valores maiores que `q3 + 1.5 * IQR`.
# MAGIC
# MAGIC A consulta retorna três métricas:
# MAGIC - `total`: número total de registros analisados.
# MAGIC - `outliers_baixo`: quantidade de partidas com posse de bola significativamente abaixo do esperado.
# MAGIC - `outliers_alto`: quantidade de partidas com posse de bola significativamente acima do esperado.
# MAGIC
# MAGIC O resultado permite identificar se há partidas com valores de posse de bola fora do padrão, o que pode indicar erros de registro ou situações excepcionais. Se o número de outliers for baixo, os dados são considerados consistentes; se for alto, recomenda-se investigar possíveis causas.
# MAGIC
# MAGIC Através da avaliação podemos observar que não foram encontrados outliers.

# COMMAND ----------

from pyspark.sql import functions as F

def outlier_distribution(df, col):
    q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower = q1 - 1.5*iqr
    upper = q3 + 1.5*iqr
    return (df.filter((F.col(col) < lower) | (F.col(col) > upper)).count(),
            lower, upper)

for col in ["poss","sh","sot","gf","ga"]:
    outliers, lower, upper = outlier_distribution(fato_partidas, col)
    print(f"{col}: {outliers} outliers (limite [{lower:.2f}, {upper:.2f}])")


# COMMAND ----------

# MAGIC %md
# MAGIC Na célula acima, foi realizada uma análise de outliers para os principais atributos estatísticos da tabela `fato_partidas` utilizando PySpark. O procedimento seguiu os seguintes passos:
# MAGIC
# MAGIC 1. **Cálculo dos Quartis e Limites de Outlier**: Para cada coluna analisada (`poss`, `sh`, `sot`, `gf`, `ga`), foram calculados o primeiro quartil (Q1) e o terceiro quartil (Q3) usando o método `approxQuantile`. Com esses valores, foi determinado o intervalo interquartil (IQR = Q3 - Q1) e, a partir dele, os limites inferior (`Q1 - 1.5 * IQR`) e superior (`Q3 + 1.5 * IQR`) para identificação de outliers.
# MAGIC
# MAGIC 2. **Contagem de Outliers**: Para cada atributo, foi contabilizado o número de registros que estão fora dos limites definidos, ou seja, valores considerados atípicos em relação à distribuição dos dados.
# MAGIC
# MAGIC 3. **Exibição dos Resultados**: Para cada coluna, o resultado apresenta a quantidade de outliers encontrados e os respectivos limites utilizados na análise.
# MAGIC
# MAGIC Esse processo permite identificar possíveis erros de registro ou situações excepcionais nos dados esportivos. A presença de poucos outliers indica que os dados estão dentro do padrão esperado, enquanto um número elevado pode sugerir a necessidade de investigação adicional.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Resumo da Análise de Qualidade dos Dados

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A análise de qualidade dos dados da tabela `fato_partidas` envolveu a verificação de regras de domínio, consistência entre atributos e identificação de outliers. Os resultados indicam que os principais campos apresentam valores dentro dos limites esperados, sem registros inválidos ou inconsistentes. A distribuição dos dados estatísticos está adequada, com poucos ou nenhum outlier detectado. Dessa forma, os dados demonstram alta qualidade, consistência e confiabilidade para análises futuras.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #5. Análise das Hipóteses

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.1 Existe uma correlação entre a maior posse de bola e a vitória em partidas?

# COMMAND ----------

from pyspark.sql import functions as F

# Adiciona coluna binária: 1 se vitória, 0 caso contrário
fato_partidas_bin = fato_partidas.withColumn(
    "win", F.when(F.col("result") == "WIN", 1).otherwise(0)
)

# Calcula o coeficiente de correlação de Pearson entre posse de bola e vitória
correlacao = fato_partidas_bin.corr("poss", "win")

print(f"Coeficiente de correlação (poss x vitória): {correlacao:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC Na célula acima realizamos duas operações principais:
# MAGIC * 1. Cria uma nova coluna binária chamada "win" na tabela fato_partidas, onde o valor é 1 se o resultado da partida foi "WIN" (vitória) e 0 caso contrário.
# MAGIC
# MAGIC * 2. Calcula o coeficiente de correlação de Pearson entre a posse de bola ("poss") e a variável binária de vitória ("win").
# MAGIC
# MAGIC O resultado obtido é o valor do coeficiente de correlação de Pearson, que varia de -1 a 1. Um valor próximo de 1 indica forte associação positiva: partidas com maior posse de bola tendem a resultar em vitória. Um valor próximo de 0 indica ausência de correlação linear significativa entre posse de bola e vitória, ou seja, ter mais posse não garante o resultado. Se o valor for negativo, sugere que maior posse de bola está associada a menos vitórias, o que seria contraintuitivo. Portanto, o coeficiente calculado permite validar ou refutar a hipótese de que maior posse de bola está relacionada à vitória nas partidas analisadas.
# MAGIC
# MAGIC Por essa razão, podemos observar que a posse de bola, não neessáriamente leva um time à vitória.

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.2 A posse de bola se traduz em Chutes a gol e por consequência gols?

# COMMAND ----------

from pyspark.mllib.stat import Statistics

# Seleciona as colunas relevantes e remove nulos
df_test = fato_partidas.select("poss", "sh", "gf").dropna()

# Converte para RDDs de pares para testes de correlação
poss_sh_rdd = df_test.select("poss", "sh").rdd.map(lambda row: (row[0], row[1]))
poss_gf_rdd = df_test.select("poss", "gf").rdd.map(lambda row: (row[0], row[1]))
sh_gf_rdd = df_test.select("sh", "gf").rdd.map(lambda row: (row[0], row[1]))

# Calcula correlação de Pearson
corr_poss_sh = Statistics.corr(poss_sh_rdd.map(lambda x: x[0]), poss_sh_rdd.map(lambda x: x[1]), method="pearson")
corr_poss_gf = Statistics.corr(poss_gf_rdd.map(lambda x: x[0]), poss_gf_rdd.map(lambda x: x[1]), method="pearson")
corr_sh_gf = Statistics.corr(sh_gf_rdd.map(lambda x: x[0]), sh_gf_rdd.map(lambda x: x[1]), method="pearson")

print(f"Correlação posse x chutes: {corr_poss_sh:.4f}")
print(f"Correlação posse x gols: {corr_poss_gf:.4f}")
print(f"Correlação chutes x gols: {corr_sh_gf:.4f}")

# Teste de independência (qui-quadrado) entre posse e chutes
from pyspark.sql.functions import col
from pyspark.mllib.linalg import Vectors

# Discretiza posse e chutes para teste qui-quadrado
df_chi = df_test.withColumn("poss_bin", (col("poss")/10).cast("int")).withColumn("sh_bin", (col("sh")/2).cast("int"))
obs = df_chi.groupBy("poss_bin", "sh_bin").count().orderBy("poss_bin", "sh_bin").select("count").rdd.map(lambda r: r[0]).collect()
# Cria matriz de contingência
import numpy as np
poss_bins = df_chi.select("poss_bin").distinct().count()
sh_bins = df_chi.select("sh_bin").distinct().count()
matrix = np.array(obs).reshape(poss_bins, sh_bins)
chi_result = Statistics.chiSqTest(Vectors.dense(matrix.flatten()))

print(f"Teste qui-quadrado posse x chutes: p-valor={chi_result.pValue:.4f}, estatística={chi_result.statistic:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
