# MVP de Engenharia de Dados — La Liga

Autor: Guilherme Mendes Ribeiro

Projeto acadêmico de construção de um pipeline de dados no Databricks para analisar a relação entre posse de bola, finalizações, gols e resultados de equipes da La Liga.

## Objetivo

O trabalho parte de três perguntas:

1. Existe correlação entre maior posse de bola e vitória?
2. A posse de bola está relacionada a mais finalizações e gols?
3. Posse, chutes e chutes no alvo ajudam a compreender os resultados das partidas?

O notebook apresenta análises estatísticas das duas primeiras perguntas e uma discussão exploratória da terceira. **Não foi treinado um modelo de previsão de vitórias** neste projeto.

## Dados

O projeto utiliza o [LaLiga Matches Dataset (2019–2025, FBref), disponibilizado no Kaggle](https://www.kaggle.com/datasets/marcelbiezunski/laliga-matches-dataset-2019-2025-fbref). O arquivo `matches_full.csv` incluído no repositório contém 4.318 registros e 29 colunas. Cada linha representa os dados de uma equipe em uma partida.

Entre os campos utilizados estão `date` (data), `season` (temporada), `team` (equipe), `opponent` (adversário), `poss` (posse de bola), `sh` (chutes totais), `sot` (chutes no alvo), `gf` (gols a favor) e `ga` (gols contra).

## Pipeline implementado

1. **Ingestão e camada inicial:** leitura do CSV armazenado em um Databricks Volume com PySpark, ajuste dos nomes das colunas e gravação em Delta no caminho `dbfs:/Volumes/workspace/default/mvp/laliga`.
2. **Silver:** conversão da data e dos principais campos numéricos; cálculo de `result` como `WIN`, `LOSS` ou `DRAW` a partir de `gf` e `ga`; gravação em `/Volumes/workspace/default/mvp/laliga_silver`.
3. **Gold:** seleção das colunas relevantes e criação da tabela `mvp_gold.fato_partidas`.
4. **Qualidade e análise:** consultas SQL para nulos, regras de domínio e consistência; investigação de valores atípicos pelo intervalo interquartil; correlações em PySpark e teste qui-quadrado com SciPy para posse e chutes totais agrupados em faixas.

O projeto utiliza uma tabela fato na Gold. As dimensões citadas no notebook são possibilidades de evolução; não há tabelas dimensão implementadas.

## Análises apresentadas

- A correlação entre posse de bola e vitória binária é aproximadamente **0,0764**, uma associação linear fraca.
- A correlação entre posse e chutes **totais** (`sh`) é aproximadamente **0,4383**; entre posse e gols (`gf`), **0,1157**; entre chutes totais e gols, **0,2527**.
- O teste qui-quadrado avalia a associação entre faixas de posse e faixas de chutes totais.

Essas análises mostram associações nos registros estudados. O código da segunda análise usa `sh` (chutes totais), embora alguns trechos explicativos do notebook se refiram a “chutes a gol”; o campo de chutes no alvo é `sot`. As conclusões sobre previsão na terceira hipótese são interpretações exploratórias das associações, sem avaliação de desempenho preditivo.

## Estrutura do repositório

```text
├── README.md
├── matches_full.csv
├── pipeline_laliga_notebook.ipynb
├── La Liga - Pipeline/
│   └── pipeline_laliga.py
└── MVP - Documentos/
    ├── Catalog_mvp_gold.jpg
    └── Pastas_databricks.png
```

O arquivo `.py` é a exportação do notebook Databricks. O repositório não inclui arquivos YAML nem configuração de agendamento do pipeline.

## Como executar

1. Disponibilize `matches_full.csv` em `dbfs:/Volumes/workspace/default/mvp/matches_full.csv` no Databricks.
2. Importe `pipeline_laliga_notebook.ipynb` no ambiente Databricks.
3. Confirme que o ambiente tem permissões de escrita no Volume e para criar a tabela em `mvp_gold`. Ajuste os caminhos no notebook caso sua organização de catálogos seja diferente.
4. Execute as células em sequência. As etapas de escrita utilizam `overwrite` e substituem os dados gravados em execuções anteriores.

**Tecnologias usadas:** Databricks, Python, PySpark, Spark SQL, Delta Lake, Pandas e SciPy.

## Escopo

Este é um MVP acadêmico com ingestão manual de um arquivo estático. O repositório documenta o processamento e as análises no notebook, mas não inclui automação de coleta, execução agendada ou serviço de previsão de resultados.
