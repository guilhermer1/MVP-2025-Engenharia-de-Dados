# MVP — Pipeline de dados da La Liga: revisão para entrega

**Autor:** Guilherme Mendes Ribeiro  
**Base:** `matches_full.csv` (4.318 linhas, 29 colunas; temporadas rotuladas 2020–2025).  
**Objetivo:** construir no Databricks um fluxo de ingestão, transformação, modelagem, validação e análise de estatísticas de futebol.

## Perguntas e resultados

| Pergunta | Método | Resultado e interpretação |
| --- | --- | --- |
| Maior posse se associa à vitória? | Correlação entre `poss` e vitória binária | r ≈ 0,0764: associação linear muito fraca. Não demonstra causalidade. |
| Posse se associa a finalizações e gols? | Correlações de Pearson | `poss × sh` = 0,4383; `poss × sot` = 0,2765; `poss × gf` = 0,1157; `sh × gf` = 0,2527; `sot × gf` = 0,5516. Chutes totais e chutes no alvo são métricas diferentes. |
| As estatísticas distinguem vitórias? | Regressão logística e baseline de classe majoritária | Teste na temporada 2025: acurácia 67,6% × 63,9%; acurácia balanceada 59,9% × 50,0%. Ganho modesto, sem sustentar suficiência ou previsão anterior à partida. |

## Fonte e granularidade

O arquivo veio da distribuição [Kaggle — LaLiga Matches Dataset 2019–2025, FBref](https://www.kaggle.com/datasets/marcelbiezunski/laliga-matches-dataset-2019-2025-fbref), por download e envio manual a um Databricks Volume. Cada registro descreve uma **equipe por partida**, e não necessariamente uma partida única. Confirmar licença e condições de redistribuição do Kaggle e da fonte original antes de publicar dados derivados ou reutilizar o CSV fora do contexto acadêmico.

## Arquitetura implementada e ajuste necessário

| Etapa | Responsabilidade | Estado do script original |
| --- | --- | --- |
| Origem | CSV preservado no Volume | Presente; caminho fixo configurado no notebook. |
| Bronze | Persistir campos e valores de origem sem alterações | O script renomeia as colunas antes de escrever Delta. Mover a normalização para a Silver. |
| Silver | Normalizar nomes, tipar, tratar nulos e derivar resultado | Tipagem e derivação implementadas; verificar registros inválidos antes da Gold. As colunas originais ainda permanecem aqui. |
| Gold | Selecionar campos para consumo | `mvp_gold.fato_partidas` contém dez colunas; seleção ocorre nesta etapa. |
| Qualidade e análise | Consultas SQL, IQR, hipóteses | Presente, com correções de interpretação abaixo. |

A Gold possui uma tabela fato desnormalizada; **não existe esquema estrela completo**. Dimensões de equipe, temporada e calendário são uma possível evolução, sujeita à definição de chaves e granularidade.

## Qualidade e limites

Verificar nulos em todos os campos críticos; `0 <= poss <= 100`; `0 <= sot <= sh`; gols não negativos; e coerência entre `gf`, `ga` e `result`. Se `gf` ou `ga` estiver nulo, não atribuir `DRAW` por padrão: sinalizar e tratar antes de derivar o resultado. O limite de dez gols é um alerta de plausibilidade, não uma regra absoluta de validade.

A consulta SQL de IQR no notebook original avalia somente `poss`; a função PySpark avalia `poss`, `sh`, `sot`, `gf` e `ga` com quartis aproximados. A afirmação genérica de que não há outliers deve ser removida. Reproduzindo IQR com quartis do CSV, há 0 em `poss`, 78 em `sh`, 80 em `sot`, 15 em `gf` e 15 em `ga`. As contagens podem variar com `approxQuantile` e outros métodos de percentil. Investigar registros atípicos antes de qualquer exclusão.

## Experimento preditivo reproduzível

- Alvo: `win = 1` se `gf > ga`; demais resultados recebem 0.
- Entradas: `poss`, `sh`, `sot`, com remoção de linhas incompletas nas colunas do experimento.
- Treino: temporadas 2020–2024 (3.800 linhas); teste: 2025 (518 linhas). Padronização ajustada **somente no treino**; regressão logística com `max_iter=1000`.
- Baseline: sempre prever “não vitória” no teste.

| Medida no teste | Modelo | Baseline |
| --- | ---: | ---: |
| Acurácia | 0,6757 | 0,6390 |
| Acurácia balanceada | 0,5985 | 0,5000 |
| Precisão da vitória | 0,5941 | 0,0000 |
| Revocação da vitória | 0,3209 | 0,0000 |
| F1 da vitória | 0,4167 | 0,0000 |

Matriz de confusão do modelo (`[[VN, FP], [FN, VP]]`): `[[290, 41], [127, 60]]`. A amostra contém registros de duas equipes de uma mesma partida; o corte por temporada impede misturar temporadas, mas uma análise futura deve construir identificadores de partida, verificar pares e testar robustez em múltiplos períodos. Como as entradas são estatísticas obtidas durante o jogo, o teste **não prediz resultados antes da partida**.

## Correções editoriais antes da entrega

1. Gravar a Bronze antes de renomear colunas; ler a Bronze na criação da Silver.
2. Ajustar a descrição da Silver: a seleção de campos acontece na Gold.
3. Substituir a declaração de esquema estrela implementado pela descrição da tabela fato atual.
4. Na segunda hipótese, calcular e identificar separadamente `sh` e `sot`.
5. Incluir o experimento preditivo no notebook executável e registrar os resultados reais da execução no Databricks.
6. Atualizar README e registrar fonte, direitos de uso, etapas, execução e limitações.

**Critério do trabalho:** objetivo, coleta documentada, nuvem, transformação/modelagem e resposta a ao menos parte das perguntas. A correção metodológica das conclusões é tão relevante quanto a execução do pipeline.
