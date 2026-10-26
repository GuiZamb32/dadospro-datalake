# Aula 02 — Camada Silver | DadosPro

## 1. Objetivo

Implementar a camada Silver do datalake DadosPro utilizando PySpark, transformando os eventos da Bronze em dados tipados, validados e preparados para análises posteriores.

## 2. Implementação

O job `pipeline/silver.py` contempla os seguintes requisitos do projeto:

- **R5 — Deduplicação:** remoção de duplicatas por `event_id`.
- **R6 — Tipagem:** extração dos parâmetros para colunas próprias, incluindo `element_id`.
- **R7 — Validação:** separação de eventos inválidos com seus respectivos motivos de rejeição.
- **R8 — Sessionização:** cálculo de sessões utilizando uma pausa superior a 30 minutos, sem utilizar `session_hint`.
- **R9 — Identity stitching:** propagação do `user_id` aos eventos do mesmo navegador cuja identidade é conhecida.
- **R10 — Explosão de itens:** transformação de `items[]` em linhas individuais.

Os dados são processados pelo Spark e gravados em Parquet no bucket S3 local, utilizando o Floci.

## 3. Resultados da execução

| Indicador | Resultado |
|---|---:|
| Registros na Bronze | 64.372 |
| Registros após deduplicação | 63.716 |
| Duplicatas removidas | 656 |
| Eventos aprovados na Silver | 61.813 |
| Eventos distintos rejeitados | 1.903 |
| Linhas na tabela de itens | 24.427 |
| Sessões identificadas | 8.793 |
| Navegadores com identidade conhecida | 447 |

### Rejeições por motivo

| Motivo | Quantidade |
|---|---:|
| Evento fora do catálogo | 271 |
| Marco fora do padrão | 272 |
| Reembolso sem compra válida | 32 |
| Timestamp ausente ou futuro | 948 |
| Transação sem ID | 31 |
| Valor monetário inválido | 349 |

## 4. Validações realizadas

Foram realizadas consultas no editor Athena do explorador do datalake para verificar:

- Unicidade de `event_id` nos eventos aprovados.
- Ausência de eventos aprovados sem `session_id`.
- Propagação de identidade para navegadores conhecidos.
- Consistência das fronteiras de sessão em relação à regra dos 30 minutos.
- Distribuição dos eventos por nome.

As consultas de validação de identidade e sessão apresentaram zero ocorrências nas condições verificadas que indicariam inconsistências.

## 5. Estrutura de saída

- `silver/event_name=.../`: eventos aprovados, particionados por nome do evento e data.
- `silver/itens/`: itens extraídos dos eventos.
- `silver/eventos_rejeitados/`: eventos rejeitados com motivo registrado.

## 6. Execução

Na raiz do repositório, execute:

```powershell
docker compose run --rm app python pipeline/silver.py
```

## 7. Evidências da entrega

- `01_bucket_silver.png`: estrutura da camada Silver no S3 local.
- `02_data_catalog_schema.png`: esquema da tabela de eventos Silver.
- `03_etl_job_sucesso.png`: execução do job com status de sucesso.
- `04_consulta_athena.png`: consulta de eventos por nome.
- `05_resultado_athena.csv`: exportação dos resultados da consulta.

Projeto: DadosPro — Engenharia de Dados  
Camada: Silver  
Tecnologia principal: PySpark e Parquet
