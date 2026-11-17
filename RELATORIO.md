# Relatório técnico e analítico — DadosPro

- **Projeto:** Datalake de tracking da DadosPro
- **Repositório:** https://github.com/GuiZamb32/dadospro-datalake
- **Branch de desenvolvimento:** `feature/implementacao-gold`
**Tecnologias:** Python, PySpark 4.2, Spark SQL, Parquet, Docker Compose e armazenamento S3 compatível (Floci)

---

## 1. Objetivo

Construir um datalake para os eventos de navegação e conversão de uma landing page fictícia de um curso online, organizando os dados em arquitetura medalhão (**Bronze, Silver e Gold**). O objetivo é transformar eventos JSON recebidos na landing em tabelas analíticas que respondam a perguntas sobre funil de conversão, engajamento, receita e aquisição, além de gerar previsões de compra e um mapa de calor para a página.

O processamento foi executado em contêineres Docker. Os dados foram lidos e gravados no bucket `datalake-tracking` por meio do Spark e de caminhos `s3a://`.

## 2. Arquitetura implementada

O fluxo do projeto é:

```text
Landing (JSON Lines)
        |
        v
      Bronze  -> Parquet, schema explícito, proveniência e append-only
        |
        v
      Silver  -> deduplicação, tipagem, validação, sessões, identidade e itens
        |
        +----> Gold Funil
        +----> Gold Engajamento
        +----> Gold Receita e Aquisição
        +----> Gold Machine Learning
        +----> Gold Mapa de Calor
                         |
                         v
               site/mapa_calor.js
                         |
                         v
                 Visualização no site
```

### 2.1 Bronze

A Bronze preserva os eventos de origem em Parquet, com schema explícito, partições por `event_name/year/month/day` e metadados de proveniência (`source_file` e `ingested_at`). A ingestão é append-only: arquivos de origem já processados não são acrescentados novamente.

Na execução completa registrada, a landing continha **64.372 registros**. A Bronze identificou que todos os arquivos já haviam sido ingeridos e acrescentou **0 novos registros**, mantendo **64.372 registros** no total.

### 2.2 Silver

A Silver executa as seguintes transformações:

- deduplicação por `event_id`;
- extração e tipagem dos campos de `params`;
- validação dos eventos, com registro do motivo de rejeição;
- criação de sessões por visitante, considerando pausa superior a 30 minutos e sem utilizar `session_hint`;
- propagação de `user_id` por identity stitching;
- separação dos itens de `items[]` em `silver/itens/`.

A coluna `element_id` também é extraída para suportar o mapa de calor.

### 2.3 Tabelas Gold

Os cinco jobs Gold criam as 15 tabelas analíticas exigidas:

| Job | Tabelas produzidas |
|---|---|
| Funil | `funil_compra`, `funil_lead` |
| Engajamento | `sessoes`, `retencao_video`, `buscas_faq`, `saidas_downloads` |
| Receita e Aquisição | `receita_diaria`, `taxa_reembolso`, `adesao_order_bump`, `lead_para_compra` |
| Machine Learning | `propensao_sessao`, `metricas_modelo`, `segmentos_visitantes` |
| Mapa de Calor | `mapa_calor_cliques`, `mapa_calor_rolagem` |

O job `exportar_mapa_calor.py` lê as duas tabelas de mapa de calor pelo Spark e grava o arquivo local `site/mapa_calor.js`, consumido pelo `site/index.html`.

## 3. Resultados e respostas às perguntas de negócio

### 3.1 Funil — em que etapa a jornada de compra perde mais pessoas?

A tabela `gold.funil_compra` analisou **8.793 sessões**. No escopo total, as contagens por etapa foram:

| Etapa | Sessões | Conversão sobre a etapa anterior |
|---|---:|---:|
| `view_item_list` | 5.258 | — |
| `select_item` | 1.719 | 32,69% |
| `add_to_cart` | 1.291 | 75,10% |
| `view_cart` | 1.255 | 97,21% |
| `begin_checkout` | 912 | 72,67% |
| `add_payment_info` | 684 | 75,00% |
| `purchase` | 573 | 83,77% |

A conversão do topo do funil até a compra foi de **10,90%** (573 de 5.258 sessões que visualizaram a lista de planos).

A maior perda ocorreu entre `view_item_list` e `select_item`: **3.539 sessões**, ou aproximadamente **67,31%** das sessões que visualizaram os planos, não registraram seleção. Entre as etapas seguintes, a maior queda proporcional foi de `view_cart` para `begin_checkout` (27,33%).

A tabela `gold.funil_lead` registrou **8.382 sessões** com visualização da landing, **5.892** com rolagem de pelo menos 50% e **522** com `generate_lead`. A conversão do topo até lead foi de **6,23%**.

**Recomendação:** priorizar testes na apresentação e comparação dos planos, nos textos e nos CTAs que levam à seleção. Em paralelo, revisar a transição do carrinho para o início do checkout, que também apresenta uma queda relevante.

### 3.2 Engajamento — o vídeo é assistido até o fim? O que as pessoas procuram no FAQ?

A tabela `gold.retencao_video` considerou **2.890 sessões** com `video_start`:

| Marco do vídeo | Sessões que atingiram | Percentual das sessões com início |
|---|---:|---:|
| 10% | 2.626 | 90,87% |
| 25% | 2.138 | 73,98% |
| 50% | 1.474 | 51,00% |
| 75% | 1.105 | 38,24% |
| 100% | 877 | 30,35% |

Aproximadamente **30,35%** das sessões que iniciaram o vídeo chegaram ao final.

Na tabela `gold.buscas_faq`, os termos com maior volume de buscas sem resultado foram:

| Termo | Buscas | Sem resultado |
|---|---:|---:|
| `estagio` | 44 | 44 |
| `presencial` | 33 | 33 |
| `bolsa` | 32 | 32 |
| `power bi` | 28 | 28 |
| `linux` | 24 | 24 |

Outros termos, como `acesso`, `parcelar`, `certificado`, `reembolso` e `apostila`, tiveram buscas registradas sem buscas sem resultado no resumo apresentado.

A tabela `gold.saidas_downloads` registrou 335 downloads de `ementa-formacao-dados.pdf`. Entre os cliques de saída, foram registrados 102 para Instagram, 92 para LinkedIn, 87 para WhatsApp e 85 para YouTube.

**Recomendação:** avaliar a duração e a organização do vídeo, principalmente a continuidade após os primeiros 25% de reprodução. Atualizar o FAQ com respostas específicas para estágio, modalidade presencial, bolsas, Power BI e Linux, que concentraram as buscas sem resultado.

### 3.3 Receita e aquisição — quanto entrou após reembolsos e quais canais trazem clientes com menor taxa de desistência?

A tabela `gold.receita_diaria` consolidou os pedidos válidos por data de compra, plano e canal. No resultado geral do job foram apurados:

| Indicador | Resultado |
|---|---:|
| Pedidos válidos | 573 |
| Receita bruta | R$ 338.523,10 |
| Receita reembolsada | R$ 28.608,50 |
| **Receita líquida** | **R$ 309.914,60** |

A receita reembolsada correspondeu a aproximadamente **8,45%** do valor bruto. A taxa de reembolso por quantidade de pedidos, agregada a partir das linhas por plano e canal da tabela `gold.taxa_reembolso`, foi de 48 em 573 pedidos (**8,38%**).

A agregação por canal foi:

| Canal | Pedidos | Pedidos reembolsados | Taxa de reembolso |
|---|---:|---:|---:|
| `direct / (none)` | 112 | 2 | 1,79% |
| `newsletter / email` | 74 | 2 | 2,70% |
| `google / organic` | 101 | 9 | 8,91% |
| `google / cpc` | 221 | 20 | 9,05% |
| `instagram / paid_social` | 65 | 15 | 23,08% |

O canal `direct / (none)` apresentou a menor taxa de reembolso no conjunto observado: **1,79%**. A newsletter apresentou a segunda menor taxa, **2,70%**, em 74 pedidos. Já `instagram / paid_social` teve a maior taxa de reembolso, **23,08%**.

Na tabela `gold.adesao_order_bump`, **90 dos 573 pedidos** incluíram a Mentoria em grupo (`BUMP-MENTORIA`), adesão de **15,71%**.

Na tabela `gold.lead_para_compra`, **40 das 522 pessoas com lead** compraram depois do lead, taxa de **7,66%**, com mediana de **4,6354 dias** até a compra. O canal `google / cpc` concentrou 16 compras posteriores entre 165 leads (9,70%), a maior taxa de conversão lead-compra entre os canais mostrados.

**Recomendação:** preservar e estudar as origens direta e newsletter, que apresentaram menor proporção de reembolsos. Investigar a qualidade do tráfego de Instagram pago, que teve a maior taxa de reembolso, sem avaliar o canal somente por volume de pedidos. Para aquisição de leads, `google / cpc` apresentou o maior volume de compras posteriores e a maior conversão lead-compra observada.

### 3.4 Previsão — quais sessões estão próximas de comprar e que perfis de visitante existem?

A tabela `gold.propensao_sessao` contém previsões por sessão. O modelo de classificação utilizado foi **Regressão Logística**, treinado com features comportamentais limitadas a eventos anteriores ao primeiro `add_to_cart`, evitando utilizar eventos posteriores do checkout como preditores.

A divisão registrada foi de **7.069 amostras de treino** e **1.724 amostras de teste**. As métricas no teste foram:

| Métrica | Resultado |
|---|---:|
| AUC | 0,9230 |
| F1 | 0,4677 |
| Precisão | 0,3070 |
| Recall | 0,9813 |

O AUC de 0,9230 indica boa capacidade de ordenação das sessões compradoras acima das não compradoras. O recall elevado (98,13%) indica que o modelo identificou a maior parte das sessões positivas no conjunto de teste. Entretanto, a precisão de 30,70% indica que muitas sessões classificadas como compradoras não se converteram em compra. Portanto, as previsões são mais adequadas para priorização de contatos ou análises do que para decisões automáticas sem revisão.

A tabela `gold.segmentos_visitantes` agrupou **5.994 visitantes em quatro clusters**. Na execução completa registrada, os tamanhos foram 3.558, 1.381, 482 e 573 visitantes. O grupo com 573 visitantes apresentou média de uma compra por visitante, enquanto os outros três grupos apresentaram média de compras igual a zero naquela execução. Os números dos grupos são identificadores técnicos do K-Means, não categorias de negócio predefinidas.

**Recomendação:** utilizar a propensão como sinal de priorização, acompanhando precisão e recall e avaliando outros limiares de classificação antes de automatizar ações. Para os clusters, analisar os perfis médios das features antes de atribuir nomes ou estratégias comerciais a cada grupo.

## 4. Mapa de calor — cliques e rolagem

As tabelas `gold.mapa_calor_cliques` e `gold.mapa_calor_rolagem` foram geradas e exportadas para `site/mapa_calor.js`. O job registrou **24 combinações de página e elemento** e **5 marcos de rolagem**.

Na landing (`pagina = "/"`), os elementos com maior volume de cliques foram:

| Elemento | Cliques | Sessões que clicaram | Taxa de clique por sessão |
|---|---:|---:|---:|
| `btn-play` | 2.890 | 2.890 | 34,48% |
| `cta-hero-ver-planos` | 2.543 | 2.543 | 30,34% |
| `plano-pro` | 1.086 | 836 | 9,97% |
| `plano-basico` | 879 | 647 | 7,72% |
| `modal-comprar` | 749 | 749 | 8,94% |

A rolagem da landing ficou assim:

| Marco | Sessões que chegaram | Percentual das sessões da landing |
|---|---:|---:|
| 0% | 8.382 | 100,00% |
| 25% | 7.362 | 87,83% |
| 50% | 5.822 | 69,46% |
| 75% | 4.219 | 50,33% |
| 90% | 2.177 | 25,97% |

O botão de reprodução do vídeo e o CTA principal de visualização dos planos foram os elementos com maior volume de cliques na landing. Apenas 25,97% das sessões chegaram ao marco de 90% da rolagem.

O arquivo `site/mapa_calor.js` é um artefato local gerado pelo job R11 e está excluído do Git pelo `.gitignore`. O HTML existente carrega esse arquivo e apresenta o mapa quando o usuário ativa o botão **Mapa de calor** no painel lateral.

**Recomendação:** avaliar a posição e a visibilidade dos CTAs e do conteúdo localizado abaixo da metade da página. Usar os resultados do mapa como evidência de interação agregada, não como registro individual de usuários.

## 5. Qualidade do dado

### 5.1 Deduplicação, aprovação e rejeição

A execução da Silver apresentou:

| Indicador | Registros |
|---|---:|
| Registros na Bronze | 64.372 |
| Após deduplicação | 63.716 |
| Duplicatas removidas | 656 |
| Eventos aprovados na Silver | 61.813 |
| Eventos distintos rejeitados | 1.903 |
| Linhas em `eventos_rejeitados` | 1.903 |
| Linhas em `silver/itens` | 24.427 |

### 5.2 Rejeições por motivo

| Motivo | Eventos rejeitados |
|---|---:|
| Timestamp ausente ou futuro | 948 |
| Valor monetário inválido | 349 |
| Evento fora do catálogo | 271 |
| Marco fora do padrão | 272 |
| Reembolso sem compra | 32 |
| Transação sem ID | 31 |
| **Total** | **1.903** |

### 5.3 Armadilhas encontradas e tratamentos

1. **Reenvio de eventos em lotes:** eventos repetidos podiam aparecer mais de uma vez. A Silver manteve uma ocorrência por `event_id` e removeu 656 duplicatas.
2. **Identificador de sessão não confiável:** o campo `session_hint` podia mudar durante a navegação. A sessão foi calculada pelo `user_pseudo_id`, ordenando os eventos e iniciando nova sessão após pausa superior a 30 minutos.
3. **Identidade anônima e autenticada:** eventos anteriores ao login ou cadastro precisavam ser associados à identidade conhecida do mesmo navegador. O processo de identity stitching propaga o `user_id` resolvido.
4. **Campos aninhados e valores inconsistentes:** `params` é um mapa com chaves que variam por evento. A Silver extrai as chaves para colunas tipadas e usa conversões tolerantes (`try_cast`/`try_to_timestamp`) para que valores malformados possam ser tratados pelas regras de validação, em vez de interromper todo o job.
5. **Regras de domínio:** compras e reembolsos precisam de `transaction_id`; valores monetários precisam ser válidos e estar em BRL; marcos de rolagem e vídeo devem pertencer aos valores permitidos; reembolsos precisam apontar para uma compra válida.
6. **Atribuição e vazamento no modelo:** o canal é associado pelo primeiro evento da sessão; para o modelo de propensão, as features comportamentais são limitadas ao período anterior ao primeiro `add_to_cart`, evitando usar eventos de checkout que antecipariam o rótulo.
7. **URLs e páginas:** para o mapa, a URL é normalizada para o caminho, removendo domínio, fragmento e parâmetros de consulta. Cliques sem `element_id` ou `page_location` não entram na tabela de cliques porque não permitem posicionar um elemento na página.

### 5.4 Houve rejeição de dados legítimos?

Os logs agregados comprovam quantidades e motivos de rejeição, mas não incluem uma auditoria semântica, evento por evento, capaz de provar que todos os 1.903 registros rejeitados eram ilegítimos. Assim, **não é possível afirmar somente com esses totais que nenhum dado legítimo foi rejeitado**. A verificação definitiva exige inspecionar amostras ou a totalidade de `silver/eventos_rejeitados/` e comparar cada registro com o contrato de eventos. Os motivos de rejeição devem ser interpretados como falhas nas regras implementadas, não como prova automática de fraude ou de irrelevância comercial.

## 6. Execução e validação final

O projeto possui o orquestrador `pipeline/executar_pipeline.py`, que executa oito etapas em sequência: Bronze, Silver, cinco jobs Gold e exportação R11. Ele parte da landing já existente e não gera novos eventos.

Comando utilizado, na raiz do repositório:

```powershell
docker compose run --rm app python pipeline/executar_pipeline.py
```

A execução registrada concluiu todas as oito etapas e encerrou com a mensagem:

```text
PIPELINE COMPLETO CONCLUÍDO COM SUCESSO
Bronze, Silver, cinco jobs Gold e exportação R11 executados.
```

A exportação confirmou 24 linhas de cliques e 5 marcos de rolagem em `site/mapa_calor.js`. As consultas das tabelas Gold foram verificadas no Athena, e a visualização do mapa foi testada no site.

## 7. Conclusão

O projeto transformou eventos brutos de navegação em dados organizados e tabelas Gold para análise de conversão, engajamento, receita, aquisição, propensão e comportamento na página. Os resultados apontam uma perda expressiva antes da seleção de um plano, baixa conclusão do vídeo, buscas relevantes sem resposta no FAQ e diferença de qualidade entre canais em relação a reembolsos. A receita líquida observada foi de **R$ 309.914,60**. O modelo de propensão apresentou AUC de **0,9230** e recall de **98,13%**, mas sua precisão de **30,70%** recomenda uso assistido, não decisão automática. Por fim, o mapa de calor foi exportado e apresentado no site, completando a integração entre a camada analítica e a visualização.
