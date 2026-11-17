"""Job Gold — Funis de compra e de leads do projeto DadosPro.

Requisitos atendidos:
- Gera gold/funil_compra com etapas independentes, conversões e cortes total
  e por plano x canal.
- Gera gold/funil_lead por canal, considerando page_view e scroll na landing.
- Usa o canal do primeiro evento da sessão e o primeiro plano selecionado.
- Usa somente PySpark e caminhos s3a:// para acessar o datalake.

Execução, na raiz do repositório:
    docker compose run --rm app python pipeline/gold_funil.py
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo


ETAPAS_COMPRA = (
    (1, "view_item_list"),
    (2, "select_item"),
    (3, "add_to_cart"),
    (4, "view_cart"),
    (5, "begin_checkout"),
    (6, "add_payment_info"),
    (7, "purchase"),
)

ETAPAS_LEAD = (
    (1, "page_view_landing"),
    (2, "scroll_50_landing"),
    (3, "generate_lead"),
)


def validar_colunas(df, obrigatorias: set[str], origem: str) -> None:
    """Falha cedo e com mensagem clara se o schema de entrada mudou."""
    ausentes = sorted(obrigatorias - set(df.columns))
    if ausentes:
        raise ValueError(
            f"A origem {origem} não contém as colunas obrigatórias: {', '.join(ausentes)}"
        )


def canal_da_sessao(eventos):
    """Obtém o canal do primeiro evento de cada sessão (definição 5.4.3)."""
    janela = Window.partitionBy("session_id").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc()
    )

    return (
        eventos
        .filter(F.col("session_id").isNotNull())
        .withColumn("_ordem_evento", F.row_number().over(janela))
        .filter(F.col("_ordem_evento") == 1)
        .select(
            "session_id",
            F.when(
                F.col("channel").isNull() | (F.length(F.trim(F.col("channel"))) == 0),
                F.lit("(desconhecido)"),
            ).otherwise(F.col("channel")).alias("canal"),
        )
    )


def plano_da_sessao(itens):
    """Obtém o primeiro item de categoria plano em um select_item da sessão."""
    janela = Window.partitionBy("session_id").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc(), F.col("item_id").asc()
    )

    return (
        itens
        .filter(
            (F.col("event_name") == "select_item")
            & (F.col("item_category") == "plano")
            & F.col("item_id").isNotNull()
            & F.col("session_id").isNotNull()
        )
        .withColumn("_ordem_plano", F.row_number().over(janela))
        .filter(F.col("_ordem_plano") == 1)
        .select("session_id", F.col("item_id").alias("plano"))
    )


def flags_funil_compra(eventos):
    """Uma linha por sessão; cada etapa conta se o evento ocorreu em qualquer momento."""
    agregacoes = [
        F.max(F.when(F.col("event_name") == nome, 1).otherwise(0))
        .cast("long")
        .alias(f"flag_{nome}")
        for _, nome in ETAPAS_COMPRA
    ]
    agregacoes.append(
        F.max(F.when(F.col("event_name") == "view_item", 1).otherwise(0))
        .cast("long")
        .alias("flag_view_item")
    )

    return (
        eventos
        .filter(F.col("session_id").isNotNull())
        .groupBy("session_id")
        .agg(*agregacoes)
    )


def agregar_dimensoes(sessoes, dimensoes: list[str], flags: list[str]):
    """Produz cortes dimensionais e uma linha de escopo total por grupo."""
    expressoes = [
        F.sum(F.col(flag)).cast("long").alias(flag.replace("flag_", "sessoes_"))
        for flag in flags
    ]

    por_dimensao = (
        sessoes
        .withColumn("escopo", F.lit("plano_canal" if "plano" in dimensoes else "canal"))
        .groupBy("escopo", *dimensoes)
        .agg(*expressoes)
    )

    total = sessoes.withColumn("escopo", F.lit("total"))
    for dimensao in dimensoes:
        total = total.withColumn(
            dimensao,
            F.lit("(todos)") if dimensao == "canal" else F.lit("(todos)"),
        )
    total = total.groupBy("escopo", *dimensoes).agg(*expressoes)

    return por_dimensao.unionByName(total)


def transformar_funil_compra(sessoes):
    """Transforma contagens de sessão em uma linha por etapa e escopo."""
    flags = [f"flag_{nome}" for _, nome in ETAPAS_COMPRA] + ["flag_view_item"]
    agregados = agregar_dimensoes(sessoes, ["plano", "canal"], flags)

    linhas = []
    topo_coluna = "sessoes_view_item_list"
    select_coluna = "sessoes_select_item"
    detalhe_coluna = "sessoes_view_item"

    for indice, (ordem, etapa) in enumerate(ETAPAS_COMPRA):
        atual = f"sessoes_{etapa}"
        anterior = f"sessoes_{ETAPAS_COMPRA[indice - 1][1]}" if indice > 0 else None

        colunas = [
            "escopo",
            "plano",
            "canal",
            F.lit(ordem).cast("int").alias("ordem_etapa"),
            F.lit(etapa).alias("etapa"),
            F.col(atual).alias("sessoes_etapa"),
            F.col(topo_coluna).alias("sessoes_topo"),
            F.round(
                F.try_divide(F.col(atual).cast("double"), F.col(topo_coluna).cast("double")),
                4,
            ).alias("conversao_sobre_topo"),
            F.col(detalhe_coluna).alias("sessoes_view_item"),
            F.col(select_coluna).alias("sessoes_select_item"),
            F.round(
                F.try_divide(
                    F.col(detalhe_coluna).cast("double"),
                    F.col(select_coluna).cast("double"),
                ),
                4,
            ).alias("taxa_view_item_sobre_select_item"),
        ]

        if anterior is None:
            colunas.extend([
                F.lit(None).cast("long").alias("sessoes_etapa_anterior"),
                F.lit(None).cast("double").alias("conversao_etapa_anterior"),
            ])
        else:
            colunas.extend([
                F.col(anterior).alias("sessoes_etapa_anterior"),
                F.round(
                    F.try_divide(F.col(atual).cast("double"), F.col(anterior).cast("double")),
                    4,
                ).alias("conversao_etapa_anterior"),
            ])

        linhas.append(agregados.select(*colunas))

    resultado = linhas[0]
    for linha in linhas[1:]:
        resultado = resultado.unionByName(linha)

    return resultado.orderBy("escopo", "plano", "canal", "ordem_etapa")


def normalizar_pagina(coluna_url):
    """Extrai o caminho sem domínio, fragmento ou query; vazio vira '/'."""
    caminho_url = F.regexp_replace(coluna_url, r"^https?://[^/]+", "")
    caminho_url = F.regexp_replace(caminho_url, r"[?#].*$", "")
    return F.when(
        caminho_url.isNull() | (F.length(caminho_url) == 0), F.lit("/")
    ).otherwise(caminho_url)


def flags_funil_lead(eventos):
    """Cria indicadores por sessão para as três etapas do funil de lead."""
    com_pagina = eventos.withColumn("_pagina", normalizar_pagina(F.col("page_location")))

    condicoes = {
        "page_view_landing": (F.col("event_name") == "page_view") & (F.col("_pagina") == "/"),
        "scroll_50_landing": (
            (F.col("event_name") == "scroll")
            & (F.col("_pagina") == "/")
            & (F.col("percent_scrolled") >= 50)
        ),
        "generate_lead": F.col("event_name") == "generate_lead",
    }

    agregacoes = [
        F.max(F.when(condicao, 1).otherwise(0)).cast("long").alias(f"flag_{nome}")
        for nome, condicao in condicoes.items()
    ]

    return (
        com_pagina
        .filter(F.col("session_id").isNotNull())
        .groupBy("session_id")
        .agg(*agregacoes)
    )


def transformar_funil_lead(sessoes):
    """Gera contagens e conversões do funil de lead, no total e por canal."""
    flags = [f"flag_{nome}" for _, nome in ETAPAS_LEAD]
    agregados = agregar_dimensoes(sessoes, ["canal"], flags)

    linhas = []
    topo_coluna = "sessoes_page_view_landing"

    for indice, (ordem, etapa) in enumerate(ETAPAS_LEAD):
        atual = f"sessoes_{etapa}"
        anterior = f"sessoes_{ETAPAS_LEAD[indice - 1][1]}" if indice > 0 else None
        colunas = [
            "escopo",
            "canal",
            F.lit(ordem).cast("int").alias("ordem_etapa"),
            F.lit(etapa).alias("etapa"),
            F.col(atual).alias("sessoes_etapa"),
            F.col(topo_coluna).alias("sessoes_topo"),
            F.round(
                F.try_divide(F.col(atual).cast("double"), F.col(topo_coluna).cast("double")),
                4,
            ).alias("conversao_sobre_topo"),
        ]

        if anterior is None:
            colunas.extend([
                F.lit(None).cast("long").alias("sessoes_etapa_anterior"),
                F.lit(None).cast("double").alias("conversao_etapa_anterior"),
            ])
        else:
            colunas.extend([
                F.col(anterior).alias("sessoes_etapa_anterior"),
                F.round(
                    F.try_divide(F.col(atual).cast("double"), F.col(anterior).cast("double")),
                    4,
                ).alias("conversao_etapa_anterior"),
            ])

        linhas.append(agregados.select(*colunas))

    resultado = linhas[0]
    for linha in linhas[1:]:
        resultado = resultado.unionByName(linha)

    return resultado.orderBy("escopo", "canal", "ordem_etapa")


def main():
    spark = criar_sessao("gold_funil")

    try:
        titulo("JOB GOLD FUNIL - DADOSPRO")
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )
        exigir_camada(
            spark,
            "silver/itens",
            "docker compose run --rm app python pipeline/silver.py",
        )

        print("Lendo eventos e itens da Silver...")
        eventos = ler_eventos(spark, "silver")
        itens = spark.read.parquet(caminho("silver/itens"))

        validar_colunas(
            eventos,
            {"event_id", "event_name", "event_ts", "session_id", "channel", "page_location", "percent_scrolled"},
            "silver.eventos",
        )
        validar_colunas(
            itens,
            {"event_id", "event_name", "event_ts", "session_id", "item_id", "item_category"},
            "silver.itens",
        )

        print("Montando atributos das sessões (canal e primeiro plano selecionado)...")
        canais = canal_da_sessao(eventos)
        planos = plano_da_sessao(itens)

        print("Calculando etapas do funil de compra...")
        compra_flags = flags_funil_compra(eventos)
        compra_sessoes = (
            compra_flags
            .join(canais, on="session_id", how="left")
            .join(planos, on="session_id", how="left")
            .fillna({"canal": "(desconhecido)", "plano": "(nenhum)"})
        )
        funil_compra = transformar_funil_compra(compra_sessoes)

        print("Calculando etapas do funil de lead...")
        lead_flags = flags_funil_lead(eventos)
        lead_sessoes = (
            lead_flags
            .join(canais, on="session_id", how="left")
            .fillna({"canal": "(desconhecido)"})
        )
        funil_lead = transformar_funil_lead(lead_sessoes)

        total_sessoes = compra_sessoes.select("session_id").distinct().count()
        total_compra = funil_compra.filter(
            (F.col("escopo") == "total") & (F.col("etapa") == "purchase")
        ).select("sessoes_etapa").first()[0]
        total_leads = funil_lead.filter(
            (F.col("escopo") == "total") & (F.col("etapa") == "generate_lead")
        ).select("sessoes_etapa").first()[0]

        print("Gravando gold/funil_compra/...")
        funil_compra.write.mode("overwrite").parquet(caminho("gold/funil_compra"))

        print("Gravando gold/funil_lead/...")
        funil_lead.write.mode("overwrite").parquet(caminho("gold/funil_lead"))

        titulo("RESULTADO DA GOLD FUNIL")
        print(f"Sessões analisadas:                 {total_sessoes:,}")
        print(f"Sessões com purchase:               {total_compra:,}")
        print(f"Sessões com generate_lead:           {total_leads:,}")
        print(f"Linhas em funil_compra:              {funil_compra.count():,}")
        print(f"Linhas em funil_lead:                {funil_lead.count():,}")
        print("\nFunil de compra — escopo total:")
        funil_compra.filter(F.col("escopo") == "total").orderBy("ordem_etapa").show(20, truncate=False)
        print("\nFunil de lead — escopo total:")
        funil_lead.filter(F.col("escopo") == "total").orderBy("ordem_etapa").show(20, truncate=False)
        print("\nJob Gold Funil concluído com sucesso.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
