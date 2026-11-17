"""Job Gold de mapa de calor do projeto DadosPro.

Cria as tabelas:
- gold/mapa_calor_cliques: cliques por pagina e element_id.
- gold/mapa_calor_rolagem: sessoes que atingiram cada marco de rolagem da landing.

Uso, na raiz do repositorio:
    docker compose run --rm app python pipeline/gold_mapa_calor.py
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo

MARCOS_ROLAGEM = (0, 25, 50, 75, 90)


def validar_colunas(df, obrigatorias: set[str], origem: str) -> None:
    """Falha cedo se o schema de entrada nao tiver as colunas esperadas."""
    ausentes = sorted(obrigatorias - set(df.columns))
    if ausentes:
        detalhe = ", ".join(ausentes)
        if "element_id" in ausentes:
            detalhe += (
                ". Inclua element_id em PARAMETROS_TIPADOS no pipeline/silver.py "
                "e rode novamente o job Silver antes deste job"
            )
        raise ValueError(f"A origem {origem} nao contem as colunas obrigatorias: {detalhe}")


def normalizar_pagina(coluna_url):
    """Retorna somente o caminho da URL, sem dominio, fragmento ou query (R5.4.12)."""
    caminho_url = F.regexp_replace(coluna_url, r"^https?://[^/]+", "")
    caminho_url = F.regexp_replace(caminho_url, r"[?#].*$", "")
    return F.when(
        caminho_url.isNull() | (F.length(F.trim(caminho_url)) == 0), F.lit("/")
    ).otherwise(caminho_url)


def criar_mapa_cliques(eventos):
    """Conta eventos marcados por element_id e contextualiza com sessoes da pagina."""
    eventos_com_pagina = (
        eventos
        .filter(
            F.col("session_id").isNotNull()
            & F.col("page_location").isNotNull()
            & (F.length(F.trim(F.col("page_location"))) > 0)
        )
        .withColumn("pagina", normalizar_pagina(F.col("page_location")))
    )

    # O contrato usa element_id em qualquer evento originado por clique, nao apenas
    # event_name=click (ex.: select_item, add_to_cart e purchase tambem podem ter).
    cliques = (
        eventos_com_pagina
        .filter(
            F.col("element_id").isNotNull()
            & (F.length(F.trim(F.col("element_id"))) > 0)
        )
        .groupBy("pagina", "element_id")
        .agg(
            F.count(F.lit(1)).cast("long").alias("cliques"),
            F.countDistinct("session_id").cast("long").alias("sessoes"),
        )
    )

    sessoes_na_pagina = (
        eventos_com_pagina
        .filter(F.col("event_name") == "page_view")
        .groupBy("pagina")
        .agg(F.countDistinct("session_id").cast("long").alias("sessoes_na_pagina"))
    )

    janela_pagina = Window.partitionBy("pagina")
    resultado = (
        cliques
        .join(sessoes_na_pagina, on="pagina", how="left")
        .withColumn(
            "taxa_clique",
            F.round(
                F.try_divide(
                    F.col("sessoes").cast("double"),
                    F.col("sessoes_na_pagina").cast("double"),
                ),
                4,
            ),
        )
        .withColumn("_max_cliques_pagina", F.max("cliques").over(janela_pagina))
        .withColumn(
            "intensidade",
            F.round(
                F.try_divide(
                    F.col("cliques").cast("double"),
                    F.col("_max_cliques_pagina").cast("double"),
                ),
                4,
            ),
        )
        .select(
            "pagina",
            "element_id",
            "cliques",
            "sessoes",
            "sessoes_na_pagina",
            "taxa_clique",
            "intensidade",
        )
        .orderBy("pagina", F.desc("cliques"), "element_id")
    )
    return resultado


def criar_mapa_rolagem(eventos, spark):
    """Calcula a retencao por marco entre sessoes que abriram a landing."""
    eventos_com_pagina = (
        eventos
        .filter(
            F.col("session_id").isNotNull()
            & F.col("page_location").isNotNull()
            & (F.length(F.trim(F.col("page_location"))) > 0)
        )
        .withColumn("pagina", normalizar_pagina(F.col("page_location")))
    )

    sessoes_landing = (
        eventos_com_pagina
        .filter((F.col("event_name") == "page_view") & (F.col("pagina") == "/"))
        .select("session_id")
        .distinct()
    )

    scroll_maximo = (
        eventos_com_pagina
        .filter(
            (F.col("event_name") == "scroll")
            & (F.col("pagina") == "/")
            & F.col("percent_scrolled").isNotNull()
        )
        .groupBy("session_id")
        .agg(F.max("percent_scrolled").cast("int").alias("scroll_maximo"))
    )

    marcos = spark.createDataFrame([(m,) for m in MARCOS_ROLAGEM], ["marco"])
    base = sessoes_landing.join(scroll_maximo, on="session_id", how="left")

    por_marco = (
        base.crossJoin(marcos)
        .groupBy("marco")
        .agg(
            F.countDistinct(
                F.when(
                    (F.col("marco") == 0)
                    | (F.col("scroll_maximo") >= F.col("marco")),
                    F.col("session_id"),
                )
            ).cast("long").alias("sessoes_que_chegaram")
        )
    )

    janela_total = Window.partitionBy()
    resultado = (
        por_marco
        .withColumn(
            "_total_landing",
            F.max(F.when(F.col("marco") == 0, F.col("sessoes_que_chegaram"))).over(janela_total),
        )
        .withColumn("pagina", F.lit("/"))
        .withColumn(
            "pct_sessoes",
            F.round(
                F.try_divide(
                    F.col("sessoes_que_chegaram").cast("double"),
                    F.col("_total_landing").cast("double"),
                ),
                4,
            ),
        )
        .select("pagina", "marco", "sessoes_que_chegaram", "pct_sessoes")
        .orderBy("marco")
    )
    return resultado


def main():
    spark = criar_sessao("gold_mapa_calor")

    try:
        titulo("JOB GOLD MAPA DE CALOR - DADOSPRO")
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )

        print("Lendo eventos da Silver...")
        eventos = ler_eventos(spark, "silver")
        validar_colunas(
            eventos,
            {"event_id", "event_name", "event_ts", "session_id", "page_location", "element_id", "percent_scrolled"},
            "silver.eventos",
        )

        print("Calculando cliques por pagina e elemento...")
        mapa_cliques = criar_mapa_cliques(eventos)

        print("Calculando marcos de rolagem da landing...")
        mapa_rolagem = criar_mapa_rolagem(eventos, spark)

        print("Gravando gold/mapa_calor_cliques/...")
        mapa_cliques.write.mode("overwrite").parquet(caminho("gold/mapa_calor_cliques"))
        print("Gravando gold/mapa_calor_rolagem/...")
        mapa_rolagem.write.mode("overwrite").parquet(caminho("gold/mapa_calor_rolagem"))

        titulo("RESULTADO DA GOLD MAPA DE CALOR")
        print(f"Elementos/paginas com cliques: {mapa_cliques.count():,}")
        print(f"Marcos de rolagem:             {mapa_rolagem.count():,}")
        print("\nCliques por elemento:")
        mapa_cliques.show(100, truncate=False)
        print("\nRolagem na landing:")
        mapa_rolagem.show(20, truncate=False)
        print("\nDestinos:")
        print("  s3a://datalake-tracking/gold/mapa_calor_cliques/")
        print("  s3a://datalake-tracking/gold/mapa_calor_rolagem/")
        print("\nJob Gold Mapa de Calor concluido com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
