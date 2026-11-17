"""Job Gold de Engajamento do projeto DadosPro.

Cria as tabelas:
- gold/sessoes: uma linha por sessão, com duração e indicadores de engajamento.
- gold/retencao_video: retenção por marco entre sessões que iniciaram o vídeo.
- gold/buscas_faq: volume de buscas e buscas sem resultado por termo.
- gold/saidas_downloads: cliques de saída por domínio e downloads por arquivo.

Uso, na raiz do repositório:
    docker compose run --rm app python pipeline/gold_engajamento.py
"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo


MARCOS_VIDEO = (10, 25, 50, 75, 100)


def gravar_tabela(df, nome: str) -> None:
    """Grava uma tabela Gold independente em Parquet."""
    destino = caminho(f"gold/{nome}")
    (df.write.mode("overwrite").parquet(destino))


def criar_sessoes(eventos):
    """Resume os eventos em uma linha por session_id."""
    janela_inicio = (
        Window.partitionBy("session_id")
        .orderBy(F.col("event_ts").asc(), F.col("event_id").asc())
    )

    contexto = (
        eventos
        .withColumn("_ordem", F.row_number().over(janela_inicio))
        .filter(F.col("_ordem") == 1)
        .select(
            "session_id",
            F.col("channel").alias("canal"),
            F.col("device_category").alias("dispositivo"),
            "user_pseudo_id",
            "user_id",
        )
    )

    resumo = (
        eventos
        .groupBy("session_id")
        .agg(
            F.min("event_ts").alias("inicio_sessao"),
            F.max("event_ts").alias("fim_sessao"),
            F.count("event_id").alias("total_eventos"),
            F.max(F.when(F.col("event_name") == "scroll", F.col("percent_scrolled")))
             .cast("int").alias("scroll_maximo"),
            F.max(F.when(F.col("event_name").isin("video_start", "video_progress", "video_complete"),
                         F.col("video_percent")))
             .cast("int").alias("video_percent_maximo"),
            F.max(F.when(F.col("event_name") == "generate_lead", 1).otherwise(0))
             .cast("boolean").alias("fez_lead"),
            F.max(F.when(F.col("event_name") == "purchase", 1).otherwise(0))
             .cast("boolean").alias("comprou"),
        )
        .withColumn(
            "duracao_segundos",
            F.col("fim_sessao").cast("long") - F.col("inicio_sessao").cast("long"),
        )
        .withColumn("duracao_minutos", F.round(F.col("duracao_segundos") / F.lit(60.0), 2))
        .join(contexto, on="session_id", how="left")
        .withColumn("canal", F.coalesce(F.col("canal"), F.lit("(desconhecido)")))
        .withColumn("dispositivo", F.coalesce(F.col("dispositivo"), F.lit("(desconhecido)")))
        .select(
            "session_id",
            "user_pseudo_id",
            "user_id",
            "inicio_sessao",
            "fim_sessao",
            "duracao_segundos",
            "duracao_minutos",
            "total_eventos",
            "scroll_maximo",
            "video_percent_maximo",
            "fez_lead",
            "comprou",
            "canal",
            "dispositivo",
        )
    )
    return resumo


def criar_retencao_video(eventos, spark):
    """Calcula a retenção sobre as sessões que tiveram video_start."""
    sessoes_inicio = (
        eventos
        .filter(F.col("event_name") == "video_start")
        .select("session_id")
        .distinct()
    )

    maior_percentual = (
        eventos
        .filter(F.col("event_name").isin("video_start", "video_progress", "video_complete"))
        .groupBy("session_id")
        .agg(F.max("video_percent").alias("maior_video_percent"))
    )

    por_sessao = sessoes_inicio.join(maior_percentual, on="session_id", how="left")
    total_sessoes_video = por_sessao.count()

    marcos = spark.createDataFrame([(m,) for m in MARCOS_VIDEO], ["marco_percentual"])

    resultado = (
        por_sessao.crossJoin(marcos)
        .groupBy("marco_percentual")
        .agg(
            F.countDistinct("session_id").alias("sessoes_com_video_start"),
            F.sum(
                F.when(F.col("maior_video_percent") >= F.col("marco_percentual"), 1)
                 .otherwise(0)
            ).alias("sessoes_que_atingiram"),
        )
        .withColumn(
            "pct_sessoes",
            F.round(
                F.try_divide(F.col("sessoes_que_atingiram"), F.lit(total_sessoes_video)),
                4,
            ),
        )
        .orderBy("marco_percentual")
    )
    return resultado


def criar_buscas_faq(eventos):
    """Agrupa buscas pelo termo e conta as buscas sem resultado."""
    buscas = eventos.filter(F.col("event_name") == "site_search")
    resultado = (
        buscas
        .withColumn(
            "termo",
            F.coalesce(F.trim(F.col("search_term")), F.lit("(sem termo)")),
        )
        .groupBy("termo")
        .agg(
            F.count("event_id").alias("total_buscas"),
            F.sum(F.when(F.coalesce(F.col("search_results"), F.lit(0)) == 0, 1)
                  .otherwise(0)).alias("buscas_sem_resultado"),
        )
        .orderBy(F.col("total_buscas").desc(), F.col("termo"))
    )
    return resultado


def criar_saidas_downloads(eventos):
    """Une cliques de saída por domínio e downloads por arquivo."""
    cliques_saida = (
        eventos
        .filter((F.col("event_name") == "click") & (F.col("outbound") == F.lit(True)))
        .withColumn("tipo", F.lit("clique_saida"))
        .withColumn("dimensao", F.coalesce(F.col("link_domain"), F.lit("(dominio desconhecido)")))
        .groupBy("tipo", "dimensao")
        .agg(
            F.count("event_id").alias("total_eventos"),
            F.countDistinct("session_id").alias("sessoes_distintas"),
        )
    )

    downloads = (
        eventos
        .filter(F.col("event_name") == "file_download")
        .withColumn("tipo", F.lit("download"))
        .withColumn("dimensao", F.coalesce(F.col("file_name"), F.lit("(arquivo desconhecido)")))
        .groupBy("tipo", "dimensao")
        .agg(
            F.count("event_id").alias("total_eventos"),
            F.countDistinct("session_id").alias("sessoes_distintas"),
        )
    )

    return cliques_saida.unionByName(downloads).orderBy("tipo", F.col("total_eventos").desc())


def main():
    spark = criar_sessao("gold_engajamento")
    try:
        titulo("JOB GOLD ENGAJAMENTO - DADOSPRO")
        exigir_camada(
            spark,
            glob_eventos("silver"),
            "docker compose run --rm app python pipeline/silver.py",
        )

        print("Lendo eventos aprovados da Silver...")
        eventos = ler_eventos(spark, "silver")

        print("Calculando resumo das sessões...")
        sessoes = criar_sessoes(eventos)
        total_sessoes = sessoes.count()

        print("Calculando retenção do vídeo...")
        retencao_video = criar_retencao_video(eventos, spark)

        print("Agrupando buscas do FAQ...")
        buscas_faq = criar_buscas_faq(eventos)

        print("Agrupando cliques de saída e downloads...")
        saidas_downloads = criar_saidas_downloads(eventos)

        print("Gravando gold/sessoes/...")
        gravar_tabela(sessoes, "sessoes")
        print("Gravando gold/retencao_video/...")
        gravar_tabela(retencao_video, "retencao_video")
        print("Gravando gold/buscas_faq/...")
        gravar_tabela(buscas_faq, "buscas_faq")
        print("Gravando gold/saidas_downloads/...")
        gravar_tabela(saidas_downloads, "saidas_downloads")

        titulo("RESULTADO DA GOLD ENGAJAMENTO")
        print(f"Sessões resumidas: {total_sessoes:,}")
        print(f"Marcos de retenção de vídeo: {retencao_video.count():,}")
        print(f"Termos de busca no FAQ: {buscas_faq.count():,}")
        print(f"Linhas em saídas e downloads: {saidas_downloads.count():,}")

        print("\nRetenção de vídeo:")
        retencao_video.show(20, truncate=False)
        print("\nBuscas FAQ:")
        buscas_faq.show(50, truncate=False)
        print("\nSaídas e downloads:")
        saidas_downloads.show(50, truncate=False)

        print("\nDestinos:")
        for tabela in ("sessoes", "retencao_video", "buscas_faq", "saidas_downloads"):
            print(f"  s3a://datalake-tracking/gold/{tabela}/")
        print("\nJob Gold Engajamento concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
