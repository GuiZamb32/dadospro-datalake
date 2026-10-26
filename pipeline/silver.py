"""Job Silver do projeto DadosPro.

Responsabilidades (R5 a R10):
- Deduplicar eventos por event_id.
- Tipar e extrair os campos de params.
- Separar eventos inválidos, com um motivo por linha.
- Calcular session_id sem usar session_hint.
- Propagar user_id aos eventos do mesmo navegador (identity stitching).
- Explodir items[] em silver/itens/.

Uso, na raiz do repositório:
    docker compose run --rm app python pipeline/silver.py
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from comum import (
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
    caminho,
)


# O contrato lista 20 famílias conceituais, representadas por 22 nomes de evento
# porque a família video_* contém video_start, video_progress e video_complete.
EVENTOS_VALIDOS = (
    "page_view",
    "scroll",
    "click",
    "site_search",
    "video_start",
    "video_progress",
    "video_complete",
    "file_download",
    "view_item_list",
    "select_item",
    "view_item",
    "add_to_cart",
    "remove_from_cart",
    "view_cart",
    "begin_checkout",
    "add_shipping_info",
    "add_payment_info",
    "purchase",
    "refund",
    "generate_lead",
    "sign_up",
    "login",
)

EVENTOS_VIDEO = ("video_start", "video_progress", "video_complete")

# nome_da_coluna: (chave em params, tipo Spark para try_cast)
PARAMETROS_TIPADOS = {
    "page_title": ("page_title", "string"),
    "percent_scrolled": ("percent_scrolled", "int"),
    "link_url": ("link_url", "string"),
    "link_domain": ("link_domain", "string"),
    "link_text": ("link_text", "string"),
    "link_id": ("link_id", "string"),
    "outbound": ("outbound", "boolean"),
    "search_term": ("search_term", "string"),
    "search_results": ("search_results", "int"),
    "search_area": ("search_area", "string"),
    "video_title": ("video_title", "string"),
    "video_provider": ("video_provider", "string"),
    "video_duration": ("video_duration", "double"),
    "video_current_time": ("video_current_time", "double"),
    "video_percent": ("video_percent", "int"),
    "file_name": ("file_name", "string"),
    "file_extension": ("file_extension", "string"),
    "item_list_id": ("item_list_id", "string"),
    "item_list_name": ("item_list_name", "string"),
    "currency": ("currency", "string"),
    "value": ("value", "double"),
    "shipping_tier": ("shipping_tier", "string"),
    "shipping": ("shipping", "double"),
    "transaction_id": ("transaction_id", "string"),
    "tax": ("tax", "double"),
    "payment_type": ("payment_type", "string"),
    "coupon": ("coupon", "string"),
    "reason": ("reason", "string"),
    "lead_source": ("lead_source", "string"),
    "method": ("method", "string"),
}

MOTIVO_EVENTO_FORA_CATALOGO = "evento fora do catálogo"
MOTIVO_TIMESTAMP = "timestamp ausente ou futuro"
MOTIVO_TRANSACAO = "transação sem id"
MOTIVO_VALOR = "valor monetário inválido"
MOTIVO_MARCO = "marco fora do padrão"
MOTIVO_REEMBOLSO = "reembolso sem compra"


def extrair_e_tipar_eventos(bronze):
    """Achata os structs e extrai params para colunas tipadas, preservando o timestamp bruto."""
    df = (
        bronze
        .withColumnRenamed("event_ts", "event_ts_raw")
        .withColumn("event_ts", F.try_to_timestamp(F.col("event_ts_raw")))
        .withColumn("page_location", F.col("page.location"))
        .withColumn("page_referrer", F.col("page.referrer"))
        .withColumn("traffic_source", F.col("traffic.source"))
        .withColumn("traffic_medium", F.col("traffic.medium"))
        .withColumn("traffic_campaign", F.col("traffic.campaign"))
        .withColumn("device_category", F.col("device.category"))
        .withColumn("device_os", F.col("device.os"))
        .withColumn("device_browser", F.col("device.browser"))
    )

    # A Bronze armazena params como map<string,string>; try_cast evita que um valor
    # inesperado derrube o job no modo ANSI do Spark 4.
    for nome_coluna, (chave, tipo) in PARAMETROS_TIPADOS.items():
        df = df.withColumn(
            nome_coluna,
            F.element_at(F.col("params"), F.lit(chave)).try_cast(tipo),
        )

    # Usado na validação para distinguir value ausente de value não conversível.
    df = df.withColumn(
        "_value_raw",
        F.element_at(F.col("params"), F.lit("value")),
    )
    df = df.withColumn(
        "channel",
        F.concat_ws(" / ", F.col("traffic_source"), F.col("traffic_medium")),
    )
    return df


def adicionar_motivos_base(eventos):
    """Avalia as regras R7, exceto reembolso órfão, que depende de compras válidas."""
    nome_evento = F.col("event_name")
    transaction_id_vazio = (
        F.col("transaction_id").isNull()
        | (F.length(F.trim(F.col("transaction_id"))) == 0)
    )

    valor_bruto_presente = F.col("_value_raw").isNotNull()
    valor_invalido = valor_bruto_presente & (
        F.col("value").isNull()
        | (F.col("value") < 0)
        | F.col("currency").isNull()
        | (F.col("currency") != F.lit("BRL"))
    )

    marco_scroll_invalido = (nome_evento == "scroll") & (
        F.col("percent_scrolled").isNull()
        | (~F.col("percent_scrolled").isin(25, 50, 75, 90))
    )
    marco_video_invalido = nome_evento.isin(*EVENTOS_VIDEO) & (
        F.col("video_percent").isNull()
        | (~F.col("video_percent").isin(0, 10, 25, 50, 75, 100))
    )

    regras = F.array(
        F.when(
            nome_evento.isNull() | (~nome_evento.isin(*EVENTOS_VALIDOS)),
            F.lit(MOTIVO_EVENTO_FORA_CATALOGO),
        ),
        F.when(
            F.col("event_ts").isNull() | (F.col("event_ts") > F.col("ingested_at")),
            F.lit(MOTIVO_TIMESTAMP),
        ),
        F.when(
            nome_evento.isin("purchase", "refund") & transaction_id_vazio,
            F.lit(MOTIVO_TRANSACAO),
        ),
        F.when(valor_invalido, F.lit(MOTIVO_VALOR)),
        F.when(
            marco_scroll_invalido | marco_video_invalido,
            F.lit(MOTIVO_MARCO),
        ),
    )

    # Cada motivo gera uma linha separada na tabela de rejeitados.
    return eventos.withColumn(
        "_motivos_base",
        F.filter(regras, lambda motivo: motivo.isNotNull()),
    )


def criar_dataframe_itens(eventos_validos):
    """Gera uma linha por item do array items[], mantendo o evento de origem."""
    itens = (
        eventos_validos
        .withColumn("_item", F.explode("items"))
        .filter(F.col("_item").isNotNull())
        .select(
            "event_id",
            "event_name",
            "event_ts",
            "user_pseudo_id",
            "user_id",
            "session_id",
            "session_start_ts",
            "transaction_id",
            "currency",
            "value",
            "channel",
            "traffic_source",
            "traffic_medium",
            "device_category",
            "page_location",
            "year",
            "month",
            "day",
            F.col("_item.item_id").alias("item_id"),
            F.col("_item.item_name").alias("item_name"),
            F.col("_item.item_category").alias("item_category"),
            F.col("_item.price").try_cast("double").alias("price"),
            F.col("_item.quantity").try_cast("int").alias("quantity"),
        )
    )
    return itens


def adicionar_sessoes_e_identidade(eventos_validos):
    """Aplica identity stitching e calcula sessões com pausa superior a 30 minutos."""
    df = eventos_validos.withColumn(
        "_identity_key",
        F.coalesce(
            F.col("user_pseudo_id"),
            F.concat(F.lit("__sem_pseudo__"), F.coalesce(F.col("event_id"), F.lit("sem_event_id"))),
        ),
    )

    # Propaga a primeira identidade não nula conhecida para todos os eventos daquele
    # navegador, inclusive os que ocorreram antes do sign_up/login.
    janela_identidade = (
        Window.partitionBy("_identity_key")
        .orderBy(F.col("event_ts").asc(), F.col("event_id").asc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df = (
        df.withColumn(
            "_user_id_resolvido",
            F.first("user_id", ignorenulls=True).over(janela_identidade),
        )
        .withColumn("user_id", F.coalesce(F.col("_user_id_resolvido"), F.col("user_id")))
        .drop("_user_id_resolvido")
        .withColumn("_session_key", F.col("_identity_key"))
    )

    janela_ordenada = Window.partitionBy("_session_key").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc()
    )
    janela_acumulada = janela_ordenada.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    df = (
        df.withColumn("_evento_anterior_ts", F.lag("event_ts").over(janela_ordenada))
        .withColumn(
            "_nova_sessao",
            F.when(
                F.col("_evento_anterior_ts").isNull()
                | (
                    F.col("event_ts").cast("long")
                    - F.col("_evento_anterior_ts").cast("long")
                    > F.lit(30 * 60)
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("session_number", F.sum("_nova_sessao").over(janela_acumulada))
    )

    janela_sessao = Window.partitionBy("_session_key", "session_number")
    df = df.withColumn("session_start_ts", F.min("event_ts").over(janela_sessao))
    df = df.withColumn(
        "session_id",
        F.concat(
            F.col("_session_key"),
            F.lit("_"),
            F.date_format(F.col("session_start_ts"), "yyyyMMddHHmmssSSS"),
        ),
    )

    return df.drop(
        "_identity_key",
        "_session_key",
        "_evento_anterior_ts",
        "_nova_sessao",
    )


def main():
    spark = criar_sessao("silver")

    try:
        titulo("JOB SILVER - DADOSPRO")
        exigir_camada(
            spark,
            glob_eventos("bronze"),
            "docker compose run --rm app python pipeline/bronze.py",
        )

        print("Lendo a Bronze...")
        bronze = ler_eventos(spark, "bronze")
        total_bronze = bronze.count()

        print("Removendo duplicatas por event_id...")
        deduplicados = bronze.dropDuplicates(["event_id"])
        deduplicados.cache()
        total_deduplicado = deduplicados.count()

        print("Extraindo e tipando params...")
        enriquecidos = extrair_e_tipar_eventos(deduplicados)
        avaliados = adicionar_motivos_base(enriquecidos)

        # Só compras que passaram pelas regras anteriores podem validar um refund.
        compras_validas = (
            avaliados
            .filter(
                (F.col("event_name") == "purchase")
                & (F.size(F.col("_motivos_base")) == 0)
                & F.col("transaction_id").isNotNull()
                & (F.length(F.trim(F.col("transaction_id"))) > 0)
            )
            .select("transaction_id")
            .distinct()
        )

        # Um refund sem transaction_id já é explicado por "transação sem id"; para
        # evitar dupla classificação artificial, o motivo de órfão exige um id.
        refunds_com_id = enriquecidos.filter(
            (F.col("event_name") == "refund")
            & F.col("transaction_id").isNotNull()
            & (F.length(F.trim(F.col("transaction_id"))) > 0)
        )
        refunds_orfaos = (
            refunds_com_id.join(compras_validas, on="transaction_id", how="left_anti")
            .withColumn("motivo_rejeicao", F.lit(MOTIVO_REEMBOLSO))
        )

        rejeitados_base = (
            avaliados
            .filter(F.size(F.col("_motivos_base")) > 0)
            .withColumn("motivo_rejeicao", F.explode(F.col("_motivos_base")))
            .drop("_motivos_base")
        )
        rejeitados = rejeitados_base.unionByName(
            refunds_orfaos, allowMissingColumns=True
        ).drop("_value_raw")

        ids_refunds_orfaos = refunds_orfaos.select("event_id").distinct()
        eventos_validos = (
            avaliados
            .filter(F.size(F.col("_motivos_base")) == 0)
            .drop("_motivos_base", "_value_raw")
            .join(ids_refunds_orfaos, on="event_id", how="left_anti")
        )

        print("Calculando identidade e sessões...")
        eventos_validos = adicionar_sessoes_e_identidade(eventos_validos)

        print("Explodindo items[]...")
        itens = criar_dataframe_itens(eventos_validos)

        # Os dados normalizados ficam nas colunas achatadas; os structs/maps originais
        # não são necessários na tabela principal da Silver. O array items é separado.
        colunas_de_origem_para_remover = [
            "event_ts_raw",
            "session_hint",
            "page",
            "traffic",
            "device",
            "params",
            "items",
        ]
        eventos_silver = eventos_validos.drop(*colunas_de_origem_para_remover)

        total_validos = eventos_silver.count()
        total_rejeicoes = rejeitados.count()
        total_eventos_rejeitados = rejeitados.select("event_id").distinct().count()
        total_itens = itens.count()

        print("Gravando eventos aprovados em silver/...")
        gravar_eventos(eventos_silver, "silver", "overwrite")

        print("Gravando itens em silver/itens/...")
        (
            itens.write
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(caminho("silver/itens"))
        )

        print("Gravando rejeitados em silver/eventos_rejeitados/...")
        rejeitados.write.mode("overwrite").parquet(caminho("silver/eventos_rejeitados"))

        titulo("RESULTADO DA SILVER")
        print(f"Registros na Bronze:                 {total_bronze:,}")
        print(f"Registros após deduplicação:          {total_deduplicado:,}")
        print(f"Duplicatas removidas:                 {total_bronze - total_deduplicado:,}")
        print(f"Eventos aprovados na Silver:          {total_validos:,}")
        print(f"Eventos distintos rejeitados:         {total_eventos_rejeitados:,}")
        print(f"Linhas em eventos_rejeitados (motivo): {total_rejeicoes:,}")
        print(f"Linhas na tabela de itens:            {total_itens:,}")
        print("\nRejeições por motivo:")
        rejeitados.groupBy("motivo_rejeicao").count().orderBy("motivo_rejeicao").show(
            100, truncate=False
        )

        print("Destinos:")
        print("  s3a://datalake-tracking/silver/event_name=.../")
        print("  s3a://datalake-tracking/silver/itens/")
        print("  s3a://datalake-tracking/silver/eventos_rejeitados/")
        print("Partições dos eventos: event_name/year/month/day")
        print("Sessões: user_pseudo_id + pausa superior a 30 minutos (session_hint ignorado)")
        print("\nJob Silver concluído com sucesso.")

        deduplicados.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
