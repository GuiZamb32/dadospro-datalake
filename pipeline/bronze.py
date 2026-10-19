"""
Job Bronze do projeto DadosPro.

Responsabilidades:
- Ler os eventos da landing com schema explícito.
- Preservar os campos originais, inclusive dados potencialmente inválidos.
- Registrar arquivo de origem e instante da ingestão.
- Manter as partições event_name/year/month/day.
- Gravar em Parquet no S3 simulado, usando append-only.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from comum import (
    camada_existe,
    criar_sessao,
    exigir_camada,
    glob_eventos,
    gravar_eventos,
    ler_eventos,
    titulo,
)


# O event_name e as datas das partições são obtidos do caminho da landing.
# event_ts permanece como texto para preservar valores inválidos
# que deverão ser tratados posteriormente na Silver.
SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("user_pseudo_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_hint", StringType(), True),

    StructField(
        "page",
        StructType([
            StructField("location", StringType(), True),
            StructField("referrer", StringType(), True),
        ]),
        True,
    ),

    StructField(
        "traffic",
        StructType([
            StructField("source", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("campaign", StringType(), True),
        ]),
        True,
    ),

    StructField(
        "device",
        StructType([
            StructField("category", StringType(), True),
            StructField("os", StringType(), True),
            StructField("browser", StringType(), True),
        ]),
        True,
    ),

    # Os valores de params serão tipados posteriormente na Silver.
    StructField(
        "params",
        MapType(StringType(), StringType(), True),
        True,
    ),

    StructField(
        "items",
        ArrayType(
            StructType([
                StructField("item_id", StringType(), True),
                StructField("item_name", StringType(), True),
                StructField("item_category", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("quantity", IntegerType(), True),
            ]),
            True,
        ),
        True,
    ),

    # Campo opcional utilizado pelo mapa de calor.
    StructField("element_id", StringType(), True),
])


def main():
    spark = criar_sessao("bronze")

    try:
        titulo("JOB BRONZE - DADOSPRO")

        # Não executa a Bronze se a landing não existir.
        exigir_camada(
            spark,
            glob_eventos("landing"),
            "docker compose run --rm app python gerador/gerar_eventos.py",
        )

        print("Lendo eventos da landing com schema explícito...")

        landing = ler_eventos(
            spark,
            "landing",
            "json",
            schema=SCHEMA,
        )

        # Metadados de proveniência: origem e instante da ingestão.
        bronze = (
            landing
            .withColumn(
                "source_file",
                F.input_file_name(),
            )
            .withColumn(
                "ingested_at",
                F.current_timestamp(),
            )
        )

        # Evita gravar uma camada vazia caso não existam eventos.
        total_landing = bronze.count()

        if total_landing == 0:
            raise RuntimeError(
                "A landing existe, mas não contém eventos para processar."
            )

        # Não reprocessa arquivos de origem já registrados na Bronze.
        if camada_existe(spark, "bronze"):
            fontes_processadas = (
                ler_eventos(spark, "bronze")
                .select("source_file")
                .distinct()
            )

            bronze_novos = bronze.join(
                fontes_processadas,
                on="source_file",
                how="left_anti",
            )
        else:
            bronze_novos = bronze

        total_novos = bronze_novos.count()

        if total_novos > 0:
            gravar_eventos(
                bronze_novos,
                "bronze",
                "append",
            )
            print(f"Novos registros acrescentados: {total_novos:,}")
        else:
            print("Nenhum arquivo novo para ingestão.")

        bronze_atual = ler_eventos(spark, "bronze")
        total_bronze = bronze_atual.count()

        titulo("RESULTADO DA BRONZE")

        print(f"Registros lidos da landing: {total_landing:,}")
        print(f"Novos registros acrescentados: {total_novos:,}")
        print(f"Total de registros na Bronze: {total_bronze:,}")
        print("Destino: s3a://datalake-tracking/bronze/")
        print("Formato: Parquet")
        print("Partições: event_name/year/month/day")
        print("Proveniência: source_file, ingested_at")

        print("\nRegistros por nome de evento:")

        (
            bronze_atual
            .groupBy("event_name")
            .count()
            .orderBy("event_name")
            .show(100, truncate=False)
        )

        print("\nJob Bronze concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()