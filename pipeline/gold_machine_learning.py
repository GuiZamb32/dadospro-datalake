"""Job Gold de Machine Learning do projeto DadosPro.

Gera:
- gold/propensao_sessao: probabilidade de compra para cada sessão;
- gold/metricas_modelo: métricas no conjunto de teste (AUC, F1, precisão e recall);
- gold/segmentos_visitantes: cluster de cada visitante e perfil médio do grupo.

Prevenção de vazamento:
As features comportamentais consideram somente eventos anteriores ao primeiro
add_to_cart da sessão. Canal e dispositivo são obtidos do primeiro evento da
sessão. O rótulo de compra é calculado com a sessão inteira, exclusivamente como
target do modelo.

Execução, na raiz do repositório:
    docker compose run --rm app python pipeline/gold_machine_learning.py
"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo


SEED = 42
TEST_SIZE = 0.20
LIMIAR_CLASSIFICACAO = 0.50
NUM_CLUSTERS = 4

FEATURES_NUMERICAS = [
    "qtd_scroll_pre_add_to_cart",
    "scroll_maximo_pre_add_to_cart",
    "sessao_iniciou_video_pre_add_to_cart",
    "video_percent_maximo_pre_add_to_cart",
    "sessao_visualizou_planos_pre_add_to_cart",
    "sessao_selecionou_plano_pre_add_to_cart",
    "sessao_abriu_detalhe_pre_add_to_cart",
    "qtd_buscas_pre_add_to_cart",
    "qtd_downloads_pre_add_to_cart",
    "qtd_cliques_pre_add_to_cart",
]
FEATURES_CATEGORICAS = ["canal", "dispositivo", "plano_selecionado"]


def validar_colunas(df, obrigatorias: set[str], origem: str) -> None:
    ausentes = sorted(obrigatorias - set(df.columns))
    if ausentes:
        raise ValueError(
            f"A origem {origem} não contém as colunas obrigatórias: {', '.join(ausentes)}"
        )


def primeiro_contexto_sessao(eventos):
    """Canal, dispositivo e identidade do primeiro evento da sessão."""
    janela = Window.partitionBy("session_id").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc()
    )
    return (
        eventos.filter(F.col("session_id").isNotNull())
        .withColumn("_ordem", F.row_number().over(janela))
        .filter(F.col("_ordem") == 1)
        .select(
            "session_id",
            "user_pseudo_id",
            "user_id",
            F.coalesce(F.col("channel"), F.lit("(desconhecido)")).alias("canal"),
            F.coalesce(F.col("device_category"), F.lit("(desconhecido)")).alias("dispositivo"),
        )
    )


def construir_features_sessao(eventos, itens):
    """Monta uma linha por sessão, separando features pré-carrinho do rótulo final."""
    primeiras_compras = (
        eventos.filter(F.col("event_name") == "add_to_cart")
        .groupBy("session_id")
        .agg(F.min("event_ts").alias("primeiro_add_to_cart_ts"))
    )

    eventos_com_limite = eventos.join(primeiras_compras, on="session_id", how="left")
    eventos_pre_cart = eventos_com_limite.filter(
        F.col("primeiro_add_to_cart_ts").isNull()
        | (F.col("event_ts") < F.col("primeiro_add_to_cart_ts"))
    )

    pre_cart = (
        eventos_pre_cart.groupBy("session_id")
        .agg(
            F.sum(F.when(F.col("event_name") == "scroll", 1).otherwise(0))
             .cast("double").alias("qtd_scroll_pre_add_to_cart"),
            F.max(F.when(F.col("event_name") == "scroll", F.col("percent_scrolled")))
             .cast("double").alias("scroll_maximo_pre_add_to_cart"),
            F.max(F.when(F.col("event_name") == "video_start", 1).otherwise(0))
             .cast("double").alias("sessao_iniciou_video_pre_add_to_cart"),
            F.max(F.when(
                F.col("event_name").isin("video_start", "video_progress", "video_complete"),
                F.col("video_percent"),
            )).cast("double").alias("video_percent_maximo_pre_add_to_cart"),
            F.max(F.when(F.col("event_name") == "view_item_list", 1).otherwise(0))
             .cast("double").alias("sessao_visualizou_planos_pre_add_to_cart"),
            F.max(F.when(F.col("event_name") == "select_item", 1).otherwise(0))
             .cast("double").alias("sessao_selecionou_plano_pre_add_to_cart"),
            F.max(F.when(F.col("event_name") == "view_item", 1).otherwise(0))
             .cast("double").alias("sessao_abriu_detalhe_pre_add_to_cart"),
            F.sum(F.when(F.col("event_name") == "site_search", 1).otherwise(0))
             .cast("double").alias("qtd_buscas_pre_add_to_cart"),
            F.sum(F.when(F.col("event_name") == "file_download", 1).otherwise(0))
             .cast("double").alias("qtd_downloads_pre_add_to_cart"),
            F.sum(F.when(F.col("event_name") == "click", 1).otherwise(0))
             .cast("double").alias("qtd_cliques_pre_add_to_cart"),
        )
    )

    # O plano selecionado é obtido somente de select_item anterior ao primeiro carrinho.
    itens_com_limite = itens.join(primeiras_compras, on="session_id", how="left")
    itens_pre_cart = itens_com_limite.filter(
        (F.col("event_name") == "select_item")
        & (F.col("item_category") == "plano")
        & (F.col("item_id").isNotNull())
        & (
            F.col("primeiro_add_to_cart_ts").isNull()
            | (F.col("event_ts") < F.col("primeiro_add_to_cart_ts"))
        )
    )
    janela_plano = Window.partitionBy("session_id").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc(), F.col("item_id").asc()
    )
    plano = (
        itens_pre_cart.withColumn("_ordem_plano", F.row_number().over(janela_plano))
        .filter(F.col("_ordem_plano") == 1)
        .select("session_id", F.col("item_id").alias("plano_selecionado"))
    )

    contexto = primeiro_contexto_sessao(eventos)
    resumo_completo = (
        eventos.filter(F.col("session_id").isNotNull())
        .groupBy("session_id")
        .agg(
            F.count("event_id").cast("long").alias("total_eventos"),
            F.min("event_ts").alias("inicio_sessao"),
            F.max("event_ts").alias("fim_sessao"),
            F.max(F.when(F.col("event_name") == "scroll", F.col("percent_scrolled")))
             .cast("double").alias("scroll_maximo_sessao"),
            F.max(F.when(
                F.col("event_name").isin("video_start", "video_progress", "video_complete"),
                F.col("video_percent"),
            )).cast("double").alias("video_percent_maximo_sessao"),
            F.max(F.when(F.col("event_name") == "generate_lead", 1).otherwise(0))
             .cast("long").alias("fez_lead"),
            F.max(F.when(F.col("event_name") == "purchase", 1).otherwise(0))
             .cast("long").alias("comprou"),
        )
        .withColumn(
            "duracao_minutos",
            F.round(
                (F.col("fim_sessao").cast("long") - F.col("inicio_sessao").cast("long")) / 60.0,
                2,
            ),
        )
    )

    base = (
        resumo_completo
        .join(contexto, on="session_id", how="left")
        .join(pre_cart, on="session_id", how="left")
        .join(plano, on="session_id", how="left")
        .fillna(0.0, subset=FEATURES_NUMERICAS)
        .fillna({
            "canal": "(desconhecido)",
            "dispositivo": "(desconhecido)",
            "plano_selecionado": "(nenhum)",
        })
        .withColumn(
            "visitor_id",
            F.coalesce("user_id", "user_pseudo_id", "session_id"),
        )
        .withColumn("label_compra", F.col("comprou").cast("double"))
    )
    return base


def treinar_classificador(base):
    """Treina regressão logística ponderada e avalia no conjunto de teste."""
    treino, teste = base.randomSplit([1.0 - TEST_SIZE, TEST_SIZE], seed=SEED)
    total_treino = treino.count()
    total_teste = teste.count()

    contagens = {
        int(linha["label_compra"]): int(linha["count"])
        for linha in treino.groupBy("label_compra").count().collect()
    }
    contagens_teste = {
        int(linha["label_compra"]): int(linha["count"])
        for linha in teste.groupBy("label_compra").count().collect()
    }
    if (
        total_treino == 0
        or total_teste == 0
        or 0 not in contagens
        or 1 not in contagens
        or 0 not in contagens_teste
        or 1 not in contagens_teste
    ):
        raise RuntimeError(
            "A divisão treino/teste precisa conter sessões com e sem compra nos dois conjuntos. "
            f"Treino={contagens}; teste={contagens_teste}."
        )

    # Pesos inversos à frequência ajudam o modelo a considerar a classe compradora,
    # que é minoritária neste conjunto de dados.
    peso_sem_compra = total_treino / (2.0 * contagens[0])
    peso_com_compra = total_treino / (2.0 * contagens[1])
    treino_ponderado = treino.withColumn(
        "peso_classe",
        F.when(F.col("label_compra") == 1.0, F.lit(peso_com_compra))
         .otherwise(F.lit(peso_sem_compra)),
    )

    indexadores = [
        StringIndexer(inputCol="canal", outputCol="canal_idx", handleInvalid="keep"),
        StringIndexer(inputCol="dispositivo", outputCol="dispositivo_idx", handleInvalid="keep"),
        StringIndexer(inputCol="plano_selecionado", outputCol="plano_idx", handleInvalid="keep"),
    ]
    encoder = OneHotEncoder(
        inputCols=["canal_idx", "dispositivo_idx", "plano_idx"],
        outputCols=["canal_ohe", "dispositivo_ohe", "plano_ohe"],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=FEATURES_NUMERICAS + ["canal_ohe", "dispositivo_ohe", "plano_ohe"],
        outputCol="features_brutas",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="features_brutas",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    classificador = LogisticRegression(
        featuresCol="features",
        labelCol="label_compra",
        weightCol="peso_classe",
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        maxIter=80,
        regParam=0.05,
        elasticNetParam=0.0,
        threshold=LIMIAR_CLASSIFICACAO,
        standardization=False,
    )
    pipeline = Pipeline(stages=indexadores + [encoder, assembler, scaler, classificador])
    modelo = pipeline.fit(treino_ponderado)

    predicoes_teste = modelo.transform(teste)
    avaliador_auc = BinaryClassificationEvaluator(
        labelCol="label_compra",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc = float(avaliador_auc.evaluate(predicoes_teste))

    contagem_metricas = predicoes_teste.agg(
        F.sum(F.when((F.col("label_compra") == 1.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("label_compra") == 0.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("label_compra") == 1.0) & (F.col("prediction") == 0.0), 1).otherwise(0)).alias("fn"),
    ).first()
    tp = int(contagem_metricas["tp"] or 0)
    fp = int(contagem_metricas["fp"] or 0)
    fn = int(contagem_metricas["fn"] or 0)
    precisao = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (2.0 * precisao * recall / (precisao + recall)) if precisao + recall else 0.0

    ids_teste = teste.select("session_id").distinct().withColumn("conjunto", F.lit("teste"))
    todas_predicoes = modelo.transform(base).join(ids_teste, on="session_id", how="left")
    todas_predicoes = (
        todas_predicoes
        .withColumn("conjunto", F.coalesce(F.col("conjunto"), F.lit("treino")))
        .withColumn("probabilidade_compra", vector_to_array("probability").getItem(1))
        .select(
            "session_id",
            "visitor_id",
            "user_pseudo_id",
            "user_id",
            "canal",
            "dispositivo",
            "plano_selecionado",
            F.col("label_compra").cast("int").alias("comprou"),
            F.round("probabilidade_compra", 6).alias("probabilidade_compra"),
            F.col("prediction").cast("int").alias("previsao_compra"),
            "conjunto",
        )
    )

    features_texto = ", ".join(FEATURES_NUMERICAS + FEATURES_CATEGORICAS)
    metricas = base.sparkSession.createDataFrame([(
        "LogisticRegression",
        round(auc, 4),
        round(f1, 4),
        round(precisao, 4),
        round(recall, 4),
        LIMIAR_CLASSIFICACAO,
        total_treino,
        total_teste,
        SEED,
        features_texto,
    )], [
        "modelo", "auc_teste", "f1_teste", "precisao_teste", "recall_teste",
        "limiar_classificacao", "amostras_treino", "amostras_teste", "seed",
        "features_utilizadas",
    ])
    resumo = {
        "auc": auc,
        "f1": f1,
        "precisao": precisao,
        "recall": recall,
        "treino": total_treino,
        "teste": total_teste,
        "positivos_treino": contagens[1],
    }
    return todas_predicoes, metricas, resumo


def criar_segmentos_visitantes(base):
    """Agrupa visitantes, aplica K-Means e adiciona o perfil médio do cluster."""
    visitantes = (
        base.groupBy("visitor_id")
        .agg(
            F.countDistinct("session_id").cast("double").alias("total_sessoes"),
            F.sum("total_eventos").cast("double").alias("total_eventos"),
            F.avg("duracao_minutos").alias("duracao_media_minutos"),
            F.avg("scroll_maximo_sessao").alias("scroll_medio_maximo"),
            F.avg("video_percent_maximo_sessao").alias("video_percent_medio_maximo"),
            F.sum("fez_lead").cast("double").alias("total_leads"),
            F.sum("comprou").cast("double").alias("total_compras"),
        )
        .fillna(0.0)
    )

    features_cluster = [
        "total_sessoes",
        "total_eventos",
        "duracao_media_minutos",
        "scroll_medio_maximo",
        "video_percent_medio_maximo",
        "total_leads",
        "total_compras",
    ]
    assembler = VectorAssembler(
        inputCols=features_cluster,
        outputCol="perfil_bruto",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="perfil_bruto",
        outputCol="features_cluster",
        withMean=True,
        withStd=True,
    )
    visitantes_assemblados = assembler.transform(visitantes)
    modelo_scaler = scaler.fit(visitantes_assemblados)
    preparado = modelo_scaler.transform(visitantes_assemblados)
    quantidade_visitantes = preparado.count()
    if quantidade_visitantes < NUM_CLUSTERS:
        raise RuntimeError(
            f"São necessários pelo menos {NUM_CLUSTERS} visitantes para executar o K-Means; "
            f"foram encontrados {quantidade_visitantes}."
        )

    kmeans = KMeans(
        k=NUM_CLUSTERS,
        seed=SEED,
        maxIter=40,
        featuresCol="features_cluster",
        predictionCol="grupo",
        initMode="k-means||",
    )
    modelo_cluster = kmeans.fit(preparado)
    agrupados = modelo_cluster.transform(preparado).withColumn("grupo", F.col("grupo").cast("int"))

    janela_grupo = Window.partitionBy("grupo")
    resultado = agrupados
    for coluna in features_cluster:
        resultado = resultado.withColumn(
            f"media_grupo_{coluna}", F.avg(F.col(coluna)).over(janela_grupo)
        )
    resultado = resultado.withColumn(
        "visitantes_no_grupo", F.count(F.lit(1)).over(janela_grupo).cast("long")
    )

    return resultado.select(
        "visitor_id",
        "grupo",
        "total_sessoes",
        "total_eventos",
        "duracao_media_minutos",
        "scroll_medio_maximo",
        "video_percent_medio_maximo",
        "total_leads",
        "total_compras",
        "visitantes_no_grupo",
        *[f"media_grupo_{coluna}" for coluna in features_cluster],
    ), quantidade_visitantes


def gravar_tabela(df, nome: str) -> None:
    df.write.mode("overwrite").parquet(caminho(f"gold/{nome}"))


def main():
    spark = criar_sessao("gold_machine_learning")
    try:
        titulo("JOB GOLD MACHINE LEARNING - DADOSPRO")
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
            {
                "event_id", "event_name", "event_ts", "session_id", "user_pseudo_id",
                "user_id", "channel", "device_category", "percent_scrolled", "video_percent",
            },
            "silver.eventos",
        )
        validar_colunas(
            itens,
            {"event_id", "event_name", "event_ts", "session_id", "item_id", "item_category"},
            "silver.itens",
        )

        print("Construindo features pré-carrinho e rótulo de compra por sessão...")
        base = construir_features_sessao(eventos, itens).cache()
        total_sessoes = base.count()
        total_compras = base.filter(F.col("label_compra") == 1.0).count()
        if total_compras == 0 or total_compras == total_sessoes:
            raise RuntimeError("O conjunto precisa conter sessões com e sem compra para treinar o classificador.")

        print("Treinando regressão logística e avaliando no conjunto de teste...")
        propensao, metricas, resumo_modelo = treinar_classificador(base)

        print("Segmentando visitantes com K-Means...")
        segmentos, total_visitantes = criar_segmentos_visitantes(base)

        print("Gravando gold/propensao_sessao/...")
        gravar_tabela(propensao, "propensao_sessao")
        print("Gravando gold/metricas_modelo/...")
        gravar_tabela(metricas, "metricas_modelo")
        print("Gravando gold/segmentos_visitantes/...")
        gravar_tabela(segmentos, "segmentos_visitantes")

        titulo("RESULTADO DA GOLD MACHINE LEARNING")
        print(f"Sessões avaliadas:             {total_sessoes:,}")
        print(f"Sessões com compra:            {total_compras:,}")
        print(f"Visitantes segmentados:        {total_visitantes:,}")
        print(f"Amostras de treino:            {resumo_modelo['treino']:,}")
        print(f"Amostras de teste:             {resumo_modelo['teste']:,}")
        print(f"Compras no treino:             {resumo_modelo['positivos_treino']:,}")
        print(f"AUC no teste:                  {resumo_modelo['auc']:.4f}")
        print(f"F1 no teste:                   {resumo_modelo['f1']:.4f}")
        print(f"Precisão no teste:             {resumo_modelo['precisao']:.4f}")
        print(f"Recall no teste:               {resumo_modelo['recall']:.4f}")
        print("\nMétricas do modelo:")
        metricas.show(truncate=False)
        print("\nQuantidade de visitantes por grupo:")
        segmentos.groupBy("grupo").agg(
            F.count("*").alias("visitantes"),
            F.avg("total_sessoes").alias("media_sessoes"),
            F.avg("total_compras").alias("media_compras"),
        ).orderBy("grupo").show(truncate=False)

        print("\nDestinos:")
        for tabela in ("propensao_sessao", "metricas_modelo", "segmentos_visitantes"):
            print(f"  s3a://datalake-tracking/gold/{tabela}/")
        print("\nFeatures comportamentais limitadas a eventos anteriores ao primeiro add_to_cart.")
        print("\nJob Gold Machine Learning concluído com sucesso.")
        base.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
