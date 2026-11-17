"""Job Gold — Receita e Aquisição do projeto DadosPro.

Cria as tabelas:
- gold/receita_diaria: valores por data da compra, plano e canal.
- gold/taxa_reembolso: pedidos reembolsados por plano e canal.
- gold/adesao_order_bump: adesão à Mentoria em grupo.
- gold/lead_para_compra: primeira conversão de lead em compra por pessoa.

As regras de negócio seguem docs/ENUNCIADO.md, seção 5.4.

Execução, na raiz do repositório:
    docker compose run --rm app python pipeline/gold_receita_aquisicao.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, ler_eventos, titulo


ITEM_ORDER_BUMP = "BUMP-MENTORIA"


def validar_colunas(df: DataFrame, obrigatorias: set[str], origem: str) -> None:
    """Interrompe com mensagem clara caso o schema da origem tenha mudado."""
    ausentes = sorted(obrigatorias - set(df.columns))
    if ausentes:
        raise ValueError(
            f"A origem {origem} não contém as colunas obrigatórias: {', '.join(ausentes)}"
        )


def gravar_tabela(df: DataFrame, nome: str) -> None:
    """Grava uma tabela Gold independente em Parquet."""
    df.write.mode("overwrite").parquet(caminho(f"gold/{nome}"))


def canais_das_sessoes(eventos: DataFrame) -> DataFrame:
    """Obtém source / medium do primeiro evento de cada sessão (definição 5.4.3)."""
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


def plano_da_compra(itens: DataFrame) -> DataFrame:
    """Identifica o item de categoria 'plano' associado a cada evento purchase."""
    planos = (
        itens
        .filter(
            (F.col("event_name") == "purchase")
            & (F.col("item_category") == "plano")
            & F.col("item_id").isNotNull()
        )
        .groupBy("event_id")
        .agg(F.min("item_id").alias("plano"))
    )
    return planos


def compras_com_contexto(eventos: DataFrame, itens: DataFrame, canais: DataFrame) -> DataFrame:
    """Monta uma linha por compra, com data, plano e canal de aquisição."""
    compras = (
        eventos
        .filter(
            (F.col("event_name") == "purchase")
            & F.col("transaction_id").isNotNull()
            & (F.length(F.trim(F.col("transaction_id"))) > 0)
            & F.col("value").isNotNull()
        )
        .select(
            "event_id",
            "transaction_id",
            "event_ts",
            "session_id",
            F.col("value").cast("double").alias("valor_compra"),
        )
        .withColumn("data_compra", F.to_date("event_ts"))
    )

    return (
        compras
        .join(plano_da_compra(itens), on="event_id", how="left")
        .join(canais, on="session_id", how="left")
        .withColumn("plano", F.coalesce(F.col("plano"), F.lit("(sem plano)")))
        .withColumn("canal", F.coalesce(F.col("canal"), F.lit("(desconhecido)")))
    )


def reembolsos_por_transacao(eventos: DataFrame) -> DataFrame:
    """Soma os refunds válidos por transaction_id para atribuí-los à compra original."""
    return (
        eventos
        .filter(
            (F.col("event_name") == "refund")
            & F.col("transaction_id").isNotNull()
            & (F.length(F.trim(F.col("transaction_id"))) > 0)
            & F.col("value").isNotNull()
        )
        .groupBy("transaction_id")
        .agg(
            F.sum(F.col("value").cast("double")).alias("receita_reembolsada"),
            F.countDistinct("event_id").alias("quantidade_reembolsos"),
        )
    )


def criar_receita_diaria(compras: DataFrame, reembolsos: DataFrame) -> DataFrame:
    """Agrega receita por data da compra, plano e canal."""
    por_compra = (
        compras
        .join(reembolsos, on="transaction_id", how="left")
        .withColumn(
            "receita_reembolsada",
            F.coalesce(F.col("receita_reembolsada"), F.lit(0.0)),
        )
    )

    return (
        por_compra
        .groupBy("data_compra", "plano", "canal")
        .agg(
            F.sum("valor_compra").alias("receita_bruta"),
            F.sum("receita_reembolsada").alias("receita_reembolsada"),
            F.countDistinct("transaction_id").cast("long").alias("pedidos"),
        )
        .withColumn(
            "receita_liquida",
            F.col("receita_bruta") - F.col("receita_reembolsada"),
        )
        .withColumn(
            "ticket_medio",
            F.try_divide(F.col("receita_bruta"), F.col("pedidos").cast("double")),
        )
        .select(
            "data_compra",
            "plano",
            "canal",
            F.round("receita_bruta", 2).alias("receita_bruta"),
            F.round("receita_reembolsada", 2).alias("receita_reembolsada"),
            F.round("receita_liquida", 2).alias("receita_liquida"),
            "pedidos",
            F.round("ticket_medio", 2).alias("ticket_medio"),
        )
        .orderBy("data_compra", "plano", "canal")
    )


def criar_taxa_reembolso(compras: DataFrame, reembolsos: DataFrame) -> DataFrame:
    """Calcula pedidos com reembolso / pedidos por plano e canal da compra."""
    por_compra = (
        compras
        .join(
            reembolsos.select("transaction_id", "quantidade_reembolsos"),
            on="transaction_id",
            how="left",
        )
        .withColumn(
            "teve_reembolso",
            F.when(F.coalesce(F.col("quantidade_reembolsos"), F.lit(0)) > 0, 1).otherwise(0),
        )
    )

    return (
        por_compra
        .groupBy("plano", "canal")
        .agg(
            F.countDistinct("transaction_id").cast("long").alias("pedidos"),
            F.countDistinct(
                F.when(F.col("teve_reembolso") == 1, F.col("transaction_id"))
            ).cast("long").alias("pedidos_reembolsados"),
        )
        .withColumn(
            "taxa_reembolso",
            F.round(
                F.try_divide(
                    F.col("pedidos_reembolsados").cast("double"),
                    F.col("pedidos").cast("double"),
                ),
                4,
            ),
        )
        .orderBy("plano", "canal")
    )


def criar_adesao_order_bump(compras: DataFrame, itens: DataFrame) -> DataFrame:
    """Calcula adesão ao BUMP-MENTORIA no total e por plano x canal."""
    compras_com_bump = (
        itens
        .filter(F.col("item_id") == ITEM_ORDER_BUMP)
        .select("event_id")
        .distinct()
        .withColumn("tem_order_bump", F.lit(1))
    )

    base = (
        compras
        .join(compras_com_bump, on="event_id", how="left")
        .withColumn(
            "tem_order_bump",
            F.coalesce(F.col("tem_order_bump"), F.lit(0)),
        )
    )

    por_grupo = (
        base
        .groupBy("plano", "canal")
        .agg(
            F.countDistinct("transaction_id").cast("long").alias("pedidos"),
            F.countDistinct(
                F.when(F.col("tem_order_bump") == 1, F.col("transaction_id"))
            ).cast("long").alias("pedidos_com_order_bump"),
        )
        .withColumn("escopo", F.lit("plano_canal"))
    )

    total = (
        base
        .agg(
            F.countDistinct("transaction_id").cast("long").alias("pedidos"),
            F.countDistinct(
                F.when(F.col("tem_order_bump") == 1, F.col("transaction_id"))
            ).cast("long").alias("pedidos_com_order_bump"),
        )
        .withColumn("escopo", F.lit("total"))
        .withColumn("plano", F.lit("(todos)"))
        .withColumn("canal", F.lit("(todos)"))
    )

    return (
        por_grupo.unionByName(total)
        .withColumn(
            "pct_com_order_bump",
            F.round(
                F.try_divide(
                    F.col("pedidos_com_order_bump").cast("double"),
                    F.col("pedidos").cast("double"),
                ),
                4,
            ),
        )
        .select(
            "escopo", "plano", "canal", "pedidos", "pedidos_com_order_bump", "pct_com_order_bump"
        )
        .orderBy("escopo", "plano", "canal")
    )


def chave_pessoa():
    """Expressão estável para pessoa: user_id resolvido, senão user_pseudo_id."""
    return F.when(
        F.col("user_id").isNotNull() & (F.length(F.trim(F.col("user_id"))) > 0),
        F.concat(F.lit("user:"), F.col("user_id")),
    ).when(
        F.col("user_pseudo_id").isNotNull() & (F.length(F.trim(F.col("user_pseudo_id"))) > 0),
        F.concat(F.lit("pseudo:"), F.col("user_pseudo_id")),
    )


def criar_lead_para_compra(eventos: DataFrame, canais: DataFrame) -> DataFrame:
    """Relaciona o primeiro lead da pessoa à primeira compra estritamente posterior.

    A saída contém uma linha total e uma linha por canal do primeiro lead.
    A mediana considera apenas pessoas que compraram depois do lead.
    """
    eventos_pessoa = (
        eventos
        .withColumn("_pessoa", chave_pessoa())
        .filter(F.col("_pessoa").isNotNull())
    )

    leads = (
        eventos_pessoa
        .filter((F.col("event_name") == "generate_lead") & F.col("event_ts").isNotNull())
        .join(canais, on="session_id", how="left")
        .withColumn("canal_lead", F.coalesce(F.col("canal"), F.lit("(desconhecido)")))
    )
    janela_primeiro_lead = Window.partitionBy("_pessoa").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc()
    )
    primeiro_lead = (
        leads
        .withColumn("_ordem_lead", F.row_number().over(janela_primeiro_lead))
        .filter(F.col("_ordem_lead") == 1)
        .select(
            "_pessoa",
            F.col("event_ts").alias("ts_primeiro_lead"),
            "canal_lead",
        )
    )

    compras_pessoa = (
        eventos_pessoa
        .filter(
            (F.col("event_name") == "purchase")
            & F.col("event_ts").isNotNull()
            & F.col("transaction_id").isNotNull()
            & (F.length(F.trim(F.col("transaction_id"))) > 0)
        )
        .select(
            "_pessoa",
            F.col("event_ts").alias("ts_compra"),
            "transaction_id",
        )
    )

    compras_depois_lead = (
        primeiro_lead.alias("l")
        .join(
            compras_pessoa.alias("c"),
            (F.col("l._pessoa") == F.col("c._pessoa"))
            & (F.col("c.ts_compra") > F.col("l.ts_primeiro_lead")),
            how="left",
        )
        .groupBy(
            F.col("l._pessoa").alias("_pessoa"),
            F.col("l.ts_primeiro_lead").alias("ts_primeiro_lead"),
            F.col("l.canal_lead").alias("canal_lead"),
        )
        .agg(F.min("c.ts_compra").alias("ts_primeira_compra_depois"))
        .withColumn(
            "dias_ate_compra",
            F.when(
                F.col("ts_primeira_compra_depois").isNotNull(),
                (
                    F.col("ts_primeira_compra_depois").cast("long")
                    - F.col("ts_primeiro_lead").cast("long")
                ) / F.lit(86400.0),
            ),
        )
        .withColumn(
            "comprou_depois",
            F.when(F.col("ts_primeira_compra_depois").isNotNull(), 1).otherwise(0),
        )
    )

    por_canal = (
        compras_depois_lead
        .groupBy("canal_lead")
        .agg(
            F.countDistinct("_pessoa").cast("long").alias("leads"),
            F.sum("comprou_depois").cast("long").alias("leads_com_compra"),
            F.expr("percentile_approx(dias_ate_compra, 0.5)").alias("mediana_dias_ate_compra"),
        )
        .withColumn("escopo", F.lit("canal"))
        .withColumnRenamed("canal_lead", "canal")
    )

    total = (
        compras_depois_lead
        .agg(
            F.countDistinct("_pessoa").cast("long").alias("leads"),
            F.sum("comprou_depois").cast("long").alias("leads_com_compra"),
            F.expr("percentile_approx(dias_ate_compra, 0.5)").alias("mediana_dias_ate_compra"),
        )
        .withColumn("escopo", F.lit("total"))
        .withColumn("canal", F.lit("(todos)"))
    )

    return (
        por_canal.unionByName(total)
        .withColumn(
            "taxa_lead_para_compra",
            F.round(
                F.try_divide(
                    F.col("leads_com_compra").cast("double"),
                    F.col("leads").cast("double"),
                ),
                4,
            ),
        )
        .select(
            "escopo",
            "canal",
            "leads",
            "leads_com_compra",
            "taxa_lead_para_compra",
            F.round("mediana_dias_ate_compra", 4).alias("mediana_dias_ate_compra"),
        )
        .orderBy("escopo", "canal")
    )


def main() -> None:
    spark = criar_sessao("gold_receita_aquisicao")
    try:
        titulo("JOB GOLD RECEITA E AQUISIÇÃO - DADOSPRO")

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

        print("Lendo eventos e itens aprovados da Silver...")
        eventos = ler_eventos(spark, "silver")
        itens = spark.read.parquet(caminho("silver/itens"))

        validar_colunas(
            eventos,
            {
                "event_id", "event_name", "event_ts", "session_id", "user_id",
                "user_pseudo_id", "transaction_id", "value", "channel",
            },
            "silver.eventos",
        )
        validar_colunas(
            itens,
            {"event_id", "event_name", "transaction_id", "item_id", "item_category"},
            "silver.itens",
        )

        print("Identificando o canal do primeiro evento de cada sessão...")
        canais = canais_das_sessoes(eventos)

        print("Montando compras com plano, canal e data de compra...")
        compras = compras_com_contexto(eventos, itens, canais).cache()
        total_pedidos = compras.select("transaction_id").distinct().count()

        print("Relacionando reembolsos às compras por transaction_id...")
        reembolsos = reembolsos_por_transacao(eventos).cache()

        print("Calculando receita diária...")
        receita_diaria = criar_receita_diaria(compras, reembolsos)

        print("Calculando taxa de reembolso...")
        taxa_reembolso = criar_taxa_reembolso(compras, reembolsos)

        print("Calculando adesão ao order bump...")
        adesao_order_bump = criar_adesao_order_bump(compras, itens)

        print("Calculando lead para compra...")
        lead_para_compra = criar_lead_para_compra(eventos, canais)

        tabelas = {
            "receita_diaria": receita_diaria,
            "taxa_reembolso": taxa_reembolso,
            "adesao_order_bump": adesao_order_bump,
            "lead_para_compra": lead_para_compra,
        }

        for nome, df in tabelas.items():
            print(f"Gravando gold/{nome}/...")
            gravar_tabela(df, nome)

        total_receita_bruta = compras.agg(F.sum("valor_compra")).first()[0] or 0.0
        total_reembolsado = (
            compras.join(reembolsos, on="transaction_id", how="left")
            .agg(F.sum(F.coalesce(F.col("receita_reembolsada"), F.lit(0.0))))
            .first()[0]
            or 0.0
        )
        total_leads = (
            lead_para_compra
            .filter(F.col("escopo") == "total")
            .select("leads")
            .first()[0]
        )
        leads_convertidos = (
            lead_para_compra
            .filter(F.col("escopo") == "total")
            .select("leads_com_compra")
            .first()[0]
        )

        titulo("RESULTADO DA GOLD RECEITA E AQUISIÇÃO")
        print(f"Pedidos válidos analisados: {total_pedidos:,}")
        print(f"Receita bruta: R$ {total_receita_bruta:,.2f}")
        print(f"Receita reembolsada: R$ {total_reembolsado:,.2f}")
        print(f"Receita líquida: R$ {total_receita_bruta - total_reembolsado:,.2f}")
        print(f"Pessoas com lead: {total_leads:,}")
        print(f"Pessoas que compraram após o lead: {leads_convertidos:,}")

        print("\nTaxa de reembolso por plano e canal:")
        taxa_reembolso.show(100, truncate=False)
        print("\nAdesão ao order bump:")
        adesao_order_bump.filter(F.col("escopo") == "total").show(20, truncate=False)
        print("\nLead para compra:")
        lead_para_compra.show(100, truncate=False)

        print("\nDestinos:")
        for nome in tabelas:
            print(f"  s3a://datalake-tracking/gold/{nome}/")
        print("\nJob Gold Receita e Aquisição concluído com sucesso.")

        compras.unpersist()
        reembolsos.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
