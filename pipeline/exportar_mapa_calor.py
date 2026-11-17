"""Job R11: exporta as tabelas Gold para site/mapa_calor.js.

Uso, na raiz do repositorio:
    docker compose run --rm app python pipeline/exportar_mapa_calor.py

O arquivo JavaScript e um resultado local gerado; nao deve ser versionado.
"""

import json
from datetime import datetime, timezone

from comum import arquivo_do_projeto, caminho, criar_sessao, exigir_camada, titulo


def main():
    spark = criar_sessao("exportar_mapa_calor")

    try:
        titulo("JOB EXPORTAR MAPA DE CALOR - R11")
        exigir_camada(
            spark,
            "gold/mapa_calor_cliques",
            "docker compose run --rm app python pipeline/gold_mapa_calor.py",
        )
        exigir_camada(
            spark,
            "gold/mapa_calor_rolagem",
            "docker compose run --rm app python pipeline/gold_mapa_calor.py",
        )

        print("Lendo as pequenas tabelas Gold pelo Spark...")
        cliques = [
            linha.asDict(recursive=True)
            for linha in spark.read.parquet(caminho("gold/mapa_calor_cliques")).collect()
        ]
        rolagem = [
            linha.asDict(recursive=True)
            for linha in spark.read.parquet(caminho("gold/mapa_calor_rolagem")).collect()
        ]

        dados = {
            "gerado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cliques": cliques,
            "rolagem": rolagem,
        }
        texto_json = json.dumps(
            dados,
            ensure_ascii=False,
            default=float,
            separators=(",", ":"),
        )

        destino = arquivo_do_projeto("site/mapa_calor.js")
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text("window.MAPA_CALOR = " + texto_json + ";\n", encoding="utf-8")

        titulo("RESULTADO DA EXPORTACAO")
        print(f"Linhas de cliques exportadas: {len(cliques):,}")
        print(f"Marcos de rolagem exportados: {len(rolagem):,}")
        print(f"Arquivo gerado: {destino}")
        print("\nJob de exportacao do mapa de calor concluido com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
