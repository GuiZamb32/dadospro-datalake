"""Orquestra os oito jobs do datalake DadosPro em ordem (requisito R3).

Execute na raiz do repositório, dentro do contêiner app:
    docker compose run --rm app python pipeline/executar_pipeline.py

Este script NÃO gera eventos. Ele parte da landing que já existe no bucket.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


RAIZ_PROJETO = Path(__file__).resolve().parents[1]
JOBS = (
    ("Bronze", "pipeline/bronze.py"),
    ("Silver", "pipeline/silver.py"),
    ("Gold — Funil", "pipeline/gold_funil.py"),
    ("Gold — Engajamento", "pipeline/gold_engajamento.py"),
    ("Gold — Receita e Aquisição", "pipeline/gold_receita_aquisicao.py"),
    ("Gold — Machine Learning", "pipeline/gold_machine_learning.py"),
    ("Gold — Mapa de Calor", "pipeline/gold_mapa_calor.py"),
    ("Exportação do Mapa de Calor (R11)", "pipeline/exportar_mapa_calor.py"),
)


def main() -> int:
    print("=" * 72)
    print("PIPELINE COMPLETO — DADOSPRO")
    print("=" * 72)
    print("Origem: landing existente no bucket datalake-tracking")
    print("Este processo não gera nem apaga os eventos da landing.\n")

    ausentes = [arquivo for _, arquivo in JOBS if not (RAIZ_PROJETO / arquivo).is_file()]
    if ausentes:
        print("Não foi possível iniciar. Arquivos de job ausentes:")
        for arquivo in ausentes:
            print(f"  - {arquivo}")
        return 1

    for indice, (nome, arquivo) in enumerate(JOBS, start=1):
        print("\n" + "#" * 72)
        print(f"ETAPA {indice}/{len(JOBS)} — {nome}")
        print(f"Executando: {arquivo}")
        print("#" * 72, flush=True)

        resultado = subprocess.run(
            [sys.executable, arquivo],
            cwd=RAIZ_PROJETO,
            check=False,
        )

        if resultado.returncode != 0:
            print(
                f"\nPIPELINE INTERROMPIDO: {nome} terminou com código "
                f"{resultado.returncode}. Corrija o erro e execute novamente."
            )
            return resultado.returncode or 1

        print(f"\nEtapa {indice}/{len(JOBS)} concluída: {nome}", flush=True)

    print("\n" + "=" * 72)
    print("PIPELINE COMPLETO CONCLUÍDO COM SUCESSO")
    print("Bronze, Silver, cinco jobs Gold e exportação R11 executados.")
    print("Arquivo local esperado: site/mapa_calor.js")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
