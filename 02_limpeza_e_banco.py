"""
PARTE 2 — Limpeza de dados e armazenamento em SQLite
=====================================================
Lê o JSON gerado pelo coletor (Parte 1), limpa e normaliza os dados,
e salva em um banco SQLite local (mercado.db).

O banco acumula coletas ao longo do tempo — cada rodada do coletor
vai adicionando novos registros, permitindo análise de crescimento.

Instalar dependências:
    pip install pandas   (sqlite3 já vem com Python)
"""

import sqlite3
import json
import glob
import os
import pandas as pd
from datetime import datetime

BANCO = "mercado.db"

# ─── 1. Leitura dos arquivos JSON gerados pelo coletor ───────────────────────

def carregar_jsons(padrao: str = "dados_mercado_*.json") -> pd.DataFrame:
    """
    Carrega todos os arquivos JSON do coletor encontrados na pasta atual.
    Útil quando você tem várias coletas acumuladas.
    """
    arquivos = sorted(glob.glob(padrao))

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão '{padrao}'.\n"
            "Rode primeiro o script 01_coletor_mercadolivre.py"
        )

    print(f"  Arquivos encontrados: {len(arquivos)}")
    for a in arquivos:
        print(f"    - {a}")

    dfs = []
    for arquivo in arquivos:
        with open(arquivo, encoding="utf-8") as f:
            dados = json.load(f)
        df = pd.DataFrame(dados)
        df["arquivo_origem"] = os.path.basename(arquivo)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


# ─── 2. Limpeza e normalização ───────────────────────────────────────────────

def limpar(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n  Registros brutos: {len(df)}")

    # Remove duplicatas exatas (mesmo id + mesma data de coleta)
    df = df.drop_duplicates(subset=["id", "coletado_em"])

    # Garante tipos corretos
    df["preco_atual"]         = pd.to_numeric(df["preco_atual"],         errors="coerce")
    df["preco_original"]      = pd.to_numeric(df["preco_original"],      errors="coerce")
    df["desconto_pct"]        = pd.to_numeric(df["desconto_pct"],        errors="coerce").fillna(0)
    df["quantidade_vendida"]  = pd.to_numeric(df["quantidade_vendida"],  errors="coerce").fillna(0).astype(int)
    df["frete_gratis"]        = df["frete_gratis"].astype(bool)
    df["coletado_em"]         = pd.to_datetime(df["coletado_em"],        errors="coerce")

    # Remove produtos sem preço ou sem título
    antes = len(df)
    df = df[df["preco_atual"].notna() & (df["preco_atual"] > 0)]
    df = df[df["titulo"].notna() & (df["titulo"].str.strip() != "")]
    print(f"  Removidos sem preço/título: {antes - len(df)}")

    # Normaliza texto
    df["titulo"]    = df["titulo"].str.strip()
    df["categoria"] = df["categoria"].str.strip()
    df["estado"]    = df["estado"].str.strip().replace("", "Desconhecido").fillna("Desconhecido")
    df["cidade"]    = df["cidade"].str.strip().replace("", "Desconhecida").fillna("Desconhecida")
    df["marca"]     = df["marca"].str.strip().fillna("")
    df["condicao"]  = df["condicao"].map({"new": "Novo", "used": "Usado"}).fillna("Desconhecido")

    # Faixa de preço (útil para filtros no dashboard)
    bins   = [0, 50, 200, 500, 1000, 3000, float("inf")]
    labels = ["Até R$50", "R$51–200", "R$201–500", "R$501–1k", "R$1k–3k", "Acima R$3k"]
    df["faixa_preco"] = pd.cut(df["preco_atual"], bins=bins, labels=labels, right=True)

    # Score simples de relevância (mais vendidos com frete grátis sobem)
    df["score"] = (
        df["quantidade_vendida"] * 1.0
        + df["frete_gratis"].astype(int) * df["quantidade_vendida"] * 0.1
        - df["desconto_pct"] * (-0.5)   # desconto alto pode indicar queima de estoque
    ).round(2)

    print(f"  Registros limpos: {len(df)}")
    return df.reset_index(drop=True)


# ─── 3. Banco de dados SQLite ────────────────────────────────────────────────

def criar_tabelas(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS produtos (
            id                TEXT,
            titulo            TEXT,
            categoria         TEXT,
            preco_atual       REAL,
            preco_original    REAL,
            desconto_pct      REAL,
            quantidade_vendida INTEGER,
            condicao          TEXT,
            tipo_anuncio      TEXT,
            frete_gratis      INTEGER,
            cidade            TEXT,
            estado            TEXT,
            reputacao_vendedor TEXT,
            marca             TEXT,
            faixa_preco       TEXT,
            score             REAL,
            link              TEXT,
            arquivo_origem    TEXT,
            coletado_em       TEXT,
            PRIMARY KEY (id, coletado_em)
        );

        -- View: ranking por categoria (última coleta)
        CREATE VIEW IF NOT EXISTS v_ranking_categoria AS
        SELECT
            categoria,
            titulo,
            marca,
            preco_atual,
            desconto_pct,
            quantidade_vendida,
            frete_gratis,
            estado,
            score,
            coletado_em
        FROM produtos
        WHERE coletado_em = (SELECT MAX(coletado_em) FROM produtos)
        ORDER BY categoria, score DESC;

        -- View: resumo por categoria
        CREATE VIEW IF NOT EXISTS v_resumo_categoria AS
        SELECT
            categoria,
            COUNT(*)                        AS total_produtos,
            ROUND(AVG(preco_atual), 2)      AS preco_medio,
            ROUND(MIN(preco_atual), 2)      AS preco_minimo,
            ROUND(MAX(preco_atual), 2)      AS preco_maximo,
            SUM(quantidade_vendida)         AS total_vendas,
            ROUND(AVG(desconto_pct), 1)     AS desconto_medio_pct,
            SUM(frete_gratis)               AS com_frete_gratis
        FROM produtos
        WHERE coletado_em = (SELECT MAX(coletado_em) FROM produtos)
        GROUP BY categoria
        ORDER BY total_vendas DESC;

        -- View: distribuição por estado
        CREATE VIEW IF NOT EXISTS v_por_estado AS
        SELECT
            estado,
            categoria,
            COUNT(*)               AS total_produtos,
            SUM(quantidade_vendida) AS total_vendas
        FROM produtos
        WHERE coletado_em = (SELECT MAX(coletado_em) FROM produtos)
        GROUP BY estado, categoria
        ORDER BY total_vendas DESC;
    """)
    conn.commit()


def salvar_banco(df: pd.DataFrame, banco: str = BANCO) -> None:
    conn = sqlite3.connect(banco)
    criar_tabelas(conn)

    # Prepara colunas para inserção
    colunas = [
        "id", "titulo", "categoria", "preco_atual", "preco_original",
        "desconto_pct", "quantidade_vendida", "condicao", "tipo_anuncio",
        "frete_gratis", "cidade", "estado", "reputacao_vendedor", "marca",
        "faixa_preco", "score", "link", "arquivo_origem", "coletado_em",
    ]

    df_insert = df[colunas].copy()
    df_insert["frete_gratis"] = df_insert["frete_gratis"].astype(int)
    df_insert["coletado_em"]  = df_insert["coletado_em"].astype(str)
    df_insert["faixa_preco"]  = df_insert["faixa_preco"].astype(str)

    # INSERT OR IGNORE evita duplicar registros em rodadas repetidas
    df_insert.to_sql("produtos", conn, if_exists="append", index=False, method="multi")

    total = conn.execute("SELECT COUNT(*) FROM produtos").fetchone()[0]
    print(f"\n  Banco '{banco}' atualizado.")
    print(f"  Total de registros no banco: {total}")
    conn.close()


# ─── 4. Relatório rápido pós-importação ─────────────────────────────────────

def relatorio_rapido(banco: str = BANCO) -> None:
    conn = sqlite3.connect(banco)

    print("\n" + "=" * 50)
    print("  RESUMO POR CATEGORIA (última coleta)")
    print("=" * 50)
    df_resumo = pd.read_sql("SELECT * FROM v_resumo_categoria", conn)
    print(df_resumo.to_string(index=False))

    print("\n" + "=" * 50)
    print("  TOP 5 ESTADOS COM MAIS VENDAS")
    print("=" * 50)
    df_estados = pd.read_sql("""
        SELECT estado, SUM(total_vendas) as vendas
        FROM v_por_estado
        GROUP BY estado
        ORDER BY vendas DESC
        LIMIT 5
    """, conn)
    print(df_estados.to_string(index=False))

    conn.close()


# ─── Execução principal ──────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Limpeza e Banco de Dados — Parte 2")
    print("=" * 50)

    df_bruto  = carregar_jsons()
    df_limpo  = limpar(df_bruto)
    salvar_banco(df_limpo)
    relatorio_rapido()

    print("\n  Pronto! Banco 'mercado.db' criado.")
    print("  Próximo passo: rode o script 03_analise_ia.py")
