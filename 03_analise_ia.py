"""
PARTE 3 — Análise de mercado com IA (Claude API)
=================================================
Lê o banco SQLite (mercado.db), monta um resumo estruturado por categoria
e envia para o Claude gerar insights em linguagem natural.

Saída: arquivo JSON com análises + arquivo .txt com relatório legível.

Instalar dependências:
    pip install anthropic pandas

Configurar chave de API:
    export ANTHROPIC_API_KEY="sk-ant-..."   (Linux/Mac)
    set ANTHROPIC_API_KEY=sk-ant-...        (Windows)
"""

import os
import json
import sqlite3
import pandas as pd
from datetime import datetime
import anthropic

BANCO   = "mercado.db"
MODELO  = "claude-sonnet-4-20250514"
CLIENT  = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


# ─── 1. Extração de dados do banco ───────────────────────────────────────────

def carregar_resumo_categorias(banco: str = BANCO) -> pd.DataFrame:
    conn = sqlite3.connect(banco)
    df = pd.read_sql("SELECT * FROM v_resumo_categoria", conn)
    conn.close()
    return df


def carregar_top_produtos(banco: str = BANCO, top_n: int = 5) -> dict[str, list]:
    """Retorna os top N produtos por categoria (última coleta)."""
    conn = sqlite3.connect(banco)
    categorias = pd.read_sql(
        "SELECT DISTINCT categoria FROM produtos", conn
    )["categoria"].tolist()

    top_por_categoria = {}
    for cat in categorias:
        df = pd.read_sql(f"""
            SELECT titulo, marca, preco_atual, desconto_pct,
                   quantidade_vendida, frete_gratis, estado, score
            FROM v_ranking_categoria
            WHERE categoria = '{cat}'
            LIMIT {top_n}
        """, conn)
        top_por_categoria[cat] = df.to_dict(orient="records")

    conn.close()
    return top_por_categoria


def carregar_distribuicao_estados(banco: str = BANCO) -> pd.DataFrame:
    conn = sqlite3.connect(banco)
    df = pd.read_sql("""
        SELECT estado, SUM(total_vendas) as vendas, SUM(total_produtos) as produtos
        FROM v_por_estado
        GROUP BY estado
        ORDER BY vendas DESC
        LIMIT 10
    """, conn)
    conn.close()
    return df


# ─── 2. Monta o contexto para o Claude ───────────────────────────────────────

def montar_contexto(
    resumo: pd.DataFrame,
    top_produtos: dict,
    estados: pd.DataFrame,
) -> str:
    """
    Serializa os dados em texto estruturado para enviar ao Claude.
    Quanto mais organizado, melhor o insight gerado.
    """
    linhas = []

    # Resumo geral por categoria
    linhas.append("## RESUMO POR CATEGORIA (última coleta)\n")
    linhas.append(resumo.to_string(index=False))

    # Top produtos por categoria
    linhas.append("\n\n## TOP PRODUTOS POR CATEGORIA\n")
    for cat, produtos in top_produtos.items():
        linhas.append(f"\n### {cat}")
        for i, p in enumerate(produtos, 1):
            frete = "frete grátis" if p["frete_gratis"] else "frete cobrado"
            linhas.append(
                f"  {i}. {p['titulo']} | "
                f"R${p['preco_atual']:.2f} | "
                f"{p['desconto_pct']:.0f}% desc | "
                f"{p['quantidade_vendida']} vendas | "
                f"{frete} | {p['estado']}"
            )

    # Distribuição por estado
    linhas.append("\n\n## VENDAS POR ESTADO (top 10)\n")
    linhas.append(estados.to_string(index=False))

    return "\n".join(linhas)


# ─── 3. Prompts para o Claude ────────────────────────────────────────────────

SYSTEM_PROMPT = """
Você é um analista de mercado especializado em e-commerce brasileiro.
Recebe dados estruturados de produtos do Mercado Livre e gera análises
objetivas, práticas e em português do Brasil.

Suas análises devem:
- Identificar tendências reais nos dados
- Destacar oportunidades e riscos por categoria
- Apontar padrões regionais relevantes
- Ser diretas e úteis para quem quer vender online
- Usar linguagem clara, sem jargão excessivo
- Sempre citar números concretos dos dados recebidos
""".strip()


def prompt_visao_geral(contexto: str) -> str:
    return f"""
Analise os dados de mercado abaixo e gere um relatório com:

1. **Visão geral do mercado**: quais categorias dominam em volume e valor
2. **Categorias em destaque**: as 3 com melhor desempenho e por quê
3. **Padrão de preços**: faixas predominantes, categorias com mais desconto
4. **Padrão regional**: quais estados concentram mais vendas e o que isso indica
5. **Oportunidades**: onde há menos competição ou preços com margem interessante
6. **Alertas**: categorias com sinais de saturação ou margens muito baixas

Dados:
{contexto}
""".strip()


def prompt_categoria(nome: str, dados_categoria: str) -> str:
    return f"""
Faça uma análise detalhada da categoria "{nome}" com base nos dados abaixo.

Inclua:
1. **Produto destaque**: qual lidera e por quê (preço, frete, desconto)
2. **Faixa de preço competitiva**: onde estão os produtos que mais vendem
3. **Papel do frete grátis**: impacto nas vendas dessa categoria
4. **Marcas em evidência** (se disponível nos dados)
5. **Recomendação para vendedor**: o que fazer para competir nessa categoria

Dados da categoria:
{dados_categoria}
""".strip()


# ─── 4. Chamadas à API do Claude ─────────────────────────────────────────────

def analisar_com_claude(prompt: str, max_tokens: int = 1500) -> str:
    """Envia um prompt ao Claude e retorna o texto da resposta."""
    try:
        response = CLIENT.messages.create(
            model=MODELO,
            max_tokens=max_tokens,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except anthropic.APIError as e:
        return f"[ERRO NA API] {e}"


def analisar_visao_geral(contexto: str) -> str:
    print("  Gerando visão geral do mercado...")
    return analisar_com_claude(prompt_visao_geral(contexto), max_tokens=2000)


def analisar_categorias(top_produtos: dict) -> dict[str, str]:
    """Gera análise individual para cada categoria."""
    analises = {}
    for cat, produtos in top_produtos.items():
        print(f"  Analisando categoria: {cat}...")
        dados_str = json.dumps(produtos, ensure_ascii=False, indent=2)
        analises[cat] = analisar_com_claude(
            prompt_categoria(cat, dados_str),
            max_tokens=1000,
        )
    return analises


# ─── 5. Salva os resultados ───────────────────────────────────────────────────

def salvar_resultados(
    visao_geral: str,
    analises_cat: dict[str, str],
    contexto: str,
) -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    # JSON estruturado (para integrar com dashboard ou banco)
    resultado_json = {
        "gerado_em": datetime.now().isoformat(),
        "modelo": MODELO,
        "visao_geral": visao_geral,
        "por_categoria": analises_cat,
    }
    arq_json = f"analise_ia_{timestamp}.json"
    with open(arq_json, "w", encoding="utf-8") as f:
        json.dump(resultado_json, f, ensure_ascii=False, indent=2)
    print(f"\n  JSON salvo: {arq_json}")

    # Relatório em texto legível
    arq_txt = f"relatorio_mercado_{timestamp}.txt"
    with open(arq_txt, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("  RELATÓRIO DE MERCADO — MERCADO LIVRE BRASIL\n")
        f.write(f"  Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
        f.write("=" * 60 + "\n\n")

        f.write("VISÃO GERAL\n")
        f.write("-" * 40 + "\n")
        f.write(visao_geral + "\n\n")

        f.write("\nANÁLISE POR CATEGORIA\n")
        f.write("-" * 40 + "\n")
        for cat, analise in analises_cat.items():
            f.write(f"\n{'=' * 40}\n")
            f.write(f"  {cat.upper()}\n")
            f.write(f"{'=' * 40}\n")
            f.write(analise + "\n")

        f.write("\n\n--- DADOS BRUTOS USADOS NA ANÁLISE ---\n")
        f.write(contexto)

    print(f"  Relatório salvo: {arq_txt}")


# ─── Execução principal ──────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Análise com IA — Parte 3")
    print("=" * 50)

    # Verifica chave de API
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise EnvironmentError(
            "Variável ANTHROPIC_API_KEY não encontrada.\n"
            "Configure com: export ANTHROPIC_API_KEY='sk-ant-...'"
        )

    # Carrega dados do banco
    print("\n  Carregando dados do banco...")
    resumo   = carregar_resumo_categorias()
    top_prod = carregar_top_produtos(top_n=5)
    estados  = carregar_distribuicao_estados()

    print(f"  Categorias encontradas: {len(top_prod)}")
    print(f"  Estados encontrados: {len(estados)}")

    # Monta contexto para o Claude
    contexto = montar_contexto(resumo, top_prod, estados)

    # Análises com IA
    print("\n  Enviando para o Claude...")
    visao_geral   = analisar_visao_geral(contexto)
    analises_cat  = analisar_categorias(top_prod)

    # Salva resultados
    salvar_resultados(visao_geral, analises_cat, contexto)

    # Preview no terminal
    print("\n" + "=" * 50)
    print("  PRÉVIA — VISÃO GERAL")
    print("=" * 50)
    print(visao_geral[:800] + "..." if len(visao_geral) > 800 else visao_geral)

    print("\n  Análise concluída!")
    print("  Próximo passo: rode o script 04_dashboard.py")
