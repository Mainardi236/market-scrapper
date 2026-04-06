"""
PARTE 4 — Dashboard interativo com Streamlit
============================================
Visualiza os dados do banco (mercado.db) e os insights gerados pela IA.

Instalar dependências:
    pip install streamlit pandas plotly

Rodar:
    streamlit run 04_dashboard.py
"""

import sqlite3
import json
import glob
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime

BANCO = "mercado.db"

# ─── Configuração da página ──────────────────────────────────────────────────

st.set_page_config(
    page_title="Análise de Mercado — Mercado Livre",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .metric-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 16px 20px;
        border-left: 4px solid #3273DC;
    }
    .insight-box {
        background: #eef6ff;
        border-radius: 8px;
        padding: 16px;
        border-left: 4px solid #2366d1;
        font-size: 0.95rem;
        line-height: 1.7;
    }
    .tag {
        display: inline-block;
        background: #dbeafe;
        color: #1e40af;
        border-radius: 4px;
        padding: 2px 8px;
        font-size: 0.8rem;
        margin: 2px;
    }
</style>
""", unsafe_allow_html=True)


# ─── Funções de carregamento ─────────────────────────────────────────────────

@st.cache_data(ttl=300)  # cache por 5 minutos
def carregar_dados(banco: str = BANCO) -> dict:
    if not os.path.exists(banco):
        return {}

    conn = sqlite3.connect(banco)

    produtos     = pd.read_sql("SELECT * FROM produtos", conn)
    resumo       = pd.read_sql("SELECT * FROM v_resumo_categoria", conn)
    ranking      = pd.read_sql("SELECT * FROM v_ranking_categoria", conn)
    por_estado   = pd.read_sql("SELECT * FROM v_por_estado", conn)

    conn.close()

    produtos["coletado_em"] = pd.to_datetime(produtos["coletado_em"], errors="coerce")
    produtos["frete_gratis"] = produtos["frete_gratis"].astype(bool)

    return {
        "produtos":   produtos,
        "resumo":     resumo,
        "ranking":    ranking,
        "por_estado": por_estado,
    }


def carregar_analise_ia() -> dict | None:
    """Carrega o JSON de análise mais recente gerado pelo script 03."""
    arquivos = sorted(glob.glob("analise_ia_*.json"), reverse=True)
    if not arquivos:
        return None
    with open(arquivos[0], encoding="utf-8") as f:
        return json.load(f)


# ─── Sidebar ─────────────────────────────────────────────────────────────────

def sidebar(dados: dict) -> dict:
    st.sidebar.title("🔎 Filtros")

    categorias = sorted(dados["produtos"]["categoria"].dropna().unique().tolist())
    cat_sel = st.sidebar.multiselect(
        "Categorias",
        options=categorias,
        default=categorias,
    )

    estados = sorted(dados["produtos"]["estado"].dropna().unique().tolist())
    est_sel = st.sidebar.multiselect(
        "Estados",
        options=estados,
        default=estados,
    )

    faixa_opcoes = ["Todos", "Até R$50", "R$51–200", "R$201–500",
                    "R$501–1k", "R$1k–3k", "Acima R$3k"]
    faixa_sel = st.sidebar.selectbox("Faixa de preço", faixa_opcoes)

    frete_sel = st.sidebar.radio("Frete grátis", ["Todos", "Sim", "Não"])

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Banco: `{BANCO}`")

    total = len(dados["produtos"])
    ultima = dados["produtos"]["coletado_em"].max()
    st.sidebar.caption(f"Total de registros: **{total:,}**")
    st.sidebar.caption(f"Última coleta: **{ultima.strftime('%d/%m/%Y %H:%M') if pd.notna(ultima) else '—'}**")

    return {
        "categorias": cat_sel,
        "estados":    est_sel,
        "faixa":      faixa_sel,
        "frete":      frete_sel,
    }


def aplicar_filtros(df: pd.DataFrame, filtros: dict) -> pd.DataFrame:
    if filtros["categorias"]:
        df = df[df["categoria"].isin(filtros["categorias"])]
    if filtros["estados"]:
        df = df[df["estado"].isin(filtros["estados"])]
    if filtros["faixa"] != "Todos":
        df = df[df["faixa_preco"] == filtros["faixa"]]
    if filtros["frete"] == "Sim":
        df = df[df["frete_gratis"] == True]
    elif filtros["frete"] == "Não":
        df = df[df["frete_gratis"] == False]
    return df


# ─── Seções do dashboard ─────────────────────────────────────────────────────

def secao_metricas(df: pd.DataFrame) -> None:
    st.subheader("📈 Visão geral")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total de produtos",    f"{len(df):,}")
    c2.metric("Preço médio",          f"R$ {df['preco_atual'].mean():,.2f}")
    c3.metric("Total de vendas",      f"{df['quantidade_vendida'].sum():,}")
    c4.metric("Com frete grátis",     f"{df['frete_gratis'].sum():,}")
    c5.metric("Desconto médio",       f"{df['desconto_pct'].mean():.1f}%")


def secao_ranking(df: pd.DataFrame) -> None:
    st.subheader("🏆 Ranking de produtos")

    cat = st.selectbox(
        "Selecione a categoria",
        options=sorted(df["categoria"].unique()),
        key="rank_cat",
    )

    top = (
        df[df["categoria"] == cat]
        .sort_values("score", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    top.index += 1

    # Tabela formatada
    exibir = top[[
        "titulo", "marca", "preco_atual", "desconto_pct",
        "quantidade_vendida", "frete_gratis", "estado",
    ]].rename(columns={
        "titulo":             "Produto",
        "marca":              "Marca",
        "preco_atual":        "Preço (R$)",
        "desconto_pct":       "Desconto %",
        "quantidade_vendida": "Vendas",
        "frete_gratis":       "Frete grátis",
        "estado":             "Estado",
    })

    exibir["Frete grátis"] = exibir["Frete grátis"].map({True: "✅", False: "—"})

    st.dataframe(
        exibir,
        use_container_width=True,
        column_config={
            "Preço (R$)":  st.column_config.NumberColumn(format="R$ %.2f"),
            "Desconto %":  st.column_config.NumberColumn(format="%.1f%%"),
            "Vendas":      st.column_config.NumberColumn(format="%d"),
        },
    )


def secao_graficos(df: pd.DataFrame, resumo: pd.DataFrame) -> None:
    st.subheader("📊 Gráficos")

    col1, col2 = st.columns(2)

    with col1:
        # Vendas por categoria
        fig = px.bar(
            resumo.sort_values("total_vendas", ascending=True),
            x="total_vendas",
            y="categoria",
            orientation="h",
            title="Total de vendas por categoria",
            color="total_vendas",
            color_continuous_scale="Blues",
            labels={"total_vendas": "Vendas", "categoria": ""},
        )
        fig.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Preço médio por categoria
        fig2 = px.bar(
            resumo.sort_values("preco_medio", ascending=True),
            x="preco_medio",
            y="categoria",
            orientation="h",
            title="Preço médio por categoria (R$)",
            color="preco_medio",
            color_continuous_scale="Greens",
            labels={"preco_medio": "Preço médio (R$)", "categoria": ""},
        )
        fig2.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Distribuição de preços (histograma)
        fig3 = px.histogram(
            df[df["preco_atual"] < df["preco_atual"].quantile(0.95)],
            x="preco_atual",
            nbins=40,
            title="Distribuição de preços (excl. outliers)",
            color_discrete_sequence=["#3273DC"],
            labels={"preco_atual": "Preço (R$)", "count": "Produtos"},
        )
        fig3.update_layout(height=320)
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        # Frete grátis por categoria
        frete = (
            df.groupby(["categoria", "frete_gratis"])
            .size()
            .reset_index(name="qtd")
        )
        frete["frete_gratis"] = frete["frete_gratis"].map({True: "Grátis", False: "Cobrado"})
        fig4 = px.bar(
            frete,
            x="categoria",
            y="qtd",
            color="frete_gratis",
            title="Frete grátis vs cobrado por categoria",
            barmode="stack",
            color_discrete_map={"Grátis": "#23d160", "Cobrado": "#ff3860"},
            labels={"qtd": "Produtos", "categoria": "", "frete_gratis": "Frete"},
        )
        fig4.update_layout(height=320, xaxis_tickangle=-30)
        st.plotly_chart(fig4, use_container_width=True)


def secao_regioes(por_estado: pd.DataFrame) -> None:
    st.subheader("🗺️ Distribuição regional")

    vendas_estado = (
        por_estado
        .groupby("estado")[["total_vendas", "total_produtos"]]
        .sum()
        .reset_index()
        .sort_values("total_vendas", ascending=False)
    )

    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.bar(
            vendas_estado.head(15),
            x="estado",
            y="total_vendas",
            title="Top estados por volume de vendas",
            color="total_vendas",
            color_continuous_scale="Purples",
            labels={"total_vendas": "Vendas", "estado": "Estado"},
        )
        fig.update_layout(coloraxis_showscale=False, height=360)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Ranking de estados**")
        st.dataframe(
            vendas_estado.head(10).rename(columns={
                "estado":         "Estado",
                "total_vendas":   "Vendas",
                "total_produtos": "Produtos",
            }),
            use_container_width=True,
            hide_index=True,
        )


def secao_insights_ia(analise: dict) -> None:
    st.subheader("🤖 Insights gerados pela IA")

    gerado_em = analise.get("gerado_em", "")
    if gerado_em:
        dt = datetime.fromisoformat(gerado_em)
        st.caption(f"Análise gerada em {dt.strftime('%d/%m/%Y às %H:%M')} · modelo: `{analise.get('modelo','')}`")

    # Visão geral
    with st.expander("📋 Visão geral do mercado", expanded=True):
        st.markdown(
            f'<div class="insight-box">{analise["visao_geral"]}</div>',
            unsafe_allow_html=True,
        )

    # Por categoria
    st.markdown("#### Análise por categoria")
    cats = list(analise.get("por_categoria", {}).keys())
    if cats:
        tabs = st.tabs(cats)
        for tab, cat in zip(tabs, cats):
            with tab:
                texto = analise["por_categoria"][cat]
                st.markdown(
                    f'<div class="insight-box">{texto}</div>',
                    unsafe_allow_html=True,
                )


# ─── App principal ───────────────────────────────────────────────────────────

def main() -> None:
    st.title("📊 Análise de Mercado — Mercado Livre Brasil")
    st.caption("Pipeline: coleta → limpeza → IA → visualização")

    # Verifica banco
    if not os.path.exists(BANCO):
        st.error(
            f"Banco `{BANCO}` não encontrado. "
            "Rode primeiro os scripts **01**, **02** e **03**."
        )
        st.code("python 01_coletor_mercadolivre.py\n"
                "python 02_limpeza_e_banco.py\n"
                "python 03_analise_ia.py")
        return

    dados   = carregar_dados()
    analise = carregar_analise_ia()
    filtros = sidebar(dados)

    df_filtrado = aplicar_filtros(dados["produtos"].copy(), filtros)

    if df_filtrado.empty:
        st.warning("Nenhum produto encontrado com os filtros selecionados.")
        return

    # Tabs principais
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Visão geral",
        "🏆 Ranking",
        "🗺️ Regiões",
        "🤖 Insights IA",
    ])

    with tab1:
        secao_metricas(df_filtrado)
        st.divider()
        resumo_filtrado = (
            df_filtrado
            .groupby("categoria")
            .agg(
                total_produtos=("id", "count"),
                preco_medio=("preco_atual", "mean"),
                total_vendas=("quantidade_vendida", "sum"),
                desconto_medio=("desconto_pct", "mean"),
            )
            .reset_index()
            .rename(columns={
                "preco_medio":    "preco_medio",
                "desconto_medio": "desconto_medio_pct",
            })
        )
        secao_graficos(df_filtrado, resumo_filtrado)

    with tab2:
        secao_ranking(df_filtrado)

    with tab3:
        secao_regioes(dados["por_estado"])

    with tab4:
        if analise:
            secao_insights_ia(analise)
        else:
            st.info(
                "Nenhuma análise de IA encontrada. "
                "Rode o script **03_analise_ia.py** para gerar os insights."
            )
            st.code("python 03_analise_ia.py")


if __name__ == "__main__":
    main()
