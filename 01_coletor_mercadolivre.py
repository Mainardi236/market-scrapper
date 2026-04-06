"""
PARTE 1 — Coleta de dados da API do Mercado Livre
================================================
Coleta produtos mais vendidos por categoria e salva em JSON local.
Não precisa de chave de API para leitura pública.

Instalar dependências:
    pip install requests
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "https://api.mercadolibre.com"

# ─── Categorias que queremos monitorar ───────────────────────────────────────
# IDs de categorias do Mercado Livre Brasil (MLB)
# Você pode descobrir mais em: https://api.mercadolibre.com/sites/MLB/categories
CATEGORIAS = {
    "Eletrônicos":     "MLB1000",
    "Celulares":       "MLB1051",
    "Computadores":    "MLB1648",
    "Moda":            "MLB1430",
    "Casa e Jardim":   "MLB1574",
    "Esportes":        "MLB1276",
    "Beleza":          "MLB1246",
    "Brinquedos":      "MLB5726",
}

# ─── Funções de coleta ───────────────────────────────────────────────────────

def buscar_mais_vendidos(categoria_id: str, limite: int = 50) -> list[dict]:
    """
    Busca os produtos mais vendidos de uma categoria.
    Ordena por 'sold_quantity' (quantidade vendida).
    """
    url = f"{BASE_URL}/sites/MLB/search"
    params = {
        "category": categoria_id,
        "sort": "sold_quantity",   # ordena pelos mais vendidos
        "limit": min(limite, 50),  # API aceita no máximo 50 por vez
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        dados = response.json()
        return dados.get("results", [])
    except requests.RequestException as e:
        print(f"  [ERRO] Falha ao buscar categoria {categoria_id}: {e}")
        return []


def extrair_campos(produto: dict, nome_categoria: str) -> dict:
    """
    Extrai apenas os campos relevantes de cada produto.
    """
    # Preço com desconto ou preço normal
    preco_original = produto.get("original_price") or produto.get("price", 0)
    preco_atual    = produto.get("price", 0)
    desconto       = 0
    if preco_original and preco_atual and preco_original > preco_atual:
        desconto = round((1 - preco_atual / preco_original) * 100, 1)

    # Localização do vendedor
    endereco = produto.get("address") or {}
    cidade   = endereco.get("city_name", "Desconhecida")
    estado   = endereco.get("state_name", "Desconhecido")

    # Atributos extras (como marca, modelo etc.)
    atributos = {}
    for attr in produto.get("attributes", []):
        atributos[attr.get("name", "")] = attr.get("value_name", "")

    return {
        "id":               produto.get("id"),
        "titulo":           produto.get("title"),
        "categoria":        nome_categoria,
        "preco_atual":      preco_atual,
        "preco_original":   preco_original,
        "desconto_pct":     desconto,
        "quantidade_vendida": produto.get("sold_quantity", 0),
        "condicao":         produto.get("condition"),          # new / used
        "tipo_anuncio":     produto.get("listing_type_id"),    # gold_special etc.
        "frete_gratis":     produto.get("shipping", {}).get("free_shipping", False),
        "cidade":           cidade,
        "estado":           estado,
        "reputacao_vendedor": produto.get("seller", {}).get("seller_reputation", {}).get("level_id"),
        "marca":            atributos.get("Marca", ""),
        "link":             produto.get("permalink"),
        "coletado_em":      datetime.now().isoformat(),
    }


def coletar_todas_categorias(categorias: dict, limite_por_categoria: int = 30) -> list[dict]:
    """
    Itera pelas categorias e coleta os mais vendidos de cada uma.
    """
    todos_produtos = []

    for nome, cat_id in categorias.items():
        print(f"  Coletando: {nome} ({cat_id})...")
        produtos_raw = buscar_mais_vendidos(cat_id, limite=limite_por_categoria)
        produtos     = [extrair_campos(p, nome) for p in produtos_raw]
        todos_produtos.extend(produtos)
        print(f"    → {len(produtos)} produtos coletados")
        time.sleep(0.5)  # respeita o rate limit da API

    return todos_produtos


def salvar_json(dados: list[dict], caminho: str) -> None:
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    print(f"\n  Salvo em: {caminho}")


# ─── Execução principal ──────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Coletor de Dados — Mercado Livre Brasil")
    print("=" * 50)

    produtos = coletar_todas_categorias(CATEGORIAS, limite_por_categoria=30)

    print(f"\n  Total coletado: {len(produtos)} produtos")

    # Salva resultado
    arquivo_saida = f"dados_mercado_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    salvar_json(produtos, arquivo_saida)

    # Preview no terminal
    print("\n  Exemplo do primeiro produto:")
    if produtos:
        for chave, valor in produtos[0].items():
            print(f"    {chave}: {valor}")
