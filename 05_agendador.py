"""
PARTE 5 — Agendamento automático + Alertas (E-mail e Telegram)
==============================================================
Roda o pipeline completo automaticamente no horário configurado e
envia alertas quando detecta produtos em alta ou mudanças relevantes.

Instalar dependências:
    pip install schedule requests anthropic pandas

Configurar variáveis de ambiente (crie um arquivo .env ou exporte):

    # Anthropic
    ANTHROPIC_API_KEY=sk-ant-...

    # E-mail (Gmail recomendado — use uma senha de app, não a senha normal)
    EMAIL_REMETENTE=seuemail@gmail.com
    EMAIL_SENHA=sua_senha_de_app
    EMAIL_DESTINATARIO=destinatario@email.com

    # Telegram (opcional — pode usar só e-mail se preferir)
    TELEGRAM_TOKEN=123456:ABC-seu-token
    TELEGRAM_CHAT_ID=123456789

Como obter o token do Telegram:
    1. Fale com @BotFather no Telegram e crie um bot (/newbot)
    2. Copie o token gerado
    3. Mande uma mensagem para o bot e acesse:
       https://api.telegram.org/bot<TOKEN>/getUpdates
    4. Copie o "chat_id" do resultado

Como criar senha de app no Gmail:
    Conta Google → Segurança → Verificação em duas etapas → Senhas de app

Rodar em segundo plano:
    python 05_agendador.py

    # Ou em background no Linux/Mac:
    nohup python 05_agendador.py > agendador.log 2>&1 &
"""

import os
import json
import time
import sqlite3
import smtplib
import logging
import schedule
import traceback
import subprocess
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

import requests as http_requests

# ─── Configurações ────────────────────────────────────────────────────────────

# Horários de execução do pipeline completo (formato HH:MM)
HORARIO_COLETA_DIARIA   = "07:00"   # coleta + análise todo dia cedo
HORARIO_RELATORIO_SEMANAL = "08:00" # relatório completo às segundas

# Thresholds para disparar alertas
ALERTA_CRESCIMENTO_PCT  = 30.0   # alerta se vendas crescerem mais que 30%
ALERTA_PRECO_QUEDA_PCT  = 20.0   # alerta se preço médio cair mais que 20%
ALERTA_TOP_N_PRODUTOS   = 5      # quantos produtos incluir nos alertas

BANCO = "mercado.db"

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("agendador.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─── 1. Envio de mensagens ────────────────────────────────────────────────────

def enviar_email(assunto: str, corpo_html: str) -> bool:
    """Envia e-mail via SMTP (Gmail)."""
    remetente   = os.environ.get("EMAIL_REMETENTE")
    senha       = os.environ.get("EMAIL_SENHA")
    destinatario = os.environ.get("EMAIL_DESTINATARIO")

    if not all([remetente, senha, destinatario]):
        log.warning("E-mail não configurado — pulando envio.")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = assunto
        msg["From"]    = remetente
        msg["To"]      = destinatario
        msg.attach(MIMEText(corpo_html, "html", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as servidor:
            servidor.login(remetente, senha)
            servidor.sendmail(remetente, destinatario, msg.as_string())

        log.info(f"E-mail enviado: {assunto}")
        return True

    except Exception as e:
        log.error(f"Falha ao enviar e-mail: {e}")
        return False


def enviar_telegram(mensagem: str) -> bool:
    """Envia mensagem via Telegram Bot API."""
    token   = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not all([token, chat_id]):
        log.warning("Telegram não configurado — pulando envio.")
        return False

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id":    chat_id,
            "text":       mensagem,
            "parse_mode": "HTML",           # suporta <b>, <i>, <code>
        }
        resp = http_requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Mensagem Telegram enviada.")
        return True

    except Exception as e:
        log.error(f"Falha ao enviar Telegram: {e}")
        return False


def notificar(assunto: str, corpo_html: str, corpo_texto: str) -> None:
    """Envia para todos os canais configurados."""
    enviar_email(assunto, corpo_html)
    enviar_telegram(corpo_texto)


# ─── 2. Detecção de alertas ───────────────────────────────────────────────────

def comparar_coletas(banco: str = BANCO) -> list[dict]:
    """
    Compara a última coleta com a anterior e detecta variações relevantes.
    Retorna uma lista de alertas encontrados.
    """
    if not os.path.exists(banco):
        return []

    conn = sqlite3.connect(banco)

    # Busca as duas últimas datas de coleta distintas
    datas = pd.read_sql("""
        SELECT DISTINCT DATE(coletado_em) as data
        FROM produtos
        ORDER BY data DESC
        LIMIT 2
    """, conn)["data"].tolist()

    if len(datas) < 2:
        log.info("Menos de 2 coletas disponíveis — sem comparação possível ainda.")
        conn.close()
        return []

    data_atual, data_anterior = datas[0], datas[1]

    # Agrega vendas por categoria em cada data
    def agregar(data):
        return pd.read_sql(f"""
            SELECT categoria,
                   SUM(quantidade_vendida) as total_vendas,
                   AVG(preco_atual)        as preco_medio,
                   COUNT(*)               as total_produtos
            FROM produtos
            WHERE DATE(coletado_em) = '{data}'
            GROUP BY categoria
        """, conn)

    df_atual     = agregar(data_atual).set_index("categoria")
    df_anterior  = agregar(data_anterior).set_index("categoria")

    conn.close()

    alertas = []
    categorias = df_atual.index.intersection(df_anterior.index)

    for cat in categorias:
        vendas_atual    = df_atual.loc[cat, "total_vendas"]
        vendas_anterior = df_anterior.loc[cat, "total_vendas"]
        preco_atual     = df_atual.loc[cat, "preco_medio"]
        preco_anterior  = df_anterior.loc[cat, "preco_medio"]

        # Crescimento de vendas
        if vendas_anterior > 0:
            cresc = (vendas_atual - vendas_anterior) / vendas_anterior * 100
            if cresc >= ALERTA_CRESCIMENTO_PCT:
                alertas.append({
                    "tipo":      "📈 Crescimento de vendas",
                    "categoria": cat,
                    "mensagem":  f"Vendas cresceram {cresc:.1f}% ({int(vendas_anterior):,} → {int(vendas_atual):,})",
                    "valor":     cresc,
                })

        # Queda de preço
        if preco_anterior > 0:
            queda = (preco_anterior - preco_atual) / preco_anterior * 100
            if queda >= ALERTA_PRECO_QUEDA_PCT:
                alertas.append({
                    "tipo":      "💰 Queda de preço",
                    "categoria": cat,
                    "mensagem":  f"Preço médio caiu {queda:.1f}% (R${preco_anterior:.2f} → R${preco_atual:.2f})",
                    "valor":     queda,
                })

    return alertas


def buscar_top_produtos(banco: str = BANCO) -> pd.DataFrame:
    """Retorna os top produtos da última coleta para incluir nos relatórios."""
    if not os.path.exists(banco):
        return pd.DataFrame()
    conn = sqlite3.connect(banco)
    df = pd.read_sql(f"""
        SELECT categoria, titulo, preco_atual, quantidade_vendida,
               desconto_pct, frete_gratis, estado
        FROM v_ranking_categoria
        GROUP BY categoria
        HAVING score = MAX(score)
        LIMIT {ALERTA_TOP_N_PRODUTOS * 3}
    """, conn)
    conn.close()
    return df


# ─── 3. Montagem dos templates de mensagem ───────────────────────────────────

def template_email_alerta(alertas: list[dict], top_produtos: pd.DataFrame) -> tuple[str, str]:
    """Retorna (assunto, corpo_html) do e-mail de alerta."""
    data_str = datetime.now().strftime("%d/%m/%Y")
    assunto  = f"🚨 Alertas de mercado — {data_str}"

    linhas_alertas = ""
    for a in alertas:
        linhas_alertas += f"""
        <tr>
          <td style="padding:8px;border-bottom:1px solid #eee">{a['tipo']}</td>
          <td style="padding:8px;border-bottom:1px solid #eee"><b>{a['categoria']}</b></td>
          <td style="padding:8px;border-bottom:1px solid #eee">{a['mensagem']}</td>
        </tr>"""

    linhas_produtos = ""
    for _, p in top_produtos.iterrows():
        frete = "✅" if p["frete_gratis"] else "—"
        linhas_produtos += f"""
        <tr>
          <td style="padding:8px;border-bottom:1px solid #eee">{p['categoria']}</td>
          <td style="padding:8px;border-bottom:1px solid #eee">{p['titulo'][:60]}...</td>
          <td style="padding:8px;border-bottom:1px solid #eee">R$ {p['preco_atual']:.2f}</td>
          <td style="padding:8px;border-bottom:1px solid #eee">{int(p['quantidade_vendida']):,}</td>
          <td style="padding:8px;border-bottom:1px solid #eee">{frete}</td>
        </tr>"""

    corpo = f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;max-width:700px;margin:auto">
      <h2 style="color:#2366d1;border-bottom:2px solid #2366d1;padding-bottom:8px">
        📊 Relatório de Mercado — {data_str}
      </h2>

      <h3>🚨 Alertas detectados ({len(alertas)})</h3>
      <table style="width:100%;border-collapse:collapse;font-size:14px">
        <tr style="background:#f0f4ff">
          <th style="padding:8px;text-align:left">Tipo</th>
          <th style="padding:8px;text-align:left">Categoria</th>
          <th style="padding:8px;text-align:left">Detalhe</th>
        </tr>
        {linhas_alertas}
      </table>

      <h3 style="margin-top:28px">🏆 Produtos em destaque</h3>
      <table style="width:100%;border-collapse:collapse;font-size:14px">
        <tr style="background:#f0f4ff">
          <th style="padding:8px;text-align:left">Categoria</th>
          <th style="padding:8px;text-align:left">Produto</th>
          <th style="padding:8px;text-align:left">Preço</th>
          <th style="padding:8px;text-align:left">Vendas</th>
          <th style="padding:8px;text-align:left">Frete</th>
        </tr>
        {linhas_produtos}
      </table>

      <p style="margin-top:24px;font-size:12px;color:#888">
        Gerado automaticamente em {datetime.now().strftime('%d/%m/%Y às %H:%M')} •
        Pipeline: Mercado Livre → SQLite → Claude IA
      </p>
    </body></html>
    """
    return assunto, corpo


def template_telegram_alerta(alertas: list[dict], top_produtos: pd.DataFrame) -> str:
    """Retorna o texto formatado para o Telegram."""
    data_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    linhas = [f"📊 <b>Relatório de Mercado</b> — {data_str}\n"]

    if alertas:
        linhas.append(f"🚨 <b>{len(alertas)} alerta(s) detectado(s):</b>")
        for a in alertas:
            linhas.append(f"  {a['tipo']} — <b>{a['categoria']}</b>\n  {a['mensagem']}")
    else:
        linhas.append("✅ Nenhum alerta no momento.")

    linhas.append("\n🏆 <b>Produtos em destaque:</b>")
    for _, p in top_produtos.head(ALERTA_TOP_N_PRODUTOS).iterrows():
        frete = "🚚 grátis" if p["frete_gratis"] else ""
        linhas.append(
            f"  • <b>{p['categoria']}</b>: {p['titulo'][:45]}...\n"
            f"    R$ {p['preco_atual']:.2f} | {int(p['quantidade_vendida']):,} vendas {frete}"
        )

    return "\n".join(linhas)


def template_email_relatorio_semanal(analise_ia: dict | None) -> tuple[str, str]:
    """Relatório semanal com insights da IA."""
    data_str = datetime.now().strftime("%d/%m/%Y")
    assunto  = f"📋 Relatório semanal de mercado — {data_str}"

    visao = analise_ia.get("visao_geral", "Análise não disponível.") if analise_ia else "Rode o script 03 para gerar análise."
    visao_html = visao.replace("\n", "<br>")

    corpo = f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;max-width:700px;margin:auto">
      <h2 style="color:#2366d1;border-bottom:2px solid #2366d1;padding-bottom:8px">
        📋 Relatório Semanal — {data_str}
      </h2>
      <div style="background:#eef6ff;border-left:4px solid #2366d1;padding:16px;border-radius:4px">
        {visao_html}
      </div>
      <p style="margin-top:24px;font-size:12px;color:#888">
        Acesse o dashboard completo em: <code>streamlit run 04_dashboard.py</code>
      </p>
    </body></html>
    """
    return assunto, corpo


# ─── 4. Execução do pipeline ──────────────────────────────────────────────────

def rodar_script(nome: str) -> bool:
    """Executa um script Python do pipeline e retorna True se ok."""
    log.info(f"Iniciando: {nome}")
    try:
        resultado = subprocess.run(
            ["python", nome],
            capture_output=True,
            text=True,
            timeout=600,         # timeout de 10 minutos por script
        )
        if resultado.returncode == 0:
            log.info(f"Concluído: {nome}")
            return True
        else:
            log.error(f"Erro em {nome}:\n{resultado.stderr}")
            return False
    except subprocess.TimeoutExpired:
        log.error(f"Timeout ao rodar {nome}")
        return False
    except Exception as e:
        log.error(f"Exceção ao rodar {nome}: {e}")
        return False


def pipeline_completo() -> None:
    """Roda coleta → limpeza → análise IA → verifica alertas → notifica."""
    log.info("=" * 50)
    log.info("INICIANDO PIPELINE COMPLETO")
    log.info("=" * 50)

    inicio = datetime.now()

    # Roda os 3 scripts em sequência
    etapas = [
        "01_coletor_mercadolivre.py",
        "02_limpeza_e_banco.py",
        "03_analise_ia.py",
    ]
    for etapa in etapas:
        ok = rodar_script(etapa)
        if not ok:
            msg = f"❌ Pipeline interrompido na etapa: {etapa}"
            log.error(msg)
            enviar_telegram(msg)
            return

    # Detecta alertas comparando com coleta anterior
    alertas     = comparar_coletas()
    top_produtos = buscar_top_produtos()

    duracao = (datetime.now() - inicio).seconds // 60
    log.info(f"Pipeline concluído em {duracao} minutos. Alertas: {len(alertas)}")

    # Sempre notifica com resumo diário (mesmo sem alertas críticos)
    assunto, corpo_html = template_email_alerta(alertas, top_produtos)
    corpo_tg = template_telegram_alerta(alertas, top_produtos)
    notificar(assunto, corpo_html, corpo_tg)


def relatorio_semanal() -> None:
    """Envia relatório completo semanal com análise da IA."""
    # Só roda às segundas-feiras
    if datetime.now().weekday() != 0:
        return

    log.info("Gerando relatório semanal...")

    import glob
    arquivos_ia = sorted(glob.glob("analise_ia_*.json"), reverse=True)
    analise_ia  = None
    if arquivos_ia:
        with open(arquivos_ia[0], encoding="utf-8") as f:
            analise_ia = json.load(f)

    assunto, corpo_html = template_email_relatorio_semanal(analise_ia)
    enviar_email(assunto, corpo_html)

    if analise_ia:
        texto_tg = (
            f"📋 <b>Relatório Semanal</b>\n\n"
            + analise_ia.get("visao_geral", "")[:800]
            + "\n\n<i>Relatório completo enviado por e-mail.</i>"
        )
        enviar_telegram(texto_tg)

    log.info("Relatório semanal enviado.")


# ─── 5. Agendamento ───────────────────────────────────────────────────────────

def configurar_agenda() -> None:
    # Pipeline completo todo dia no horário definido
    schedule.every().day.at(HORARIO_COLETA_DIARIA).do(pipeline_completo)
    log.info(f"Pipeline agendado: todo dia às {HORARIO_COLETA_DIARIA}")

    # Relatório semanal (função interna checa se é segunda)
    schedule.every().monday.at(HORARIO_RELATORIO_SEMANAL).do(relatorio_semanal)
    log.info(f"Relatório semanal agendado: segundas às {HORARIO_RELATORIO_SEMANAL}")


def proxima_execucao() -> str:
    job = schedule.next_run()
    if job:
        delta = job - datetime.now()
        horas  = delta.seconds // 3600
        minutos = (delta.seconds % 3600) // 60
        return f"{horas}h {minutos}min"
    return "—"


# ─── Execução principal ───────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("  Agendador iniciado")
    log.info("=" * 50)

    configurar_agenda()

    # Notifica que o agendador subiu
    enviar_telegram(
        f"✅ <b>Agendador iniciado</b>\n"
        f"Pipeline diário: {HORARIO_COLETA_DIARIA}\n"
        f"Relatório semanal: segundas às {HORARIO_RELATORIO_SEMANAL}\n"
        f"Próxima execução em: {proxima_execucao()}"
    )

    # Opção: rodar o pipeline imediatamente na primeira vez
    rodar_agora = input("\nRodar o pipeline agora também? (s/N): ").strip().lower()
    if rodar_agora == "s":
        pipeline_completo()

    # Loop principal
    log.info("Aguardando próxima execução agendada... (Ctrl+C para parar)")
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)   # verifica a cada 30 segundos
    except KeyboardInterrupt:
        log.info("Agendador encerrado pelo usuário.")
