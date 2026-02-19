import os
import io
import datetime as dt

import pyodbc
import pandas as pd
import matplotlib.pyplot as plt

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


import base64
import requests
import msal

import schedule
import time

from dotenv import load_dotenv
import sys

# Detecta se está rodando como .exe (PyInstaller)
if getattr(sys, 'frozen', False):
    base_path = os.path.dirname(sys.executable)
else:
    base_path = os.path.dirname(os.path.abspath(__file__))

env_path = os.path.join(base_path, ".env")

if os.path.exists(env_path):
    load_dotenv(env_path)
    print(f".env carregado de: {env_path}")
else:
    print(f".env NÃO encontrado em: {env_path}")

# =========================
# CONFIG (via variáveis de ambiente)
# =========================
DB_CONN_STR = os.environ.get("DB_CONN_STR")  # string completa do pyodbc
SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", SMTP_USER)
EMAIL_TO = os.environ.get("EMAIL_TO")  # separado por vírgula
EMAIL_SUBJECT = os.environ.get("EMAIL_SUBJECT", "Pacotes por meio de movimento (Diário)")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "14"))  # janela do gráfico


AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET")
GRAPH_SENDER_UPN = os.environ.get("GRAPH_SENDER_UPN")  # ex: relatorios@empresa.com

def _get_graph_token() -> str:
    missing = [k for k, v in {
        "AZURE_TENANT_ID": AZURE_TENANT_ID,
        "AZURE_CLIENT_ID": AZURE_CLIENT_ID,
        "AZURE_CLIENT_SECRET": AZURE_CLIENT_SECRET,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Variáveis ausentes para Graph: {', '.join(missing)}")

    authority = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        AZURE_CLIENT_ID,
        authority=authority,
        client_credential=AZURE_CLIENT_SECRET,
    )

    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Falha ao obter token Graph: {result.get('error')} - {result.get('error_description')}")
    return result["access_token"]


def send_email_graph_inline_image(file_path: str, pivot: pd.DataFrame, start_date: dt.date):
    if not GRAPH_SENDER_UPN:
        raise RuntimeError("GRAPH_SENDER_UPN não definido (ex: relatorios@empresa.com).")
    if not EMAIL_TO:
        raise RuntimeError("EMAIL_TO não definido.")
    if not os.path.exists(file_path):
        raise RuntimeError(f"Imagem não encontrada: {file_path}")

    token = _get_graph_token()

    to_list = [x.strip() for x in EMAIL_TO.split(",") if x.strip()]
    to_recipients = [{"emailAddress": {"address": addr}} for addr in to_list]

    # Resumo (opcional)
    total_janela = int(pivot.sum().sum())
    ultimo_dia = pivot.index.max()
    ult_vals = pivot.loc[ultimo_dia]

    resumo_html = f"""
    <ul>
      <li><b>Data mais recente:</b> {pd.to_datetime(ultimo_dia).strftime('%d/%m/%Y')}</li>
      <li><b>Computador:</b> {int(ult_vals.get('Computador', 0))}</li>
      <li><b>Coletor:</b> {int(ult_vals.get('Coletor', 0))}</li>
    </ul>
    """

    # HTML com imagem inline por CID
    content_id = "chart1"
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <p>Relatório Diário – Metros Cúbicos Transferidos (Com Coletor vs. Sem Coletor).</p>
        {resumo_html}
        <p><b>Gráfico:</b></p>
        <img src="cid:{content_id}" style="max-width: 100%; height: auto; border: 1px solid #ddd;" />
        <p style="color:#666; font-size: 12px;">
          Período: {start_date.strftime('%d/%m/%Y')} até {dt.date.today().strftime('%d/%m/%Y')}<br/>
          Gerado em {dt.datetime.now().strftime('%d/%m/%Y %H:%M')}
        </p>
      </body>
    </html>
    """

    with open(file_path, "rb") as f:
        img_b64 = base64.b64encode(f.read()).decode("utf-8")

    # Monta payload Graph (sendMail)
    payload = {
        "message": {
            "subject": EMAIL_SUBJECT,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": to_recipients,
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": os.path.basename(file_path),
                    "contentType": "image/png",
                    "contentBytes": img_b64,
                    "isInline": True,
                    "contentId": content_id
                }
            ]
        },
        "saveToSentItems": True
    }

    url = f"https://graph.microsoft.com/v1.0/users/{GRAPH_SENDER_UPN}/sendMail"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code not in (202, 200):
        raise RuntimeError(f"Erro ao enviar e-mail via Graph: HTTP {resp.status_code} - {resp.text}")

    print(f"E-mail enviado via Graph para: {', '.join(to_list)}")

SQL = """
SELECT
  CAST(DTRECEB AS date) AS DataMovimento,
  CASE WHEN HISREAL.USUARIO <> 'ANILDO' THEN 'Computador' ELSE 'Coletor' END AS MeioMovimento,
  cast(sum(qtreceb) as integer) AS NroMovimento
FROM HISREAL
INNER JOIN estoque e
  ON e.codigo = hisreal.codigo
 AND e.CATEGORIA = '99'
 AND e.FAMILIA ='15'
WHERE DTRECEB >= ?
  AND HISREAL.EMPRESA_RECNO = 1
  AND forma = 'P'
GROUP BY
  CAST(DTRECEB AS date),
  CASE WHEN HISREAL.USUARIO <> 'ANILDO' THEN 'Computador' ELSE 'Coletor' END
ORDER BY
  CAST(DTRECEB AS date) ASC;
"""

def fetch_data():
    if not DB_CONN_STR:
        raise RuntimeError("DB_CONN_STR não definido nas variáveis de ambiente.")

    start_date = (dt.date.today() - dt.timedelta(days=DAYS_BACK))
    with pyodbc.connect(DB_CONN_STR, timeout=30) as conn:
        df = pd.read_sql(SQL, conn, params=[start_date])

    df["DataMovimento"] = pd.to_datetime(df["DataMovimento"])
    df["MeioMovimento"] = df["MeioMovimento"].astype(str)
    df["NroMovimento"] = pd.to_numeric(df["NroMovimento"], errors="coerce").fillna(0).astype(int)
    return df, start_date

def build_and_save_chart(df, start_date):
    import numpy as np

    pivot = df.pivot_table(
        index="DataMovimento",
        columns="MeioMovimento",
        values="NroMovimento",
        aggfunc="sum",
        fill_value=0
    ).sort_index()

    # Garantir colunas
    for col in ["Computador", "Coletor"]:
        if col not in pivot.columns:
            pivot[col] = 0

    # 🔹 Criar range completo de datas
    end_date = pd.to_datetime(dt.date.today())
    all_dates = pd.date_range(start=start_date, end=end_date)

    pivot = pivot.reindex(all_dates, fill_value=0)
    pivot.index.name = "DataMovimento"

    # 🔹 Plot
    plt.figure(figsize=(14, 6))

    x = pivot.index

    plt.plot(x, pivot["Computador"], marker="o")
    plt.plot(x, pivot["Coletor"], marker="o")

    # 🔹 Exibir valor em cada ponto
    for i, v in enumerate(pivot["Computador"]):
        plt.text(x[i], v, str(v), fontsize=10, ha='center', va='bottom')

    for i, v in enumerate(pivot["Coletor"]):
        plt.text(x[i], v, str(v), fontsize=10, ha='center', va='bottom')

    plt.title(f"Metros Cúbicos Transferidos (Com Coletor vs. Sem Coletor) (últimos {DAYS_BACK} dias)")
    plt.xlabel("Data")
    plt.ylabel("Quantidade em metros cúbicos")

    # 🔹 Forçar todas as datas no eixo X
    plt.xticks(x, [d.strftime("%d/%m") for d in x], rotation=45)

    plt.grid(True, linestyle="--", linewidth=0.5)
    plt.legend(["Computador", "Coletor"])
    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(OUTPUT_DIR, f"pacotes_meio_movimento_{stamp}.png")

    plt.savefig(file_path, format="png", dpi=150)
    plt.close()

    return file_path, pivot


def main():
    df, start_date = fetch_data()
    file_path, pivot = build_and_save_chart(df, start_date)
    print("Imagem do gráfico salva em:", file_path)
    # Depois vamos usar file_path para enviar no corpo do e-mail
    # Envio por Microsoft Graph (Azure service account)
    send_email_graph_inline_image(file_path, pivot, start_date)

def job():
    try:
        main()
    except Exception as e:
        print("Erro na execução:", e)

if __name__ == "__main__":
    main()
    schedule.every().day.at("03:00").do(job)

    print("Serviço iniciado. Aguardando 03:00...")

    while True:
        schedule.run_pending()
        time.sleep(60)    