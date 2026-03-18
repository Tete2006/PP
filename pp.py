import psycopg2
import mysql.connector
import pandas as pd
import re

# ======================================================
# CONFIGURAÇÕES DE CONEXÃO
# ======================================================

# Supabase PostgreSQL (SSL obrigatório)
SUPABASE_CONFIG = {
    "host": "db.lffdvyvlwwpgsbazrkkk.supabase.co",
    "database": "postres",
    "user": "postgres",
    "password": "Senac.01#fortalezaBy2026",
    "port": "5432",
    "sslmode": "require"
}

# MySQL Local
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "etl_project"
}

# ======================================================
# EXTRACT
# ======================================================
def extract():
    conn = psycopg2.connect(**SUPABASE_CONFIG)
    query = "SELECT * FROM raw_clients"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"Total de registros extraídos: {len(df)}")
    return df

# ======================================================
# FUNÇÕES AUXILIARES
# ======================================================
def is_valid_email(email):
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(pattern, str(email))

def clean_phone(phone):
    phone = re.sub(r'\D', '', str(phone))
    if len(phone) < 10:
        return None
    return phone

# ======================================================
# TRANSFORM
# ======================================================
def transform(df):
    report = {"total": len(df), "null_fields": 0, "invalid_email": 0, "invalid_phone": 0, "duplicates": 0}

    # Padronizar nomes, emails e telefones
    df["name"] = df["nome"].astype(str).str.strip().str.title()
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["phone"] = df["telefone"].astype(str).str.strip()

    # Remover campos nulos
    before = len(df)
    df = df.dropna(subset=["name", "email"])
    report["null_fields"] = before - len(df)

    # Validar emails
    valid_email_mask = df["email"].apply(is_valid_email)
    report["invalid_email"] = len(df[~valid_email_mask])
    df = df[valid_email_mask]

    # Limpar telefones
    df["phone"] = df["phone"].apply(clean_phone)
    before = len(df)
    df = df.dropna(subset=["phone"])
    report["invalid_phone"] = before - len(df)

    # Remover duplicados por email
    before = len(df)
    df = df.drop_duplicates(subset=["email"])
    report["duplicates"] = before - len(df)

    print("Transformação concluída")
    return df, report

# ======================================================
# LOAD
# ======================================================
def load(df):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    insert_query = "INSERT INTO clients (name, email, phone) VALUES (%s, %s, %s)"
    data = df[["name", "email", "phone"]].values.tolist()
    cursor.executemany(insert_query, data)
    conn.commit()
    print(f"{cursor.rowcount} registros inseridos no MySQL")
    cursor.close()
    conn.close()

# ======================================================
# RELATÓRIO
# ======================================================
def print_report(report, loaded):
    discarded = report["total"] - loaded
    print("\n===== RELATÓRIO FINAL =====")
    print(f"Total extraídos: {report['total']}")
    print(f"Total carregados: {loaded}")
    print(f"Total descartados: {discarded}")
    print("\nMotivos de descarte:")
    print(f"Campos nulos: {report['null_fields']}")
    print(f"Emails inválidos: {report['invalid_email']}")
    print(f"Telefones inválidos: {report['invalid_phone']}")
    print(f"Duplicados: {report['duplicates']}")

# ======================================================
# PIPELINE PRINCIPAL
# ======================================================
def main():
    df = extract()
    df_clean, report = transform(df)
    load(df_clean)
    print_report(report, len(df_clean))

if __name__ == "__main__":
    main()


