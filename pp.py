import psycopg2
import mysql.connector
import pandas as pd
import re


# ======================================================
# CONFIG (variáveis de ambiente)
# ======================================================
SUPABASE_CONFIG = {
    "host": 'localhost',
    "database": 'project',
    "user": 'postgres',
    "password": 'postgres',
    "port": 5432
}

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

    print(f"Total extraídos: {len(df)}")
    return df

# ======================================================
# FUNÇÕES AUXILIARES
# ======================================================
def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}$'
    return bool(re.fullmatch(pattern, str(email)))

def clean_phone(phone):
    phone = re.sub(r'\D', '', str(phone))
    if len(phone) not in [10, 11]:
        return None
    return phone

# ======================================================
# TRANSFORM
# ======================================================
def transform(df):

    report = {
        "total": len(df),
        "null_fields": 0,
        "invalid_email": 0,
        "invalid_phone": 0,
        "duplicates": 0
    }

    # Padronização
    df["name"] = df["nome"].astype(str).str.strip().str.title()
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["phone"] = df["telefone"].astype(str).str.strip()
    df["cidade"] = df["cidade"].astype(str).str.strip().str.title()

    # Remover valores inválidos tipo "None"
    before = len(df)
    df = df[(df["name"] != "") & (df["name"] != "None")]
    df = df[(df["email"] != "") & (df["email"] != "None")]
    report["null_fields"] = before - len(df)

    # Validar email
    mask_email = df["email"].apply(is_valid_email)
    report["invalid_email"] = len(df[~mask_email])
    df = df[mask_email]

    # Validar telefone
    df["phone"] = df["phone"].apply(clean_phone)
    before = len(df)
    df = df.dropna(subset=["phone"])
    report["invalid_phone"] = before - len(df)

    # Remover duplicados
    before = len(df)
    df = df.drop_duplicates(subset=["email"])
    report["duplicates"] = before - len(df)
    print()
    
    return (df, report)

# ======================================================
# LOAD
# ======================================================
def load(df):


    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO clients (nome, email, telefone, cidade, created_at)
    VALUES (%s, %s, %s, %s, NOW())
    """

    data = df[["name", "email", "phone", "cidade"]].values.tolist()

    cursor.executemany(insert_query, data)
    conn.commit()

    inserted = cursor.rowcount

    cursor.close()
    conn.close()

    print(f"{inserted} registros carregados no MySQL")

    return inserted

# ======================================================
# RELATÓRIO
# ======================================================
def print_report(report, loaded):
    discarded = report["total"] - loaded

    print("\n===== RELATÓRIO FINAL =====")
    print(f"Total extraídos: {report['total']}")
    print(f"Total carregados: {loaded}")
    print(f"Total descartados: {discarded}")
    print(f"Taxa de aproveitamento: {(loaded/report['total'])*100:.2f}%")

    print("\nMotivos de descarte:")
    print(f"Campos nulos: {report['null_fields']}")
    print(f"Emails inválidos: {report['invalid_email']}")
    print(f"Telefones inválidos: {report['invalid_phone']}")
    print(f"Duplicados: {report['duplicates']}")

# ======================================================
# MAIN
# ======================================================
def main():


    df = extract()
    df_clean, report = transform(df)
    loaded = load(df_clean)
    print_report(report, loaded)



if __name__ == "__main__":
    main()


