Pipeline ETL com PostgreSQL → MySQL

Este projeto implementa um pipeline ETL (Extract, Transform, Load) em Python, responsável por extrair dados de um banco PostgreSQL, realizar limpeza e transformação, e carregar os dados tratados em um banco MySQL.

Tecnologias utilizadas

Python 3

psycopg2 (PostgreSQL)

mysql-connector-python (MySQL)

pandas

re (regex)

Estrutura do Pipeline

O pipeline é dividido em 3 etapas principais:

1. Extract (Extração)

Conecta ao PostgreSQL

Extrai os dados da tabela raw_clients

Retorna um DataFrame com os dados brutos

Query utilizada:
SELECT * FROM raw_clients

2. Transform (Transformação)

Responsável por limpar e padronizar os dados.

Padronização:

Nome → formato Title Case

Email → minúsculo

Cidade → formato Title Case

Telefone → remoção de caracteres não numéricos

Remoções:

Campos nulos ou vazios

Emails inválidos

Telefones inválidos (diferentes de 10 ou 11 dígitos)

Registros duplicados (baseado em email)

Métricas coletadas:

Total de registros

Campos nulos

Emails inválidos

Telefones inválidos

Duplicados

Regras de Validação

Email:
Validação feita com regex:
^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+.[a-z]{2,}$

Telefone:

Remove tudo que não for número

Aceita apenas números com 10 ou 11 dígitos

3. Load (Carga)

Conecta ao MySQL

Insere os dados tratados na tabela clients

Query utilizada:
INSERT INTO clients (nome, email, telefone, cidade, created_at)
VALUES (%s, %s, %s, %s, NOW())

Estrutura dos Bancos

PostgreSQL (origem):

CREATE TABLE raw_clients (
id SERIAL PRIMARY KEY,
nome TEXT,
email TEXT,
telefone TEXT,
cidade TEXT,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

MySQL (destino):

CREATE TABLE clients (
id INT AUTO_INCREMENT PRIMARY KEY,
name VARCHAR(150),
email VARCHAR(150) UNIQUE,
phone VARCHAR(20)
);

Geração de Dados de Teste

O projeto inclui um script SQL que gera dados aleatórios com inconsistências, simulando dados reais.

Problemas simulados:

Nomes com espaços extras

Variação de maiúsculas e minúsculas

Valores nulos

Emails inválidos

Telefones incorretos

Cidades variadas

Como executar

Instale as dependências:
pip install psycopg2-binary mysql-connector-python pandas

Configure os bancos no código:

SUPABASE_CONFIG

MYSQL_CONFIG

Execute o script:
python etl.py

Saída esperada

Durante a execução, o pipeline exibe:

Quantidade de dados extraídos

Registros removidos por tipo de erro

Total carregado no MySQL

Exemplo de relatório final:

===== RELATÓRIO FINAL =====
Total extraídos: 200
Total carregados: 120
Total descartados: 80
Taxa de aproveitamento: 60.00%

Motivos de descarte:
Campos nulos: 20
Emails inválidos: 25
Telefones inválidos: 15
Duplicados: 20
