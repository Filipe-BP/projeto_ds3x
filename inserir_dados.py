import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import os

# Obtém o diretório atual onde o script está sendo executado
base_dir = os.path.dirname(os.path.realpath(__file__))

# Caminho do arquivo de credenciais
key_path = os.path.join(base_dir, 'service_account', 'service_account.json')

# Conexão com o BigQuery
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Caminhos de entrada e saída usando caminhos relativos
input_icc_path = os.path.join(base_dir, 'downloads', 'icc.xlsx')
input_icf_path = os.path.join(base_dir, 'downloads', 'icf.xlsx')
output_icc_raw_path = os.path.join(base_dir, 'processado', 'icc_raw.xlsx')
output_icf_raw_path = os.path.join(base_dir, 'processado', 'icf_raw.xlsx')
output_icc_trusted_path = os.path.join(base_dir, 'processado', 'icc_trusted.xlsx')
output_icf_trusted_path = os.path.join(base_dir, 'processado', 'icf_trusted.xlsx')
output_refined_path = os.path.join(base_dir, 'processado', 'icf_icc_refined.xlsx')

# Função para processar os arquivos raw
def processar_arquivo_raw(input_path, tipo):
    df = pd.read_excel(input_path, header=None)
    df = df.drop(index=0).drop(columns=[df.columns[1], df.columns[15]])
    df.columns = ['indice', '202312', '202401', '202402', '202403', '202404',
                  '202405', '202406', '202407', '202408', '202409', '202410',
                  '202411', '202412', 'var_202412_202411', 'var_202412_202312']
    df = df.drop(index=1).dropna(how='all').drop(index=df.index[-1])
    df['tipo'] = tipo
    return df

# Função para limpar os nomes das colunas
def limpar_nomes_colunas(df):
    df.columns = df.columns.str.replace(r'[^\w\s]', '_', regex=True)
    df.columns = df.columns.str.replace(' ', '_')
    return df

# Função para carregar os dados no BigQuery
def carregar_no_bigquery(df, tabela_nome):
    df['load_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = limpar_nomes_colunas(df)

    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].fillna(0)
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    dataset_id = "ps-eng-dados-ds3x.filipebernardop"
    table_id = f"{dataset_id}.{tabela_nome}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET
    )

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    print(f"Tabela {tabela_nome} carregada com sucesso!")

# Processar os arquivos ICC e ICF para dados raw
df_icc_raw = processar_arquivo_raw(input_icc_path, 'ICC')
df_icf_raw = processar_arquivo_raw(input_icf_path, 'ICF')

# Salvar os arquivos raw
df_icc_raw.to_excel(output_icc_raw_path, index=False)
df_icf_raw.to_excel(output_icf_raw_path, index=False)

# Carregar os dados raw no BigQuery
carregar_no_bigquery(df_icc_raw, "icc_raw")
carregar_no_bigquery(df_icf_raw, "icf_raw")

# Consultas para os dados trusted
query_icc = """
SELECT indice, ano_mes, AVG(SAFE_CAST(valor AS FLOAT64)) AS valor, var_202412_202411, var_202412_202312, tipo
FROM `ps-eng-dados-ds3x.filipebernardop.icc_raw`
UNPIVOT(valor FOR ano_mes IN (`202312`, `202401`, `202402`, `202403`, `202404`, `202405`, `202406`, `202407`, `202408`, `202409`, `202410`, `202411`, `202412`))
GROUP BY indice, var_202412_202411, var_202412_202312, tipo, ano_mes;
"""

query_icf = """
SELECT indice, ano_mes, AVG(SAFE_CAST(valor AS FLOAT64)) AS valor, var_202412_202411, var_202412_202312, tipo
FROM `ps-eng-dados-ds3x.filipebernardop.icf_raw`
UNPIVOT(valor FOR ano_mes IN (`202312`, `202401`, `202402`, `202403`, `202404`, `202405`, `202406`, `202407`, `202408`, `202409`, `202410`, `202411`, `202412`))
GROUP BY indice, var_202412_202411, var_202412_202312, tipo, ano_mes;
"""

query_refined = """
SELECT * FROM `ps-eng-dados-ds3x.filipebernardop.icc_trusted`
UNION ALL
SELECT * FROM `ps-eng-dados-ds3x.filipebernardop.icf_trusted`
"""

# Função para obter dados via query do BigQuery
def obter_dados(query):
    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

# Obter dados para trusted
df_icc_trusted = obter_dados(query_icc)
df_icf_trusted = obter_dados(query_icf)

# Adicionar load_timestamp
load_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_icc_trusted['load_timestamp'] = load_timestamp
df_icf_trusted['load_timestamp'] = load_timestamp

# Converter a coluna 'ano_mes' para tipo numérico (int64)
df_icc_trusted['ano_mes'] = pd.to_numeric(df_icc_trusted['ano_mes'], errors='coerce')
df_icf_trusted['ano_mes'] = pd.to_numeric(df_icf_trusted['ano_mes'], errors='coerce')

# Carregar os dados nas tabelas trusted do BigQuery
carregar_no_bigquery(df_icc_trusted, "icc_trusted")
carregar_no_bigquery(df_icf_trusted, "icf_trusted")

# Obter os dados combinados de icc_trusted e icf_trusted para refined
df_icc_icf_refined = obter_dados(query_refined)

# Carregar os dados na tabela refined do BigQuery
carregar_no_bigquery(df_icc_icf_refined, "icf_icc_refined")

# Salvar os dados processados em Excel
df_icc_trusted.to_excel(output_icc_trusted_path, index=False)
df_icf_trusted.to_excel(output_icf_trusted_path, index=False)
df_icc_icf_refined.to_excel(output_refined_path, index=False)

print(f'Arquivo ICC Trusted salvo em: {output_icc_trusted_path}')
print(f'Arquivo ICF Trusted salvo em: {output_icf_trusted_path}')
print(f'Arquivo ICF ICC Refined salvo em: {output_refined_path}')
print('A inserção dos dados no BigQuery foi realizada com sucesso!')
