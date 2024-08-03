from google.cloud import bigquery, storage
import os
from pandas import DataFrame

# Configure a variável de ambiente para as credenciais
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'
] = 'auth/api-conta-azul-424818-36485ce7e1c1.json'

# Substitua pelo seu ID do projeto
project_id = 'api-conta-azul-424818'
# Substitua pelo seu ID do dataset
dataset_id = 'db_conta_azul'
# Nome do bucket no Google Cloud Storage
bucket_name = 'cloud_logs_v1'
# Nome do arquivo CSV a ser salvo no bucket
csv_filename = 'tabela_atualizacoes.csv'

# Lista de tabelas para verificar
table_ids = [
    'f_fluxo_caixa',
    'd_categoria_pagar',
    'd_centro',
    'd_banco',
    'd_categoria_receber',
    'f_extrato',
    'f_receber',
    'f_pagar',
]

# Cria um cliente do BigQuery
client = bigquery.Client(project=project_id)

# Função para obter a data de última modificação
def get_table_last_modified(dataset_id, table_id):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return table.modified


# Coleta os dados de última modificação para cada tabela
rows_to_insert = []
for table_id in table_ids:
    last_modified = get_table_last_modified(dataset_id, table_id)
    rows_to_insert.append(
        {
            'table_name': table_id,
            'last_modified': last_modified.isoformat(),  # Converte datetime para string no formato ISO 8601
        }
    )
    print(f'Tabela: {table_id}, Última Modificação: {last_modified}')

# Cria o DataFrame a partir dos dados coletados
df = DataFrame(rows_to_insert)

# Salva o DataFrame em um arquivo CSV
df.to_csv(csv_filename, index=False)
print(f"Arquivo CSV '{csv_filename}' criado com sucesso.")

# Cria um cliente do Cloud Storage
storage_client = storage.Client(project=project_id)

# Faz o upload do arquivo CSV para o bucket
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(csv_filename)
blob.upload_from_filename(csv_filename)
print(
    f"Arquivo CSV '{csv_filename}' carregado com sucesso no bucket '{bucket_name}'."
)
