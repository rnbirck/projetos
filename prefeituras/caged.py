# %%
from sqlalchemy import create_engine
import pandas as pd
import time

caminho_destino = "C:/Users/rnbirck/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Emprego e Renda/"
start_time = time.time()
# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"

# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")

query_caged = """
    SELECT 
        a.ano, 
        a.mes, 
        a.id_municipio, 
        b.municipio, 
        a.sigla_uf, 
        a.grau_instrucao, 
        a.faixa_etaria, 
        a.raca_cor,
        a.sexo, 
        a.cnae_2_subclasse, 
        a.saldo_movimentacao 
    FROM caged_prefeituras a
    JOIN municipio b 
        ON a.id_municipio = b.id_municipio"""

chunk_size = 100000  # Ajuste conforme necessário
chunks = []

with engine.connect() as conn:
    for chunk in pd.read_sql(
        query_caged,
        con=conn,
        chunksize=chunk_size,
    ):
        chunks.append(chunk)

df_caged = pd.concat(chunks, ignore_index=True).to_csv(
    caminho_destino + "base_caged.csv", index=False
)

end_time = time.time()
print("Arquivo de caged mensal salvo")
print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
