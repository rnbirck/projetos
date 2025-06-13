# %%
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import importlib
import utils
import os

importlib.reload(utils)

mes = "05"
ano = "2025"

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"

# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")
diretorio_base_relativo = rf"data/preco_varejo/{ano}-{mes}"

# Dicionário de empresas por UF
empresas_por_uf = {
    "DF": ["Leroy Merlin"],
    "PR": ["Nichele", "Balaroti", "Cassol"],
    "RJ": ["Amoedo", "Chatuba", "Obramax"],
    "SP": [
        "C&C",
        "Center Castilho",
        "Center Mega",
        "Joli",
        "Leroy Merlin",
        "Obramax",
        "Sodimac",
        "Telhanorte",
        "Tumkus",
    ],
}

# Loop de conversão
for uf, lojas in empresas_por_uf.items():
    for loja in lojas:
        nome_arquivo = f"Pesq_Precos_Anfacer_V1190 {uf} - {loja}.xls"

        caminho_relativo_ao_script = os.path.join(diretorio_base_relativo, nome_arquivo)
        caminho_absoluto = os.path.abspath(caminho_relativo_ao_script)

        print(f"Processando arquivo: {caminho_absoluto}")

        if os.path.exists(caminho_absoluto):
            try:
                utils.converter_xls_para_xlsx(caminho_absoluto)
                print(f"Convertido com sucesso: {nome_arquivo}")
            except Exception as e:
                print(f"Erro na conversão de {nome_arquivo}: {e}")
                print(f"  Traceback: {e.__traceback__}")
        else:
            print(f"Arquivo não encontrado: {caminho_absoluto}")


versao = "V1190"
ufs = ["DF", "PR", "RJ", "SP"]
lojas = [
    "Leroy Merlin",
    "Balaroti",
    "Cassol",
    "Nichele",
    "Amoedo",
    "Chatuba",
    "Obramax",
    "C&C",
    "Center Castilho",
    "Center Mega",
    "Joli",
    "Telhanorte",
    "Sodimac",
    "Tumkus",
]

todos_dados = []
arquivos_lidos = 0

# Iterando por todas as combinações de parâmetros

for uf in ufs:
    for loja in lojas:
        # Monta o caminho do arquivo
        nome_arquivo_xlsx = f"Pesq_Precos_Anfacer_{versao} {uf} - {loja}.xlsx"
        caminho_arquivo = os.path.join(diretorio_base_relativo, nome_arquivo_xlsx)
        caminho_arquivo = os.path.abspath(caminho_arquivo)

        # Verifica se o arquivo existe
        if os.path.exists(caminho_arquivo):
            # Carrega os dados do arquivo
            df = utils.carregar_dados_arquivo(caminho_arquivo)
            if df is not None:
                # Ajusta as colunas
                df = utils.processar_dados(df, ano, mes, uf, loja)
                # Adiciona o DataFrame processado à lista
                todos_dados.append(df)
                arquivos_lidos += 1
        else:
            print(f"Arquivo não encontrado: {caminho_arquivo}")

# Concatena todos os DataFrames de uma vez fora do loop
df = pd.concat(todos_dados, ignore_index=True)

df = (
    df.rename(
        columns={
            "Data": "date",
            "Loja": "loja",
            "Produto": "produto",
            "Fabricante": "fabricante",
            "Formato": "formato",
            "Pr_Promo": "pr_promo",
            "Pr_Min": "pr_min",
            "Pr_Max": "pr_max",
            "Ano": "ano",
            "Mes": "mes",
            "UF": "uf",
            "UF - Loja": "uf_loja",
        }
    )
    .replace(0, np.nan)
    .assign(
        pr_medio=lambda x: x[["pr_promo", "pr_min", "pr_max"]].sum(axis=1, min_count=1)
        / x[["pr_promo", "pr_min", "pr_max"]].notna().sum(axis=1)
    )
)
df.to_sql("anfacer_precos", con=engine, if_exists="append", index=False)

print(f"Total de registros inseridos: {len(df)}")
