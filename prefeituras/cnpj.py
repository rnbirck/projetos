# %%
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
from tqdm import tqdm
import zipfile
import utils

ano = 2025
mes = 4
mes_formatado = f"{mes:02}"

BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
URL = f"{BASE_URL}{ano}-{mes_formatado}/"
DOWNLOAD_DIR = "data/cnpj/"
directory_name = f"{ano}-{mes_formatado}/"
directory_path = os.path.join(DOWNLOAD_DIR, directory_name)

os.makedirs(directory_path, exist_ok=True)


def extrair_e_renomear_zip(caminho_zip, diretorio_destino):
    """
    Extrai o conteúdo de um ZIP e tenta renomear o PRIMEIRO item listado
    para o nome base do ZIP. Assume que tudo vai dar certo.
    """
    print(f"Processando: {os.path.basename(caminho_zip)}")
    nome_base_zip = os.path.splitext(os.path.basename(caminho_zip))[0]
    novo_caminho_desejado = os.path.join(diretorio_destino, nome_base_zip)

    try:
        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
            # Pega o nome do PRIMEIRO item da lista interna do ZIP
            nome_original_interno = zip_ref.namelist()[0]

            # Extrai tudo
            print("  Extraindo...")
            zip_ref.extractall(diretorio_destino)

        # Constrói o caminho original do item (baseado no primeiro nome da lista)
        caminho_original_extraido = os.path.join(
            diretorio_destino, nome_original_interno.strip("/")
        )

        # Renomeia
        print(
            f"  Renomeando '{nome_original_interno.strip('/')}' para '{nome_base_zip}'..."
        )
        os.rename(caminho_original_extraido, novo_caminho_desejado)
        print("  Renomeado.")

    except Exception as e:
        # Captura qualquer erro (ZIP não encontrado, vazio, corrompido, erro ao renomear, etc.)
        print(f"  ERRO ao processar {os.path.basename(caminho_zip)}: {e}")


arquivo = range(0, 10)
for i in arquivo:
    nome_arquivo = f"Estabelecimentos{i}.zip"
    if os.path.exists(directory_path):
        print(f"Extraindo {nome_arquivo}...")
        extrair_e_renomear_zip(directory_path, directory_path)
        print(f"{nome_arquivo} extraído com sucesso.")
    else:
        print(f"Arquivo {nome_arquivo} não encontrado.")
# %%
# MEI

nome_arquivo_mei = "Simples.zip"
caminho_arquivo_mei = os.path.join(directory_path, nome_arquivo_mei)
if os.path.exists(caminho_arquivo_mei):
    print(f"Extraindo {nome_arquivo_mei}...")
    extrair_e_renomear_zip(caminho_arquivo_mei, directory_path)
    print(f"{nome_arquivo_mei} extraído com sucesso.")
