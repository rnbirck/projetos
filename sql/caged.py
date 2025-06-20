# %%
import os
import py7zr
import pandas as pd
from ftplib import FTP
from sqlalchemy import create_engine

usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")

ano = 2025
mes = 4

breaks = [10, 15, 18, 25, 30, 40, 50, 65, float("inf")]
labels = ["1", "2", "3", "4", "5", "6", "7", "8"]

dict_uf = {
    11: "RO",
    12: "AC",
    13: "AM",
    14: "RR",
    15: "PA",
    16: "AP",
    17: "TO",
    21: "MA",
    22: "PI",
    23: "CE",
    24: "RN",
    25: "PB",
    26: "PE",
    27: "AL",
    28: "SE",
    29: "BA",
    31: "MG",
    32: "ES",
    33: "RJ",
    35: "SP",
    41: "PR",
    42: "SC",
    43: "RS",
    50: "MS",
    51: "MT",
    52: "GO",
    53: "DF",
}

dict_movimentacao = {
    33: "0",
    99: "0",
    40: "0",
    60: "0",
    20: "1",
    35: "1",
    70: "1",
    98: "0",
    10: "1",
    50: "0",
    80: "0",
    45: "0",
    32: "0",
    31: "0",
    43: "0",
    25: "1",
    90: "0",
    97: "1",
}

cols_rename = {
    "município": "id_municipio_6",
    "uf": "sigla_uf",
    "graudeinstrução": "grau_instrucao",
    "raçacor": "raca_cor",
    "subclasse": "cnae_2_subclasse",
    "saldomovimentação": "saldo_movimentacao",
    "tipomovimentação": "tipo_movimentacao",
    "salário": "massa_salarial",
}

trad_mun = pd.read_sql_query(
    "SELECT id_municipio, id_municipio_6 FROM municipio", engine
)


def leitura_arquivos(tipo):
    list_df = []
    base_path = os.path.join(os.path.dirname(__file__), "data", "caged")
    for ano in anos:
        for mes in meses:
            # Formatar o mês com dois dígitos
            mes_formatado = str(mes).zfill(2)

            columns = [
                "competênciamov",
                "município",
                "uf",
                "subclasse",
                "graudeinstrução",
                "sexo",
                "raçacor",
                "idade",
                "saldomovimentação",
                "tipomovimentação",
                "salário",
            ]
            arquivo = f"CAGED{tipo}{ano}{mes_formatado}.txt"
            pasta = f"{ano}{mes_formatado}"
            caminho_arquivo = os.path.join(base_path, pasta, arquivo)
            # Identifica os arquivos lidos
            # Lê o arquivo CSV usando o delimitador ";"

            try:
                if not os.path.exists(caminho_arquivo):
                    print(f"Arquivo não encontrado: {caminho_arquivo}")
                    continue
                df = pd.read_csv(
                    caminho_arquivo,
                    sep=";",
                    engine="pyarrow",
                    usecols=columns,
                    decimal=",",
                )
                list_df.append(df)
                print(f"arquivo lido: {arquivo}")
            except Exception as e:
                print(f"Erro ao processar {arquivo}: {str(e)}")
                continue

    df = pd.concat(list_df, ignore_index=True)
    return df


def ajuste_caged_prefeituras(df, trad_mun):
    return (
        df.rename(columns=cols_rename)
        .query("sigla_uf == 43")
        .assign(
            idade=lambda x: x["idade"].astype("Int8"),
            sigla_uf=lambda x: x["sigla_uf"].map(dict_uf),
            faixa_etaria=lambda x: pd.cut(
                x["idade"], bins=breaks, labels=labels, right=False
            )
            .astype("string")
            .fillna("99"),
            sexo=lambda x: x["sexo"].astype("str").replace({"3": "2", "1": "1"}),
            id_municipio_6=lambda x: x["id_municipio_6"].astype(str),
            ano=lambda x: x["competênciamov"].astype(str).str[:4].astype(int),
            mes=lambda x: x["competênciamov"].astype(str).str[4:].astype(int),
            cnae_2_subclasse=lambda x: x["cnae_2_subclasse"].astype(str).str.zfill(7),
            grau_instrucao=lambda x: x["grau_instrucao"].astype(str).str.zfill(2),
            raca_cor=lambda x: x["raca_cor"].astype(str),
        )
        .groupby(
            [
                "ano",
                "mes",
                "id_municipio_6",
                "sigla_uf",
                "grau_instrucao",
                "sexo",
                "raca_cor",
                "faixa_etaria",
                "cnae_2_subclasse",
            ],
            observed=True,
        )
        .agg({"saldo_movimentacao": "sum"})
        .reset_index()
        .sort_values("saldo_movimentacao", ascending=False)
        .merge(trad_mun, on="id_municipio_6", how="left")
        .drop(columns=["id_municipio_6"])
    )


def ajuste_caged_cnae(df, trad_mun):
    return (
        df.rename(columns=cols_rename)
        .assign(
            sigla_uf=lambda x: x["sigla_uf"].map(dict_uf),
            id_municipio_6=lambda x: x["id_municipio_6"].astype(str),
            ano=lambda x: x["competênciamov"].astype(str).str[:4].astype(int),
            mes=lambda x: x["competênciamov"].astype(str).str[4:].astype(int),
            cnae_2_subclasse=lambda x: x["cnae_2_subclasse"].astype(str).str.zfill(7),
            tipo_movimentacao=lambda x: x["tipo_movimentacao"].map(dict_movimentacao),
            massa_salarial=lambda x: x["massa_salarial"].astype(float),
        )
        .groupby(
            [
                "ano",
                "mes",
                "id_municipio_6",
                "sigla_uf",
                "tipo_movimentacao",
                "cnae_2_subclasse",
            ],
            observed=True,
        )
        .agg({"massa_salarial": "sum", "saldo_movimentacao": "sum"})
        .reset_index()
        .sort_values("saldo_movimentacao", ascending=False)
        .merge(trad_mun, on="id_municipio_6", how="left")
        .drop(columns=["id_municipio_6"])
    )


script_dir = os.path.dirname(os.path.abspath(__file__))
diretorio_base = os.path.join(script_dir, "data", "caged")
os.makedirs(diretorio_base, exist_ok=True)
anos = range(ano, ano + 1)
meses = range(mes, mes + 1)
tipos = ["FOR", "MOV", "EXC"]

for ano in anos:
    for mes in meses:
        for tipo in tipos:
            mes_str = str(mes).zfill(2)
            arquivo_7z = (
                f"CAGED{tipo}{ano}{mes_str}.7z"  # definindo o nome do arquivo .7z
            )
            caminho_7z = os.path.join(
                diretorio_base, arquivo_7z
            )  # definindo o caminho do arquivo .7z
            try:
                ftp = FTP("ftp.mtps.gov.br")
                ftp.login()
                ftp.cwd(f"/pdet/microdados/NOVO CAGED/{ano}/{ano}{mes_str}/")
                with open(caminho_7z, "wb") as f:
                    ftp.retrbinary(f"RETR {arquivo_7z}", f.write)
                ftp.quit()
                print(f"Arquivo {arquivo_7z} baixado com sucesso.")
                # criar o subdiretorio para extracao
                diretorio_extracao = os.path.join(diretorio_base, f"{ano}{mes_str}")
                os.makedirs(diretorio_extracao, exist_ok=True)
                # extrair o arquivo .7z
                with py7zr.SevenZipFile(caminho_7z, mode="r") as z:
                    z.extractall(path=diretorio_extracao)
                # listar arquivos extraidos
                arquivos_extraidos = os.listdir(diretorio_extracao)
                print(
                    f"Arquivos extraídos para {diretorio_extracao}: {arquivos_extraidos}"
                )
            except Exception as e:
                print(f"Erro ao processar {arquivo_7z}: {e}")


df_mov = leitura_arquivos("MOV")
df_for = leitura_arquivos("FOR")
df_exc = leitura_arquivos("EXC")

df_exc = df_exc.assign(saldomovimentação=lambda x: -x["saldomovimentação"])
df = pd.concat([df_mov, df_for, df_exc], ignore_index=True)

# Salvar no banco de dados
df_caged_prefeituras = ajuste_caged_prefeituras(df, trad_mun).to_sql(
    "caged_prefeituras", con=engine, if_exists="append", index=False, chunksize=1000
)
df_caged_cnae = ajuste_caged_cnae(df, trad_mun).to_sql(
    "caged_cnae", con=engine, if_exists="append", index=False, chunksize=1000
)
