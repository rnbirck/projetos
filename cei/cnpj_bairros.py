# %%
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"

# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")

ano = 2025
mes = 4
mes_formatado = f"{mes:02}"
dia = 19

arquivo = range(0, 10)
caminho = f"data/cnpj/{ano}-{mes_formatado}/"

trad_mun = pd.read_sql_query(
    "SELECT id_municipio, id_municipio_rf, municipio FROM municipio", engine
)

trad_cnae = pd.read_sql_query(
    "SELECT cod_subclasse AS cnae, subclasse FROM cnae", engine
)

trad_bairro = pd.read_sql_query(
    "SELECT CEP as cep, descricao_bairro AS bairro FROM logradouro WHERE UF = 'RS'",
    engine,
)

cep_faltantes = {
    "93048266": "Feitoria",
    "93125465": "Scharlau",
    "93032236": "Jardim América",
    "93032104": "Jardim América",
    "93046809": "Campestre",
    "93035003": "Santa Teresa",
    "93145012": "Arroio da Manteiga",
    "93135014": "Campina",
    "93125082": "Scharlau",
    "93115454": "Santos Dumont",
    "93046804": "Campestre",
    "93040335": "Rio Branco",
    "93052000": "Feitoria",
    "93020080": "Padre Reus",
    "93020690": "Centro",
    "93025000": "São João Batista",
    "93040290": "São José",
    "93032000": "Rio Branco",
    "93022000": "São João Batista",
    "93042030": "Centro",
    "93025510": "São Miguel",
    "93048000": "Feitoria",
    "93032044": "Rio Branco",
    "93125140": "Scharlau",
    "93113132": "Santos Dumont",
    "93037005": "Santa Teresa",
    "93035116": "Vicentina",
    "93140506": "Arroio da Manteiga",
    "93120562": "Scharlau",
    "93048083": "Campestre",
    "93035084": "Jardim América",
    "93115446": "Santos Dumont",
    "93054072": "Feitoria",
    "93150015": "Boa Vista",
    "93032098": "Jardim América",
    "93020190": "Padre Reus",
    "93030228": "Morro do Espelho",
    "93120612": "Scharlau",
    "93140536": "Arroio da Manteiga",
    "93032097": "Jardim América",
    "93052132": "Feitoria",
    "93030135": "Morro do Espelho",
    "93020770": "Cristo Rei",
    "93110334": "Rio dos Sinos",
    "93046813": "Campestre",
    "93046816": "Campestre",
    "93150801": "Boa Vista",
    "93001959": "Centro",
    "93032386": "Jardim América",
    "93046807": "Campestre",
    "93115448": "Santos Dumont",
    "93140528": "Arroio da Manteiga",
    "93110320": "Rio dos Sinos",
    "93145458": "Arroio da Manteiga",
    "93135534": "Arroio da Manteiga",
    "93046342": "Campestre",
    "93140522": "Arroio da Manteiga",
    "93050260": "Feitoria",
    "93032284": "Jardim América",
    "93030132": "Morro do Espelho",
    "93035472": "Fazenda São Borja",
    "93120616": "Scharlau",
    "93022560": "São João Batista",
    "93040582": "Rio Branco",
    "93140526": "Arroio da Manteiga",
    "93140500": "Arroio da Manteiga",
    "93030015": "Morro do Espelho",
    "93044234": "Campestre",
    "93115600": "Santos Dumont",
    "93130290": "Campina",
    "93030004": "Morro do Espelho",
    "93115120": "Santos Dumont",
    "93113079": "Santos Dumont",
    "93032082": "Rio Branco",
    "93035507": "Fazenda São Borja",
    "93046808": "Campestre",
    "93030232": "Morro do Espelho",
    "93046812": "Campestre",
}


def arquivo_cnpj_bairro(caminho):
    """
    Função para ler os arquivos de CNPJ e retornar um DataFrame com os dados
    filtrados e renomeados.
    """
    cnpj_raw = (
        pd.read_csv(
            caminho,
            sep=";",
            header=None,
            encoding="latin1",
            usecols=[0, 5, 11, 18, 19, 20],
            engine="pyarrow",
        )
        .rename(
            columns={
                0: "cnpj",
                1: "situacao_cadastral",
                2: "cnae",
                3: "cep",
                4: "uf",
                5: "id_municipio_rf",
            }
        )
        .astype({"cnae": str})
        .query("situacao_cadastral == 2 & uf == 'RS'")
    )
    return cnpj_raw


def arquivo_mei(caminho):
    """
    Função para ler os arquivos de MEI e retornar um DataFrame com os dados
    filtrados e renomeados.
    """
    mei_raw = (
        pd.read_csv(
            caminho,
            sep=";",
            header=None,
            encoding="latin1",
            usecols=[0, 1, 4],
            engine="pyarrow",
        )
        .rename(
            columns={
                0: "cnpj",
                1: "simples",
                2: "mei",
            }
        )
        .assign(
            simples=lambda x: x.simples.map({"N": "Não", "S": "Sim"}),
            mei=lambda x: x.mei.map({"N": "Não", "S": "Sim"}),
        )
    )
    return mei_raw


def criar_dataframe_cnpj_bairro(
    cnpj_raw, trad_mun, trad_cnae, trad_bairro, mei_raw, municipio
):
    """
    Função para criar o DataFrame final com os dados de CNPJ e bairros.
    """
    cnae = ["45", "46", "47"]
    filtro_cnae = cnpj_raw.cnae.str.startswith(tuple(cnae))

    cnpj = (
        cnpj_raw[filtro_cnae]
        .astype({"cnae": str, "id_municipio_rf": str})
        .assign(cep=lambda x: x.cep.astype(int).astype(str))
        .merge(trad_mun, on="id_municipio_rf", how="left")
        .merge(trad_cnae, on="cnae", how="left")
        .query("municipio == @municipio")
        .merge(trad_bairro, on="cep", how="left")
        .merge(mei_raw, on="cnpj", how="left")
        .assign(
            bairro=lambda x: np.where(
                x.bairro.isna(), x.cep.map(cep_faltantes), x.bairro
            ),
            categoria=lambda x: np.where(
                (x["mei"] == "Sim") | (x["simples"] == "Sim"), "Simples/MEI", "CNPJ"
            ),
            cod=lambda x: x["cnae"].str[:2],
        )
        .groupby(
            ["municipio", "bairro", "cod", "cnae", "subclasse", "categoria"],
            as_index=False,
        )
        .agg(numero=("cnpj", "count"))
        .reset_index()
        .assign(
            ano=ano,
            mes=mes_formatado,
            dia=dia,
        )
        .drop(columns=["index"])
    )
    return cnpj


lista_df = []
for i in arquivo:
    cnpj_raw = arquivo_cnpj_bairro(f"{caminho}Estabelecimentos{i}")
    lista_df.append(cnpj_raw)

cnpj_raw = pd.concat(lista_df, ignore_index=True)


mei_raw = arquivo_mei(caminho + "Simples")

cnpj = criar_dataframe_cnpj_bairro(
    cnpj_raw, trad_mun, trad_cnae, trad_bairro, mei_raw, municipio="São Leopoldo"
)


cnpj.to_excel(
    "data/cnpj/cnpj_bairro_sao_leopoldo.xlsx",
    index=False,
    engine="openpyxl",
)
