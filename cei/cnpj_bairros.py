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
mes = 5
mes_formatado = f"{mes:02}"
dia = 19

arquivo = range(0, 10)
caminho = f"../data/cnpj/{ano}-{mes_formatado}/"

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

map_situacao = {1: "NULA", 2: "ATIVA", 3: "SUSPENSA", 4: "INAPTA", 8: "BAIXADA"}

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
    "93046802": "Campestre",
    "93125646": "Scharlau",
    "93140520": "Arroio da Manteiga",
    "93115192": "Santos Dumont",
    "93120617": "Scharlau",
    "93135069": "Arroio da Manteiga",
    "93135063": "Arroio da Manteiga",
    "93135396": "Arroio da Manteiga",
    "93044805": "Fazenda São Borja",
    "93140504": "Arroio da Manteiga",
    "93035494": "Fazenda São Borja",
    "93150382": "Boa Vista",
    "93048032": "Campestre",
    "93046803": "Campestre",
    "93140530": "93140530",
    "93125704": "Scharlau",
    "93140508": "Arroio da Manteiga",
    "93025517": "Vicentina",
    "93032024": "Rio Branco",
    "93046818": "Campestre",
    "93113130": "Santos Dumont",
    "93113136": "Santos Dumont",
    "93140524": "Arroio da Manteiga",
    "93110336": "Rio dos Sinos",
    "93113134": "Santos Dumont",
    "93113128": "Santos Dumont",
    "93125642": "Scharlau",
    "93140532": "Arroio da Manteiga",
    "93032554": "Fazenda São Borja",
    "93046806": "Campestre",
    "93125647": "Scharlau",
    "93140534": "Arroio da Manteiga",
    "93140540": "Arroio da Manteiga",
    "93135730": "Arroio da Manteiga",
    "93046814": "Campestre",
    "93046333": "Campestre",
    "93032312": "Jardim América",
    "93020418": "Cristo Rei",
    "93035410": "Jardim América",
    "93032014": "Rio Branco",
    "93020152": "Padre Reus",
    "93035018": "Santa Teresa",
    "93046334": "Campestre",
    "93050410": "Feitoria",
    "93042280": "Pinheiro",
    "93046422": "Campestre",
    "93035254": "Jardim América",
    "93046332": "Campestre",
    "93110332": "Rio dos Sinos",
    "93050230": "Feitoria",
    "93032086": "Rio Branco",
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
            usecols=[0, 5, 6, 10, 11, 18, 19, 20],
            engine="pyarrow",
        )
        .rename(
            columns={
                0: "cnpj",
                1: "situacao_cadastral",
                2: "data_situacao_cadastral",
                3: "data_inicio_atividade",
                4: "cnae",
                5: "cep",
                6: "uf",
                7: "id_municipio_rf",
            }
        )
        .astype({"cnae": str})
        .query("uf == 'RS'")
        .assign(
            situacao_cadastral=lambda x: x.situacao_cadastral.map(map_situacao),
            data_inicio_atividade=lambda x: pd.to_datetime(
                x.data_inicio_atividade, format="%Y%m%d", errors="coerce"
            ),
            data_situacao_cadastral=lambda x: pd.to_datetime(
                x.data_situacao_cadastral, format="%Y%m%d", errors="coerce"
            ),
        )
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


# %%
def calcular_empresas_ativas_mensal(
    df_entrada, trad_municipios_df, ano_str, mes_str, municipio
):
    """
    Calcula o número de empresas ativas mensalmente por município e subclasse.

    Args:
        df_entrada (pd.DataFrame): DataFrame com os dados das empresas.
                                   Deve conter as colunas: 'data_inicio_atividade',
                                   'data_situacao_cadastral', 'situacao_cadastral',
                                   'id_municipio_rf', 'cod_subclasse'.
        trad_municipios_df (pd.DataFrame): DataFrame para tradução de 'id_municipio_rf'
                                           para 'id_municipio'. Deve conter as colunas:
                                           'id_municipio_rf', 'id_municipio'.
        ano_str (str): Ano final para o período de análise.
        mes_str (str): Mês final para o período de análise.
    Returns:
        pd.DataFrame: DataFrame com a contagem de empresas ativas por período,
                      município e subclasse.
    """
    # Criar um DataFrame com todos os meses desde janeiro de 2023 até o presente
    data_inicio = pd.Timestamp("2023-01-31")
    # Ajuste para garantir que a data_fim seja o último dia do mês fornecido
    data_fim_calc = pd.Timestamp(f"{ano_str}-{mes_str}-01") + pd.offsets.MonthEnd(0)
    meses_ref = pd.date_range(start=data_inicio, end=data_fim_calc, freq="ME")

    # Certificar que as colunas de data estão no formato datetime
    df = df_entrada.copy()  # Trabalhar com uma cópia para evitar SettingWithCopyWarning
    df["data_inicio_atividade"] = pd.to_datetime(
        df["data_inicio_atividade"], errors="coerce"
    )
    df["data_situacao_cadastral"] = pd.to_datetime(
        df["data_situacao_cadastral"], errors="coerce"
    )

    resultados_calc = []

    for data_ref in meses_ref:
        periodo = f"{data_ref.year}_{data_ref.month:02d}"

        mask_inicio = df["data_inicio_atividade"] <= data_ref
        mask_ativas_situacao = (df["situacao_cadastral"] == "ATIVA") & mask_inicio
        mask_outras_situacao = (
            (df["situacao_cadastral"] != "ATIVA")
            & mask_inicio
            & (
                df["data_situacao_cadastral"].isnull()
                | (df["data_situacao_cadastral"] >= data_ref)
            )  # Considera nulos como ainda não baixados/suspensos/inaptos *antes* da data_ref
        )
        mask_empresas_ativas_periodo = mask_ativas_situacao | mask_outras_situacao

        contagem = (
            df[mask_empresas_ativas_periodo]
            .groupby(["id_municipio_rf", "cnae", "cep", "categoria"])
            .size()
            .reset_index(name="empresas_ativas")  # Nomear a coluna diretamente
        )
        # contagem.columns = ["id_municipio_rf", "cnae", "empresas_ativas"] # Linha anterior, alternativa acima
        contagem["periodo"] = periodo
        contagem["ano_mes"] = f"{data_ref.year}-{data_ref.month:02d}"
        contagem["ano"] = data_ref.year
        contagem["mes"] = data_ref.month

        resultados_calc.append(contagem)

    resultado_final_mensal = pd.concat(resultados_calc, ignore_index=True)

    resultado_final_mensal = (
        resultado_final_mensal.sort_values(
            ["id_municipio_rf", "cnae", "cep", "periodo"]
        )
        .astype({"id_municipio_rf": str})
        .merge(trad_municipios_df, how="left", on="id_municipio_rf")
        .drop(columns=["id_municipio_rf"])
        .query("municipio == @municipio")
        .loc[
            :,
            [
                "id_municipio",
                "municipio",
                "cnae",
                "empresas_ativas",
                "periodo",
                "ano_mes",
                "ano",
                "mes",
                "cep",
                "categoria",
            ],
        ]
    )
    return resultado_final_mensal


# %%
lista_df = []
for i in arquivo:
    cnpj_raw = arquivo_cnpj_bairro(f"{caminho}Estabelecimentos{i}.csv")
    lista_df.append(cnpj_raw)

cnpj_raw = pd.concat(lista_df, ignore_index=True)
mei_raw = arquivo_mei(caminho + "Simples.csv")
# %%
cnpj = (
    cnpj_raw.merge(mei_raw, on="cnpj", how="left")
    .assign(
        categoria=lambda x: np.where(
            (x["mei"] == "Sim") | (x["simples"] == "Sim"), "Simples/MEI", "CNPJ"
        ),
    )
    .drop(columns=["simples", "mei"])
)
# %%
resultado_mensal_cnpj = calcular_empresas_ativas_mensal(
    cnpj, trad_mun, "2025", "04", "São Leopoldo"
)

# %%
cnae = ["45", "46", "47"]
cnpj_ativos_bairro = (
    resultado_mensal_cnpj.assign(cep=lambda x: x.cep.astype(int).astype(str))
    .merge(trad_cnae, on="cnae", how="left")
    .merge(trad_bairro, on="cep", how="left")
    .assign(
        bairro=lambda x: np.where(x.bairro.isna(), x.cep.map(cep_faltantes), x.bairro),
        cod=lambda x: x["cnae"].str[:2],
    )
    .query("cod in @cnae")
    .groupby(
        [
            "periodo",
            "ano_mes",
            "ano",
            "mes",
            "municipio",
            "bairro",
            "cod",
            "cnae",
            "subclasse",
            "categoria",
        ],
        as_index=False,
    )
    .agg(numero=("empresas_ativas", "sum"))
    .reset_index(drop=True)
)
# %%
cnpj_ativos_bairro.to_excel(
    "resultados/cnpj_bairro_sao_leopoldo.xlsx",
    index=False,
    engine="openpyxl",
)
