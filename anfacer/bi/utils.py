import numpy as np
import pandas as pd  # type: ignore
import psutil
import traceback
import win32com.client
import os


def ajuste_sh6_m2(df, unidade_nova, unidade_antiga, sh6, qtd, valor_ajuste):
    """AJUSTA OS SH6 PARA CONVERTER KG EM M2"""
    df[unidade_nova] = np.where(
        (df[unidade_antiga] == "KG") & (df["COMMODITY"] == sh6),
        df[unidade_nova].fillna(df[qtd] / valor_ajuste),
        df[unidade_nova],
    )
    return df


def apply_commodity_conversions(df):
    """Aplica todas as conversões específicas de commodities"""
    commodities = [
        (690710, 14.72),
        (690721, 20.30),
        (690722, 14.00),
        (690723, 14.00),
        (690730, 12.50),
        (690740, 14.00),
        (690790, 19.00),
        (690810, 13.51),
        (690890, 14.00),
    ]

    for sh6, factor in commodities:
        # Para UNIT1
        mask1 = (df["UNIT1"] == "KG") & (df["COMMODITY"] == sh6) & df["UNIT3"].isna()
        df.loc[mask1, "UNIT3"] = df.loc[mask1, "QTY1"] / factor

        # Para UNIT2
        mask2 = (df["UNIT2"] == "KG") & (df["COMMODITY"] == sh6) & df["UNIT3"].isna()
        df.loc[mask2, "UNIT3"] = df.loc[mask2, "QTY2"] / factor

    return df


ptn_iso2_to_iso3 = {
    "AF": "AFG",
    "AX": "ALA",
    "AL": "ALB",
    "DZ": "DZA",
    "AS": "ASM",
    "AD": "AND",
    "AO": "AGO",
    "AI": "AIA",
    "AQ": "ATA",
    "AG": "ATG",
    "AR": "ARG",
    "AM": "ARM",
    "AW": "ABW",
    "AU": "AUS",
    "AT": "AUT",
    "AZ": "AZE",
    "BS": "BHS",
    "BH": "BHR",
    "BD": "BGD",
    "BB": "BRB",
    "BY": "BLR",
    "BE": "BEL",
    "BZ": "BLZ",
    "BJ": "BEN",
    "BM": "BMU",
    "BT": "BTN",
    "BO": "BOL",
    "BQ": "BES",
    "BA": "BIH",
    "BW": "BWA",
    "BV": "BVT",
    "BR": "BRA",
    "BU": "MMR",
    "IO": "IOT",
    "BN": "BRN",
    "BG": "BGR",
    "BF": "BFA",
    "BI": "BDI",
    "CV": "CPV",
    "KH": "KHM",
    "CM": "CMR",
    "CA": "CAN",
    "KY": "CYM",
    "CF": "CAF",
    "TD": "TCD",
    "CL": "CHL",
    "CN": "CHN",
    "CX": "CXR",
    "CC": "CCK",
    "CO": "COL",
    "KM": "COM",
    "CG": "COG",
    "CD": "COD",
    "CK": "COK",
    "CR": "CRI",
    "CI": "CIV",
    "HR": "HRV",
    "CU": "CUB",
    "CW": "CUW",
    "CY": "CYP",
    "CZ": "CZE",
    "DK": "DNK",
    "DJ": "DJI",
    "DM": "DMA",
    "DO": "DOM",
    "EC": "ECU",
    "EG": "EGY",
    "SV": "SLV",
    "GQ": "GNQ",
    "ER": "ERI",
    "EE": "EST",
    "SZ": "SWZ",
    "ET": "ETH",
    "FK": "FLK",
    "FO": "FRO",
    "FJ": "FJI",
    "FI": "FIN",
    "FR": "FRA",
    "GF": "GUF",
    "PF": "PYF",
    "TF": "ATF",
    "GA": "GAB",
    "GM": "GMB",
    "GE": "GEO",
    "DE": "DEU",
    "GH": "GHA",
    "GI": "GIB",
    "GR": "GRC",
    "GL": "GRL",
    "GD": "GRD",
    "GP": "GLP",
    "GU": "GUM",
    "GT": "GTM",
    "GG": "GGY",
    "GN": "GIN",
    "GW": "GNB",
    "GY": "GUY",
    "HT": "HTI",
    "HM": "HMD",
    "VA": "VAT",
    "HN": "HND",
    "HK": "HKG",
    "HU": "HUN",
    "IS": "ISL",
    "IN": "IND",
    "ID": "IDN",
    "IR": "IRN",
    "IQ": "IRQ",
    "IE": "IRL",
    "IM": "IMN",
    "IL": "ISR",
    "IT": "ITA",
    "JM": "JAM",
    "JP": "JPN",
    "JE": "JEY",
    "JO": "JOR",
    "KZ": "KAZ",
    "KE": "KEN",
    "KI": "KIR",
    "KP": "PRK",
    "KR": "KOR",
    "KW": "KWT",
    "KG": "KGZ",
    "LA": "LAO",
    "LV": "LVA",
    "LB": "LBN",
    "LS": "LSO",
    "LR": "LBR",
    "LY": "LBY",
    "LI": "LIE",
    "LT": "LTU",
    "LU": "LUX",
    "MO": "MAC",
    "MG": "MDG",
    "MW": "MWI",
    "MY": "MYS",
    "MV": "MDV",
    "ML": "MLI",
    "MT": "MLT",
    "MH": "MHL",
    "MQ": "MTQ",
    "MR": "MRT",
    "MU": "MUS",
    "YT": "MYT",
    "MX": "MEX",
    "FM": "FSM",
    "MD": "MDA",
    "MC": "MCO",
    "MN": "MNG",
    "ME": "MNE",
    "MS": "MSR",
    "MA": "MAR",
    "MZ": "MOZ",
    "MM": "MMR",
    "NA": "NAM",
    "NR": "NRU",
    "NP": "NPL",
    "NL": "NLD",
    "NC": "NCL",
    "NZ": "NZL",
    "NI": "NIC",
    "NE": "NER",
    "NG": "NGA",
    "NU": "NIU",
    "NF": "NFK",
    "MK": "MKD",
    "MP": "MNP",
    "NO": "NOR",
    "OM": "OMN",
    "PK": "PAK",
    "PW": "PLW",
    "PS": "PSE",
    "PA": "PAN",
    "PG": "PNG",
    "PY": "PRY",
    "PE": "PER",
    "PH": "PHL",
    "PN": "PCN",
    "PL": "POL",
    "PT": "PRT",
    "PR": "PRI",
    "QA": "QAT",
    "RE": "REU",
    "RO": "ROU",
    "RU": "RUS",
    "RW": "RWA",
    "BL": "BLM",
    "SH": "SHN",
    "KN": "KNA",
    "LC": "LCA",
    "MF": "MAF",
    "PM": "SPM",
    "VC": "VCT",
    "WS": "WSM",
    "SM": "SMR",
    "ST": "STP",
    "SA": "SAU",
    "SN": "SEN",
    "RS": "SRB",
    "SC": "SYC",
    "SL": "SLE",
    "SG": "SGP",
    "SX": "SXM",
    "SK": "SVK",
    "SI": "SVN",
    "SB": "SLB",
    "SO": "SOM",
    "ZA": "ZAF",
    "GS": "SGS",
    "SS": "SSD",
    "ES": "ESP",
    "LK": "LKA",
    "SD": "SDN",
    "SR": "SUR",
    "SJ": "SJM",
    "SE": "SWE",
    "CH": "CHE",
    "SY": "SYR",
    "TW": "TWN",
    "TJ": "TJK",
    "TZ": "TZA",
    "TH": "THA",
    "TL": "TLS",
    "TG": "TGO",
    "TK": "TKL",
    "TO": "TON",
    "TT": "TTO",
    "TN": "TUN",
    "TR": "TUR",
    "TM": "TKM",
    "TC": "TCA",
    "TV": "TUV",
    "UG": "UGA",
    "UA": "UKR",
    "AE": "ARE",
    "GB": "GBR",
    "US": "USA",
    "UM": "UMI",
    "UY": "URY",
    "UZ": "UZB",
    "VU": "VUT",
    "VE": "VEN",
    "VN": "VNM",
    "VG": "VGB",
    "VI": "VIR",
    "WF": "WLF",
    "EH": "ESH",
    "YE": "YEM",
    "ZM": "ZMB",
    "XK": "XKV",
    "AN": "ANT",
    "ZW": "ZWE",
    "CS": "CSK",
    "XB": "CO3",
    "ZF": "CO9",
    "EU": "C13",
    "UN": "C39",
}

rpt_iso2_to_iso3 = {
    "AF": "AFG",
    "AX": "ALA",
    "AL": "ALB",
    "DZ": "DZA",
    "AS": "ASM",
    "AD": "AND",
    "AO": "AGO",
    "AI": "AIA",
    "AQ": "ATA",
    "AG": "ATG",
    "AR": "ARG",
    "AM": "ARM",
    "AW": "ABW",
    "AU": "AUS",
    "AT": "AUT",
    "AZ": "AZE",
    "BS": "BHS",
    "BH": "BHR",
    "BD": "BGD",
    "BB": "BRB",
    "BY": "BLR",
    "BE": "BEL",
    "BZ": "BLZ",
    "BJ": "BEN",
    "BM": "BMU",
    "BT": "BTN",
    "BO": "BOL",
    "BQ": "BES",
    "BA": "BIH",
    "BW": "BWA",
    "BV": "BVT",
    "BR": "BRA",
    "BU": "MMR",
    "IO": "IOT",
    "BN": "BRN",
    "BG": "BGR",
    "BF": "BFA",
    "BI": "BDI",
    "CV": "CPV",
    "KH": "KHM",
    "CM": "CMR",
    "CA": "CAN",
    "KY": "CYM",
    "CF": "CAF",
    "TD": "TCD",
    "CL": "CHL",
    "CN": "CHN",
    "CX": "CXR",
    "CC": "CCK",
    "CO": "COL",
    "KM": "COM",
    "CG": "COG",
    "CD": "COD",
    "CK": "COK",
    "CR": "CRI",
    "CI": "CIV",
    "HR": "HRV",
    "CU": "CUB",
    "CW": "CUW",
    "CY": "CYP",
    "CZ": "CZE",
    "DK": "DNK",
    "DJ": "DJI",
    "DM": "DMA",
    "DO": "DOM",
    "EC": "ECU",
    "EG": "EGY",
    "SV": "SLV",
    "GQ": "GNQ",
    "ER": "ERI",
    "EE": "EST",
    "SZ": "SWZ",
    "ET": "ETH",
    "FK": "FLK",
    "FO": "FRO",
    "FJ": "FJI",
    "FI": "FIN",
    "FR": "FRA",
    "GF": "GUF",
    "PF": "PYF",
    "TF": "ATF",
    "GA": "GAB",
    "GM": "GMB",
    "GE": "GEO",
    "DE": "DEU",
    "GH": "GHA",
    "GI": "GIB",
    "GR": "GRC",
    "GL": "GRL",
    "GD": "GRD",
    "GP": "GLP",
    "GU": "GUM",
    "GT": "GTM",
    "GG": "GGY",
    "GN": "GIN",
    "GW": "GNB",
    "GY": "GUY",
    "HT": "HTI",
    "HM": "HMD",
    "VA": "VAT",
    "HN": "HND",
    "HK": "HKG",
    "HU": "HUN",
    "IS": "ISL",
    "IN": "IND",
    "ID": "IDN",
    "IR": "IRN",
    "IQ": "IRQ",
    "IE": "IRL",
    "IM": "IMN",
    "IL": "ISR",
    "IT": "ITA",
    "JM": "JAM",
    "JP": "JPN",
    "JE": "JEY",
    "JO": "JOR",
    "KZ": "KAZ",
    "KE": "KEN",
    "KI": "KIR",
    "KP": "PRK",
    "KR": "KOR",
    "KW": "KWT",
    "KG": "KGZ",
    "LA": "LAO",
    "LV": "LVA",
    "LB": "LBN",
    "LS": "LSO",
    "LR": "LBR",
    "LY": "LBY",
    "LI": "LIE",
    "LT": "LTU",
    "LU": "LUX",
    "MO": "MAC",
    "MG": "MDG",
    "MW": "MWI",
    "MY": "MYS",
    "MV": "MDV",
    "ML": "MLI",
    "MT": "MLT",
    "MH": "MHL",
    "MQ": "MTQ",
    "MR": "MRT",
    "MU": "MUS",
    "YT": "MYT",
    "MX": "MEX",
    "FM": "FSM",
    "MD": "MDA",
    "MC": "MCO",
    "MN": "MNG",
    "ME": "MNE",
    "MS": "MSR",
    "MA": "MAR",
    "MZ": "MOZ",
    "MM": "MMR",
    "NA": "NAM",
    "NR": "NRU",
    "NP": "NPL",
    "NL": "NLD",
    "NC": "NCL",
    "NZ": "NZL",
    "NI": "NIC",
    "NE": "NER",
    "NG": "NGA",
    "NU": "NIU",
    "NF": "NFK",
    "MK": "MKD",
    "MP": "MNP",
    "NO": "NOR",
    "OM": "OMN",
    "PK": "PAK",
    "PW": "PLW",
    "PS": "PSE",
    "PA": "PAN",
    "PG": "PNG",
    "PY": "PRY",
    "PE": "PER",
    "PH": "PHL",
    "PN": "PCN",
    "PL": "POL",
    "PT": "PRT",
    "PR": "PRI",
    "QA": "QAT",
    "RE": "REU",
    "RO": "ROU",
    "RU": "RUS",
    "RW": "RWA",
    "BL": "BLM",
    "SH": "SHN",
    "KN": "KNA",
    "LC": "LCA",
    "MF": "MAF",
    "PM": "SPM",
    "VC": "VCT",
    "WS": "WSM",
    "SM": "SMR",
    "ST": "STP",
    "SA": "SAU",
    "SN": "SEN",
    "RS": "SRB",
    "SC": "SYC",
    "SL": "SLE",
    "SG": "SGP",
    "SX": "SXM",
    "SK": "SVK",
    "SI": "SVN",
    "SB": "SLB",
    "SO": "SOM",
    "ZA": "ZAF",
    "GS": "SGS",
    "SS": "SSD",
    "ES": "ESP",
    "LK": "LKA",
    "SD": "SDN",
    "SR": "SUR",
    "SJ": "SJM",
    "SE": "SWE",
    "CH": "CHE",
    "SY": "SYR",
    "TW": "TWN",
    "TJ": "TJK",
    "TZ": "TZA",
    "TH": "THA",
    "TL": "TLS",
    "TG": "TGO",
    "TK": "TKL",
    "TO": "TON",
    "TT": "TTO",
    "TN": "TUN",
    "TR": "TUR",
    "TM": "TKM",
    "TC": "TCA",
    "TV": "TUV",
    "UG": "UGA",
    "UA": "UKR",
    "AE": "ARE",
    "GB": "GBR",
    "US": "USA",
    "UM": "UMI",
    "UY": "URY",
    "UZ": "UZB",
    "VU": "VUT",
    "VE": "VEN",
    "VN": "VNM",
    "VG": "VGB",
    "VI": "VIR",
    "WF": "WLF",
    "EH": "ESH",
    "YE": "YEM",
    "ZM": "ZMB",
    "XK": "XKV",
    "ZW": "ZWE",
    "AUC": "AUS",
    "CZC": "CZE",
    "DEC": "DEU",
    "ESC": "ESP",
    "FIC": "FIN",
    "FRC": "FRA",
    "IEC": "IRL",
    "ITC": "ITA",
    "MTC": "MLT",
    "PLC": "POL",
    "SKC": "SVK",
    "UKS": "GBR",
}


def ajuste_tdm_sem_brasil(df_raw, lista_sh6):
    filtro_sh6 = df_raw["COMMODITY"].isin(lista_sh6)
    return (
        df_raw[filtro_sh6]
        .assign(
            UNIT1=lambda x: x["UNIT1"].replace(["M", "M3"], "M2"),
            UNIT2=lambda x: x["UNIT2"].replace(["M", "M3"], "M2"),
            FLOW=lambda x: x["FLOW"].replace({"E": "Exportação", "I": "Importação"}),
            DATE=lambda x: x["MONTH"].astype(str) + "-" + x["YEAR"].astype(str),
        )
        .assign(
            DATE=lambda x: pd.to_datetime(x["DATE"], format="%m-%Y"),
            PTN_ISO=lambda x: np.where(
                x["PARTNER"] == "Namibia", "NA", x["PTN_ISO"]
            ),  # Ajustar namíbia poios o codigo ISO está carregando como NA
            UNIT3=lambda x: np.where(x["UNIT1"] == "M2", x["QTY1"], np.nan),
        )
        .assign(
            UNIT3=lambda x: np.where(x["UNIT2"] == "M2", x["QTY2"], x["UNIT3"]),
            QTY1=lambda x: np.where(x["UNIT1"] == "T", x["QTY1"] * 1000, x["QTY1"]),
            QTY2=lambda x: np.where(x["UNIT2"] == "T", x["QTY2"] * 1000, x["QTY2"]),
            UNIT1=lambda x: x["UNIT1"].replace("T", "KG"),
            UNIT2=lambda x: x["UNIT2"].replace("T", "KG"),
        )
        .pipe(apply_commodity_conversions)
        .assign(
            PTN_ISO2=lambda x: x["PTN_ISO"].map(ptn_iso2_to_iso3),
            CTY_RPT2=lambda x: x["CTY_RPT"].map(rpt_iso2_to_iso3),
        )
        .dropna(subset=["PTN_ISO", "CTY_RPT"])
        .drop(columns=["PTN_ISO", "CTY_RPT", "REPORTER", "PARTNER", "CTY_PTN", "QTY1"])
        .rename(
            columns={
                "PTN_ISO2": "CTY_PTN",
                "CTY_RPT2": "CTY_RPT",
                "FLOW": "FLUXO",
                "COMMODITY": "SH6",
                "UNIT3": "QTY1",
            }
        )
        .query('CTY_RPT != "BRA"')
        .drop_duplicates()
    )


exp_updates_config = [
    {
        "criteria": {
            "mes": 2,
            "ano": 2018,
            "ncm": 69072200,
            "pais": 647,
            "uf": "SP",
            "kg": 7480160,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 532210, "VL_FOB": 1361389},
    },
    {
        "criteria": {
            "mes": 9,
            "ano": 2019,
            "ncm": 69072300,
            "pais": 586,
            "uf": "SC",
            "kg": 207701,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 20146},
    },
    {
        "criteria": {
            "mes": 9,
            "ano": 2020,
            "ncm": 69072300,
            "pais": 249,
            "uf": "SC",
            "kg": 140,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 2},
    },
    {
        "criteria": {
            "mes": 7,
            "ano": 2021,
            "ncm": 69072300,
            "pais": 169,
            "uf": "SC",
            "kg": 65189,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 22748},
    },
    {
        "criteria": {
            "mes": 7,
            "ano": 2021,
            "ncm": 69072300,
            "pais": 249,
            "uf": "SC",
            "kg": 2393735,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 1032513, "VL_FOB": 1146267},
    },
    {
        "criteria": {
            "mes": 8,
            "ano": 2021,
            "ncm": 69072300,
            "pais": 249,
            "uf": "SC",
            "kg": 1767101,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 119201},
    },
    {
        "criteria": {
            "mes": 10,
            "ano": 2021,
            "ncm": 69072300,
            "pais": 586,
            "uf": "SC",
            "kg": 30969,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 4102},
    },
    {
        "criteria": {
            "mes": 2,
            "ano": 2022,
            "ncm": 69072200,
            "pais": 97,
            "uf": "SC",
            "kg": 571658,
            "fluxo": "Exp",
        },
        "ajustes": {"QT_ESTAT": 43270},
    },
]

imp_updates_config = [
    {
        "criteria": {
            "mes": 2,
            "ano": 2018,
            "ncm": 69072300,
            "pais": 245,
            "uf": "PR",
            "kg": 67734,
            "fluxo": "Imp",
        },
        "ajustes": {"QT_ESTAT": 5771},
    },
    {
        "criteria": {
            "mes": 2,
            "ano": 2018,
            "ncm": 69072200,
            "pais": 160,
            "uf": "MA",
            "kg": 607829,
            "fluxo": "Imp",
        },
        "ajustes": {"QT_ESTAT": 31883},
    },
    {
        "criteria": {
            "mes": 2,
            "ano": 2018,
            "ncm": 69072200,
            "pais": 160,
            "uf": "MA",
            "kg": 607829,
            "fluxo": "Imp",
        },
        "ajustes": {"QT_ESTAT": 31883},
    },
    {
        "criteria": {
            "mes": 2,
            "ano": 2018,
            "ncm": 69072200,
            "pais": 245,
            "uf": "SC",
            "kg": 123714,
            "fluxo": "Imp",
        },
        "ajustes": {"QT_ESTAT": 13286},
    },
    {
        "criteria": {
            "mes": 3,
            "ano": 2018,
            "ncm": 69072200,
            "pais": 245,
            "uf": "SC",
            "kg": 50000,
            "fluxo": "Imp",
        },
        "ajustes": {"QT_ESTAT": 15000},
    },
]


def ajustes_comex_mask(df, mes, ano, ncm, pais, uf, kg, fluxo):
    mask = (
        (df["CO_MES"] == mes)
        & (df["CO_ANO"] == ano)
        & (df["CO_NCM"] == ncm)
        & (df["CO_PAIS"] == pais)
        & (df["SG_UF_NCM"] == uf)
        & (df["KG_LIQUIDO"] == kg)
        & (df["FLUXO"] == fluxo)
    )
    return mask


def apply_comexstat_updates(df, updates_config_list):
    for config in updates_config_list:
        mask = ajustes_comex_mask(df, **config["criteria"])
        for col, value in config["ajustes"].items():
            df.loc[mask, col] = value
    return df


def ajustes_exp_comexstat(df, exp_updates_config, lista_sh6, trad_ncm, trad_cod_pais):
    return (
        df.assign(FLUXO="Exp")
        .pipe(apply_comexstat_updates, exp_updates_config)
        .merge(trad_ncm, on="CO_NCM", how="left")
        .query(f"CO_SH6 in {lista_sh6}")
        .groupby(
            [
                "CO_ANO",
                "CO_MES",
                "CO_SH6",
                "CO_PAIS",
                "SG_UF_NCM",
                "FLUXO",
                "CO_VIA",
                "CO_URF",
            ],
            as_index=False,
        )
        .agg({"VL_FOB": "sum", "QT_ESTAT": "sum"})
        .astype({"CO_SH6": "Int64"})
        .assign(
            CO_PAIS=lambda x: x["CO_PAIS"].astype(str).str.zfill(3),
            CO_URF=lambda x: x["CO_URF"].astype(str).str.zfill(7),
            CTY_RPT="BRA",
        )
        .astype(
            {
                "CO_ANO": "category",
                "CO_MES": "category",
                "CO_PAIS": "category",
                "CO_SH6": "category",
                "CO_VIA": "category",
                "CO_URF": "category",
                "FLUXO": "category",
                "SG_UF_NCM": "category",
            }
        )
        .merge(trad_cod_pais, on="CO_PAIS", how="left")
        .drop(columns=["CO_PAIS"])
    )


def ajustes_imp_comexstat(df, imp_updates_config, lista_sh6, trad_ncm, trad_cod_pais):
    return (
        df.assign(FLUXO="Imp")
        .pipe(apply_comexstat_updates, imp_updates_config)
        .merge(trad_ncm, on="CO_NCM", how="left")
        .query(f"CO_SH6 in {lista_sh6}")
        .groupby(
            [
                "CO_ANO",
                "CO_MES",
                "CO_SH6",
                "CO_PAIS",
                "SG_UF_NCM",
                "FLUXO",
                "CO_VIA",
                "CO_URF",
            ],
            as_index=False,
        )
        .agg({"VL_FOB": "sum", "QT_ESTAT": "sum"})
        .astype({"CO_SH6": "Int64"})
        .assign(
            CO_PAIS=lambda x: x["CO_PAIS"].astype(str).str.zfill(3),
            CO_URF=lambda x: x["CO_URF"].astype(str).str.zfill(7),
            CTY_RPT="BRA",
        )
        .astype(
            {
                "CO_ANO": "category",
                "CO_MES": "category",
                "CO_PAIS": "category",
                "CO_SH6": "category",
                "CO_VIA": "category",
                "CO_URF": "category",
                "FLUXO": "category",
                "SG_UF_NCM": "category",
            }
        )
        .merge(trad_cod_pais, on="CO_PAIS", how="left")
        .drop(columns=["CO_PAIS"])
    )


def ajustes_comexstat_final(df_exp_comexstat, df_imp_comexstat):
    return (
        pd.concat([df_exp_comexstat, df_imp_comexstat], ignore_index=True)
        .assign(
            FLUXO=lambda x: x["FLUXO"]
            .replace("Exp", "Exportação")
            .replace("Imp", "Importação")
        )
        .assign(DATE=lambda x: x["CO_MES"].astype(str) + "-" + x["CO_ANO"].astype(str))
        .assign(DATE=lambda x: pd.to_datetime(x["DATE"], format="%m-%Y"))
        .rename(
            columns={
                "CO_ANO": "YEAR",
                "CO_MES": "MONTH",
                "CO_SH6": "SH6",
                "QT_ESTAT": "QTY1",
                "VL_FOB": "VALUE",
            }
        )
    )


def ajustes_comex_25(df, mes, ano, sh6, pais, uf, value):
    mask = (
        (df["MONTH"] == mes)
        & (df["YEAR"] == ano)
        & (df["SH6"] == sh6)
        & (df["CTY_PTN"] == pais)
        & (df["SG_UF_NCM"] == uf)
        & (df["VALUE"] == value)
    )
    return mask


def update_comex_25(df):
    df.loc[
        ajustes_comex_25(
            df=df, mes=1, ano=2025, sh6=690740, pais="USA", uf="SC", value=17576
        ),
        "QTY1",
    ] = 646
    df.loc[
        ajustes_comex_25(
            df=df, mes=12, ano=2024, sh6=690740, pais="USA", uf="SC", value=942
        ),
        "QTY1",
    ] = 17
    return df


def ajustes_tdm_final(df_comexstat, df_tdm_sem_brasil):
    cols_tdm = ["DATE", "SH6", "CTY_RPT", "CTY_PTN", "FLUXO", "VALUE", "QTY1"]
    return pd.concat(
        [
            df_comexstat.pipe(update_comex_25)
            .groupby(
                ["YEAR", "MONTH", "SH6", "CTY_PTN", "CTY_RPT", "FLUXO"],
                observed=False,
                as_index=False,
            )[["QTY1", "VALUE"]]
            .sum()
            .reset_index(drop=True)
            .assign(DATE=lambda x: x["MONTH"].astype(str) + "-" + x["YEAR"].astype(str))
            .assign(DATE=lambda x: pd.to_datetime(x["DATE"], format="%m-%Y"))[cols_tdm],
            df_tdm_sem_brasil[cols_tdm],
        ],
        ignore_index=True,
    ).query("VALUE != 0")


# Preco no varejo
def carregar_dados_arquivo(caminho_arquivo):
    try:
        return pd.read_excel(caminho_arquivo, sheet_name="dB", engine="openpyxl")
    except FileNotFoundError:
        print(f"File not found: {caminho_arquivo}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def processar_dados(df, ano, mes, uf, loja):
    cols = [
        "Data",
        "Loja",
        "Produto",
        "Fabricante",
        "Formato",
        "Pr_Promo",
        "Pr_Min",
        "Pr_Max",
    ]
    df = (
        df[cols]
        .assign(
            Ano=ano,
            Mes=mes,
            UF=uf,
            Loja=loja,
            **{"UF - Loja": lambda x: (x["UF"] + " - " + x["Loja"])},
        )
        .query("Pr_Promo != 0 or Pr_Min != 0 or Pr_Max != 0")
        .query("Fabricante not in [0, ' -', '-']")
        .replace(
            {
                "Produto": {
                    "Porcelanato_Esmaltado": "Porcelanato Esmaltado",
                    "Porcelanato_Tecnico": "Porcelanato Técnico",
                }
            }
        )
    )
    return df


def kill_excel_processes():
    """Força o fechamento de todos os processos do Excel"""
    for proc in psutil.process_iter(["name"]):
        if proc.info["name"] == "EXCEL.EXE":
            proc.terminate()


def converter_xls_para_xlsx(caminho_arquivo_xls):
    caminho_arquivo_xlsx = os.path.splitext(caminho_arquivo_xls)[0] + ".xlsx"

    try:
        # Forçar fechamento de processos Excel anteriores
        kill_excel_processes()

        # Iniciar Excel
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False

        # Abrir workbook
        wb = excel.Workbooks.Open(caminho_arquivo_xls)

        # Salvar como XLSX
        wb.SaveAs(caminho_arquivo_xlsx, FileFormat=51)  # 51 = xlsx

        # Fechar workbook e Excel
        wb.Close()
        excel.Quit()

        # Mais uma verificação de fechamento
        kill_excel_processes()

        print(f"Arquivo convertido com sucesso: {caminho_arquivo_xlsx}")
        return True

    except Exception:
        print(f"Erro na conversão de {caminho_arquivo_xls}:")
        print(traceback.format_exc())
        return False
