import pyodbc
import pandas as pd
import re 
from connection_config import TAG_TABLE,CIMIO_TABLES, CIMIO_COLS, OUTPUT_COLS
from additional_resource import time_todecaseconds, time_toseconds, on_off_to_binary
from appsettings import servers,crediantials

import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
warnings.filterwarnings("ignore", message="DataFrame is highly fragmented")
warnings.filterwarnings("ignore", message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated")


def get_connection(host,user,pwd,charint):
    CONN_STR = (
    "DRIVER={AspenTech SQLplus};"
    f"HOST={host};"
    "PORT=10014;"
    f"UID=EUA\\{user};"
    f"PWD={pwd};"
    f"CHARINT={charint};"
    "CHARFLOAT=N;"
    "CHARTIME=N;"
    "ansi=True"
)   
    conn = pyodbc.connect(CONN_STR)
    conn.setdecoding(pyodbc.SQL_CHAR,encoding='cp1252')
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding='cp1252')
    conn.setencoding(encoding='utf-8')
    return conn


def fetch_tag(host,user,pwd):
    dfs = []

    # 1. Pegue schemas de todas as tabelas de uma vez só (usar só uma conexão pra schema)
    with get_connection(host, user, pwd, charint='N') as conn_schema:
        table_schemas = {}
        for table in TAG_TABLE:
            try:
                cols = pd.read_sql(f"SELECT * FROM {table} WHERE 1=0", conn_schema).columns
                table_schemas[table] = cols
            except Exception as e:
                print(f'{table}: not exported')

    # 2. Reaproveite conexões para cada tipo (evite abrir/fechar a cada tabela)
    with get_connection(host, user, pwd, charint='N') as conn_n, \
         get_connection(host, user, pwd, charint='Y') as conn_y:

        for table in TAG_TABLE:
            try:
                cols = table_schemas.get(table, [])
                if not len(cols):
                    print(f'{table}: not found, skipping')
                    continue

                cols_charint_y = [c for c in cols if c.upper() in ("IP_PLANT_AREA", "IP_ENG_UNITS", "IP_MESSAGE_SWITCH")]
                cols_charint_n = [c for c in cols if c.upper() not in ("IP_PLANT_AREA", "IP_ENG_UNITS", "IP_MESSAGE_SWITCH")]

                # CHARINT=N (sempre traz NAME)
                if cols_charint_n:
                    cols_query_n = ", ".join([
                        'SUBSTRING(NAME FROM 1 FOR 200) AS NAME' if c.upper() == "NAME" else f'"{c}"'
                        for c in cols_charint_n
                    ])
                    query_n = f"SELECT {cols_query_n} FROM {table}"
                    df_n = pd.read_sql(query_n, conn_n)
                else:
                    df_n = pd.DataFrame()

                # CHARINT=Y (traz só NAME e as colunas especiais)
                if cols_charint_y:
                    cols_query_y = ", ".join(['"NAME"'] + [f'"{c}"' for c in cols_charint_y if c.upper() != "NAME"])
                    query_y = f"SELECT {cols_query_y} FROM {table}"
                    df_y = pd.read_sql(query_y, conn_y)
                else:
                    df_y = pd.DataFrame()

                # Merge pelos campos
                if not df_n.empty and not df_y.empty:
                    df = pd.merge(df_n, df_y, on='NAME', how='left')
                elif not df_n.empty:
                    df = df_n
                elif not df_y.empty:
                    df = df_y
                else:
                    df = pd.DataFrame()

                if not df.empty:
                    df["DefinitionRecord"] = table
                    dfs.append(df)
                    print(f'{table}: {len(df)} lines exported.')
                else:
                    print(f'{table}: not exported')
            except Exception as e:
                print(f'{table}: not exported ({e})')

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()








# def fetch_tag(conn):
#     dfs = []
#     # with get_connection() as conn:
#     for table in TAG_TABLE:
#         try: 
#             cols = pd.read_sql(f"SELECT * FROM {table} WHERE 1=0", conn).columns
#             cols_query = ", ".join([
#                 f'SUBSTRING(NAME FROM 1 FOR 200) AS NAME' if c.upper() == "NAME" else f'"{c}"'
#                 for c in cols
#             ])
#             query = f"SELECT {cols_query} FROM {table}"
#             df = pd.read_sql(query, conn)
#             df["DefinitionRecord"] = table
#             dfs.append(df)
#             print(f'{table}: {len(df)} lines exported.')
#         except Exception as e:
#             print(f'{table}: not exported')
#     return pd.concat(dfs,ignore_index=True) if dfs else pd.DataFrame

def fetch_cimio(conn):
    dfs = []
    cursor = conn.cursor()
    for table in CIMIO_TABLES:
        try:
            cols = [row.column_name.upper() for row in cursor.columns(table=table)]
            fields = ["NAME","OCCNUM","IO_TAGNAME","IO_RECORD_PROCESSING",'"IO_ASYNC?"',"IO_TIMEOUT_VALUE","IO_FREQUENCY","IO_DATA_TYPE",'"IO_VALUE_RECORD&&FLD"']
            
            query = f"SELECT {', '.join(fields)} FROM {table}"
            df = pd.read_sql(query,conn)
            # TODO: Check this part (I haven't review the code below and just excluded it from the original script)
            if "IO_RECORD_PROCESSING" in df.columns:
                df["IO_RECORD_PROCESSING"] = df["IO_RECORD_PROCESSING"].apply(on_off_to_binary)
            df["SOURCE_TABLE"] = table
            dfs.append(df)
            print(f"{table}: {len(df)} lines exported.")
    
        except Exception as e:
            print(f"{table}: there is no data here.")
    return pd.concat(dfs,ignore_index=True) if dfs else pd.DataFrame()

def search_for_cimio(tagname:str,cimio_df: pd.DataFrame) -> pd.Series:
    
    if cimio_df.empty or "IO_VALUE_RECORD&FLD" not in cimio_df.columns:
        print('columns: ' + cimio_df.columns)
        return pd.Series({col: "" for col in CIMIO_COLS})
    
    tagname_norm = str(tagname).strip().upper() + " "
    cimio_norm = cimio_df["IO_VALUE_RECORD&FLD"].fillna("").apply(lambda x: str(x).strip().upper())
    filtro = cimio_norm.str.startswith(tagname_norm)
    match = cimio_df.loc[filtro]
    
    if not match.empty:
        row = match.iloc[0]
        return pd.Series({
            "CIMIO_Definition": row.get("SOURCE_TABLE", ""),
            "CIMIO_Name":row.get("NAME", ""),
            "CIMIO_IO_RECORD_PROCESSING":row.get("IO_RECORD_PROCESSING", ""),
            "CIM_IO_ASYNC":row.get("IO_ASYNC?", ""),
            "CIMIO_IO_TIMEOUT_VALUE":row.get("IO_TIMEOUT_VALUE", ""),
            "CIMIO_IO_FREQUENCY":row.get("IO_FREQUENCY", ""),
            "CIMIO_TAG_IO_DATA_TYPE":row.get("IO_DATA_TYPE", ""),
            "CIMIO_TAG_IO_VALUE_RECORD":row.get("IO_VALUE_RECORD&FLD", ""),
        })
    return pd.Series({col:"" for col in CIMIO_COLS})

def prepare_output(tags_df, cimio_df, hostname):
    tags_df["CIMIO_ORIGINAL_Name"] = tags_df["NAME"].astype(str)
    tags_df[CIMIO_COLS] = tags_df["CIMIO_ORIGINAL_Name"].apply(lambda x: search_for_cimio(x,cimio_df))
    tags_df["HostName"] = hostname
    tags_df["Name"] = tags_df.get("NAME", tags_df.get("CIMIO_ORIGINAL_Name",""))
    # tags_df["IP_DC_MAX_TIME_INT"] = tags_df["IP_DC_MAX_TIME_INT"]
    tags_df["IP_TREND_VIEW_TIME"] = tags_df["IP_TREND_VIEW_TIME"].apply(time_toseconds)
    tags_df["IP_ARCHIVING"] = tags_df["IP_ARCHIVING"].apply(on_off_to_binary)
    tags_df["IP_BF_ARCHIVING"] = tags_df["IP_BF_ARCHIVING"].apply(on_off_to_binary)
    
    return tags_df[OUTPUT_COLS]
    