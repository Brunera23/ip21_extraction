import pyodbc
import pandas as pd
import re 
from connection_config import TAG_TABLE,CIMIO_TABLES
from additional_resource import time_todecaseconds, time_toseconds, on_off_to_binary
import warnings

warnings.filterwarnings("ignore",message="pandas only supports SQLAlchemy connectable")

def get_connection():
    
    conn = pyodbc.connect(CONN_STR)
    conn.setdeconding(pyodbc.SQL_CHAR,enconding='cp1252')
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding='cp1252')
    conn.setdecoding(encoding='uft-8')
    return conn

def fetch_tag():
    dfs = []
    with get_connection() as conn:
        for table in TAG_TABLE:
            try: 
                cols = pd.read_sql(f"SELECT * FROM {table} WHERE 1=0", conn).columns
                cols_query = ", ".join([
                    f'SUBSTRING(NAME FROM 1 FOR 200) AS NAME' if c.upper() == "NAME" else f'"{c}'
                    for c in cols
                ])
                query = f"SELECT {cols_query} FROM {table}"
                df = pd.read_sql(query, conn)
                df["DefinitionRecord"] = table
                dfs.append(df)
                print(f'{table}: {len(df)} lines exported.')
            except Exception as e:
                print(f'{table}: not exported | {e}')
    return pd.concat(dfs,ignore_index=True) if dfs else pd.DataFrame

def fetch_cimio():
    dfs = []
    with get_connection() as conn:
        cursor = conn.cursor()
        for table in CIMIO_TABLES:
            try:
                cols = [row.column_name.upper() for row in cursor.columns(table=table)]
                fields = ["NAME","OCCNUM","IO_TAGNAME","IO_RECORD_PROCESSING","IO_ASYNC?","IO_TIMEOUT_VALUE","IO_FREQUENCY","IO_DATA_TYPE",'"IO_VALUE_RECORD&&FLD']
                
                query = f"SELECT {', '.join(fields)} FROM {table}"
                df = pd.read_sql(query,conn)
                # TODO: Check this part (I haven't review the code below and just excluded it from the original script)
                # if "IO_RECORD_PROCESSING" in df.columns:
                #     df["IO_RECORD_PROCESSING"] = df["IO_RECORD_PROCESSING"].apply(on_off_to_binary)
                df["SOURCE_TABLE"] = table
                dfs.append(df)
                print(f"{table}: {len(df)} lines exported.")
        
            except Exception as e:
                print(f"{table}: there is no data here.")
    return pd.concat(dfs,ignore_index=True) if dfs else pd.DataFrame()

def search_for_cimio(tagname:str,cimio_df: pd.DataFrame) -> pd.Series:
    if cimio_df.empty or 'IO_VALUE_RECORD&FLD' not in cimio_df.columns:
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
            "CIMIO_TAG_IO_VALUE_RECORD":row.get("IO_VALUE_RECORD&&FLD", ""),
        })
    return pd.Series({col:"" for col in CIMIO_COLS})

def prepare_output(tags_df, cimio_df, hostname):
    tags_df["CIMIO_ORIGINAL_Name"] = tags_df["NAME"].astype(str)
    tags_df[CIMIO_COLS] = tags_df["CIMIO_ORIGINAL_Name"].apply(lambda x: search_for_cimio(x,cimio_df))
    tags_df["HostName"] = hostname
    tags_df["Name"] = tags_df.get("NAME", tags_df.get("CIMIO_ORIGINAL_Name",""))
    tags_df["IP_DC_MAX_TIME_INT"] = tags_df["IP_DC_MAX_TIME_INT"].apply(time_todecaseconds)
    tags_df["IP_TREND_VIEW_TIME"] = tags_df["IP_TREND_VIEW_TIME"].apply(time_toseconds)
    tags_df["IP_ARCHIVING"] = tags_df["IP_ARCHIVING"].apply(on_off_to_binary)
    tags_df["IP_BF_ARCHIVING"] = tags_df["IP_BF_ARCHIVING"].apply(on_off_to_binary)
    
    return tags_df[OUTPUT_COLS]
    