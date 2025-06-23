import pyodbc
import pandas as pd
from appsettings import servers,crediantials
# from source.utils import on_off_to_binary

def get_connection(host,user,pwd, gbu,site):
    CONN_STR = (
    "DRIVER={AspenTech SQLplus};"
    f"HOST={host};"
    "PORT=10014;"
    f"UID=EUA\\{user};"
    f"PWD={pwd};"
    "CHARINT=N;"
    "CHARFLOAT=N;"
    "CHARTIME=N;"
    "ansi=True"
)
    
TAG_TABLE = [
    "IP_AnalogDef", "IP_DiscreteDef", "IP_TextDef", "ip_analogdbldef",
    "KPIDef", "BatchKPIDef", "IP_ADef", "IP_DDef", "IP_QM_AnalogDef",
    "IP_QM_discrDef", "IP_QM_TextDef", "IP_SLV_ADef"
]
    
CIMIO_TABLES = [
    "IoGetDef", "IoLongTagGetDef", "IoLLTagGetDef"
]    

CIMIO_COLS = [
    "CIMIO_Definition",
    "CIMIO_Name",
    "CIMIO_IO_RECORD_PROCESSING",
    "CIM_IO_ASYNC",
    "CIMIO_IO_TIMEOUT_VALUE",
    "CIMIO_IO_FREQUENCY",
    "CIMIO_TAG_IO_DATA_TYPE",
    "CIMIO_TAG_IO_VALUE_RECORD"
]    

OUTPUT_COLS = [
    "HostName", "Name", "IP_DESCRIPTION", "DefinitionRecord", "IP_PLANT_AREA",
    "IP_ENG_UNITS", "IP_VALUE_FORMAT", "IP_DC_SIGNIFICANCE", "IP_DC_MAX_TIME_INT",
    "IP_GRAPH_MAXIMUM", "IP_GRAPH_MINIMUM", "IP_STEPPED", "IP_MESSAGE_SWITCH",
    "IP_HIGH_HIGH_LIMIT", "IP_HIGH_LIMIT", "IP_LOW_LIMIT", "IP_LOW_LOW_LIMIT",
    "IP_LIMIT_DEADBAND", "IP_TREND_VIEW_TIME", "IP_REPOSITORY", "IP_ARCHIVING",
    "IP_BF_REPOSITORY", "IP_BF_ARCHIVING", "IP_#_OF_TREND_VALUES", "IP_#_OF_BF_VALUES",
    *CIMIO_COLS, "CIMIO_ORIGINAL_Name", "IP_VALUE_TIME", "IP_VALUE"
]
