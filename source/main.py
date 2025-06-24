from connection_config import get_connection
from appsettings import servers,crediantials
from export_tags import fetch_tag, fetch_cimio,prepare_output
from datetime import datetime
import warnings

warnings.filterwarnings("ignore",message="pandas only supports SQLAlchemy connectable")

for gbu,sites in servers.items():
    for site,hostnames in sites.items():
        for hostname in hostnames:
            print(f'this process is currently running for {gbu}: {site} - {hostname}')
            with get_connection(hostname,crediantials["Username"],crediantials["Password"]) as conn:
                print('1/5')
                tags_df = fetch_tag(conn)
                print('2/5')
                cimio_df = fetch_cimio(conn)
                print('3/5')
                final_df = prepare_output(tags_df,cimio_df,hostname)
                print('4/5')
                outname = f"data/{gbu}/TagExport-{hostname}-{datetime.now():%Y%m%d}.csv"
                print('5/5')
                final_df.to_csv(outname,index=False,encoding="utf-8")
                print(f"{outname} exported.")
            
            
            