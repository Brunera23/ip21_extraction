from connection_config import get_connection
from appsettings import servers,crediantials
from export_tags import fetch_tag, fetch_cimio,prepare_output
from datetime import datetime

for gbu,sites in servers.items():
    # print (f"Gbu: {gbu}")
    for site,hostnames in sites.items():
        # print(f"Site: {site}")
        for hostname in hostnames:
            # print(f"Host: {hostname}")

            print(f'this process is currently running for {gbu}: {site} - {hostname}')
            get_connection(hostname,crediantials["Username"],crediantials["Password"],gbu,site)
            tags_df = fetch_tag()
            # if tags_df.empty:
            #     print("Nenhuma tabela de tags exportada.")
            #     return
            cimio_df = fetch_cimio()
            final_df = prepare_output(tags_df,cimio_df,hostname)
            outname = f"{gbu}\TagExport-{hostname}-{datetime.now():%Y%m%d}.csv"
            final_df.to_csv(outname,index=False,encoding="utf-8")
            print(f"{outname} exported.")
            
            
            