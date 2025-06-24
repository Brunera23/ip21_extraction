import sys
sys.path.append(r'source')  
from connection_config import servers  

for gbu, sites in servers.items():
    for site, hostnames in sites.items():
        for hostname in hostnames:
            print(f'this process is currently running for {gbu}: {site} - {hostname}')