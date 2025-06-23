import json

try:
    with open(
        'source/appsettings.json',
        'r',
    ) as f:
        config = json.load(f)
except Exception as error:
    print (f'Error: {str(error)}')
    print(f'Could not open/read file: appsettings.json')
    exit()

servers = config.get("Servers",{})
crediantials = config.get("Credentials",{})

print(crediantials)




# GBU = config.get('')
# USER_NAME = config.get('')
# USER_PASSWORD = config.get('Password')
# print(USER_PASSWORD)
# HOST = config.get('')
