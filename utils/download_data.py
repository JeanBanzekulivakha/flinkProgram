import requests

csv_url = 'https://dl.lsdupm.ovh/CLOUD/2122/sample-traffic-3xways.csv'
req = requests.get(csv_url)
url_content = req.content
csv_file = open('./src/main/resources/sample-traffic-3xways.csv', 'wb')

csv_file.write(url_content)
csv_file.close()
