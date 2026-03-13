import json

import requests

r = requests.get('http://localhost:5000/api/orders', timeout=60)
arr = r.json().get('orders', [])
if not arr:
    print('NO_ORDERS')
else:
    print(json.dumps(arr[0], indent=2, ensure_ascii=False))
