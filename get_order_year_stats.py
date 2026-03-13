from collections import Counter

import requests

r = requests.get('http://localhost:5000/api/orders', timeout=60)
orders = r.json().get('orders', [])

years = []
for o in orders:
    if not isinstance(o, dict):
        continue
    date = o.get('date')
    dstr = None
    if isinstance(date, str):
        dstr = date
    elif isinstance(date, dict):
        # prefer create/update
        for k in ('create', 'created', 'create_date', 'created_at', 'update', 'updated_at'):
            v = date.get(k)
            if isinstance(v, str):
                dstr = v
                break
        if not dstr:
            # take first string value
            for v in date.values():
                if isinstance(v, str):
                    dstr = v
                    break
    if not dstr:
        for k in ('created_at','created','create'):
            v = o.get(k)
            if isinstance(v, str):
                dstr = v
                break
    if not dstr:
        continue
    # extract year
    try:
        y = int(dstr[:4])
        years.append(y)
    except Exception:
        continue

c = Counter(years)
print('orders total:', len(orders))
print('years present:', sorted(c.items(), reverse=True))
if years:
    sample_year = max(set(years))
    print('sample year (max):', sample_year)
    print('examples:')
    for o in orders[:5]:
        print(o.get('date'))
else:
    print('no dates found')
