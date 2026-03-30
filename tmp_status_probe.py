import collections
import json
from pathlib import Path

p = Path(__file__).resolve().parent / 'cache' / 'cache_vehicles.json'
with open(p, encoding='utf-8') as f:
    d = json.load(f)

items = d.values() if isinstance(d, dict) else d
status_counter = collections.Counter()
visible_counter = collections.Counter()
state_counter = collections.Counter()

n = 0
for v in items:
    if not isinstance(v, dict):
        continue
    n += 1
    status_counter[str(v.get('status'))] += 1
    state_counter[str(v.get('state'))] += 1
    props = v.get('properties') if isinstance(v.get('properties'), dict) else {}
    visible_counter[str(props.get('visible'))] += 1

print('total', n)
print('status', status_counter.most_common(20))
print('state', state_counter.most_common(20))
print('visible', visible_counter)
