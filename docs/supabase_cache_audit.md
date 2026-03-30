# Supabase Cache Audit

Generated: 2026-03-28T12:00:40.155975+00:00

## Cleanup Model

- `active`: current production `sale/...` keys used by `syscara-api-python`
- `legacy`: old unprefixed keys from `api/index.py`
- `unknown`: manual review required before cleanup

## Current Base Inventory

| Base Key | Status | Stored Rows | Latest Updated At | Sample Keys |
| --- | --- | ---: | --- | --- |
| sale/ads | active | 4 | 1774682430 | sale/ads#chunk0, sale/ads#chunk1, sale/ads#chunk2, sale/ads#meta |
| sale/equipment | active | 7 | 1774682200 | sale/equipment, sale/equipment#chunk0, sale/equipment#chunk1, sale/equipment#chunk2 |
| sale/lists | active | 3 | 1774682315 | sale/lists, sale/lists#chunk0, sale/lists#meta |
| sale/orders | active | 25 | 1774682213 | sale/orders#chunk4, sale/orders#chunk5, sale/orders#chunk6, sale/orders#chunk7 |
| sale/orders_full | active | 184 | 1774683197 | sale/orders_full#chunk37, sale/orders_full#chunk38, sale/orders_full#chunk39, sale/orders_full#chunk40 |
| sale/vehicles | active | 15 | 1774682382 | sale/vehicles#chunk0, sale/vehicles#chunk1, sale/vehicles#chunk2, sale/vehicles#chunk3 |
| sale/vehicles_full | active | 70 | 1774683073 | sale/vehicles_full#chunk5, sale/vehicles_full#chunk6, sale/vehicles_full#chunk7, sale/vehicles_full#chunk8 |

## Active Payload Fingerprints

| Base Key | Payload Size | MD5 |
| --- | ---: | --- |
| sale/ads | 4872514 | 7bcdd0e67835b46615f19dda82984306 |
| sale/equipment | 190407 | b94e719a1408714637e0b45f5722ec9d |
| sale/lists | 100 | bb1a1d74b2ac229b435703895533cbf1 |
| sale/orders | 95379365 | 61cc3e346be2689060f8e6a67f4af2ed |
| sale/orders_full | 230466001 | 6475debb552f7186b0471e0256de3bac |
| sale/vehicles | 72761818 | c4fc2e02db26a8147941037d47a4fe7c |
| sale/vehicles_full | 133891405 | 45360e3b45192ed0f3b8fd10b43c38e8 |

## Duplicate Active Payloads

- No duplicate payloads detected across active `sale/...` keys.

## Cleanup Notes

- Delete only `legacy` keys after backup.
- Keep all `active` `sale/...` keys unchanged.
- Re-run this script after each cleanup step to confirm the new state.