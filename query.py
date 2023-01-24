QUERY = """
with base as (
select *,
regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data
from optimism.core.fact_event_logs
where block_timestamp > '2022-12-14'
and contract_address = '0xee36eaad94d1cc1d0eccadb55c38bffb6be06c77' 
and topics[0]::string = '0x28710dfecab43d1e29e02aa56b2e1e610c0bae19135c9cf7a83a1adb6df96d85'
)
, decoded AS (
select 
block_number,
block_timestamp,
tx_hash,
origin_from_address,
origin_to_address,
event_index,
CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS creator,
CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS about,
replace(topics [3] :: STRING,'0x','') as key,
try_hex_decode_string(key::string) as decoded_key,
substr(data::string,131,(ethereum.public.udf_hex_to_int(segmented_data[1]::string) * 2)) as val,
try_hex_decode_string(val::string) as val_text
  -- segmented_data[1]::string as val_text
  from base 
  ),
-- ens lookups
user_ens as 
(
  select 
  owner,
  ens_name as name
  from 
  crosschain.core.ez_ens
  where owner in (select distinct origin_from_address from decoded)
  and ens_set = 'Y'
),
links as 
(
    SELECT 
    coalesce(user_ens.name,concat(left(origin_from_address,4),'..',right(origin_from_address,4))) as source,
    case 
        when decoded_key like '%Twitter%'
        then 'Twitter'
        when decoded_key like '%Flipside_user_scoring%'
        then 'Flipside Score'
        when decoded_key like '%onchaincorgi:bool%'
        then 'OCC'
        else decoded_key
    end as target,
    block_timestamp
    FROM decoded
    left join user_ens on decoded.origin_from_address = user_ens.owner
)
select 
source,
target
from
links
QUALIFY RANK() OVER (PARTITION BY concat(source,'-',target) ORDER BY block_timestamp) = 1
"""
