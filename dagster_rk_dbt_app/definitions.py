

from dagster_rk_dbt_app import assets

import dagster as dg

dagster_assets = dg.load_assets_from_modules([assets])

defs = dg.Definitions(assets=dagster_assets)