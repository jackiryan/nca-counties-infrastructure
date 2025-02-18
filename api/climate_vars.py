from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from pydantic import BaseModel, create_model
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    Float,
    Numeric,
    String,
    select,
    and_,
)
from typing import Optional
import uvicorn

app = FastAPI()

dev = False
if dev:
    origins = ["*"]
else:
    origins = ["https://jackiepi.xyz", "https://www.jackiepi.xyz"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Authorization", "Content-Type"],
)

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL", "")
MAPTILER_API_KEY = os.getenv("MAPTILER_API_KEY", "")

engine = create_engine(DATABASE_URL, echo=True)
metadata = MetaData()

counties = Table(
    "counties",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("fips", String),
    Column("state_abbr", String),
)

climate_variables = Table(
    "climate_variables",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("county_id", Integer),
    Column("gwl", Numeric(2, 1)),
    Column("pr_above_nonzero_99th", Float),
    Column("prmax1day", Float),
    Column("prmax5yr", Float),
    Column("tavg", Float),
    Column("tmax1day", Float),
    Column("tmax_days_ge_100f", Float),
    Column("tmax_days_ge_105f", Float),
    Column("tmax_days_ge_95f", Float),
    Column("tmean_jja", Float),
    Column("tmin_days_ge_70f", Float),
    Column("tmin_days_le_0f", Float),
    Column("tmin_days_le_32f", Float),
    Column("tmin_jja", Float),
    Column("pr_annual", Float),
    Column("pr_days_above_nonzero_99th", Float),
)

climate_normals = Table(
    "climate_normals",
    metadata,
    Column("county_fips", String, primary_key=True),
    Column("tavg", Float),
    Column("tmax_days_ge_100f", Float),
    Column("tmean_jja", Float),
    Column("tmin_days_ge_70f", Float),
    Column("tmin_days_le_0f", Float),
    Column("tmin_days_le_32f", Float),
    Column("tmin_jja", Float),
    Column("pr_annual", Float),
)

VALID_COLUMNS = {col.name for col in climate_variables.columns}
VALID_NORMALS = {
    col.name for col in climate_normals.columns if col.name != "county_fips"
}


class ClimateData(BaseModel):
    id: int
    county_id: int
    gwl: float
    pr_above_nonzero_99th: Optional[float] = None
    prmax1day: Optional[float] = None
    prmax5yr: Optional[float] = None
    tavg: Optional[float] = None
    tmax1day: Optional[float] = None
    tmax_days_ge_100f: Optional[float] = None
    tmax_days_ge_105f: Optional[float] = None
    tmax_days_ge_95f: Optional[float] = None
    tmean_jja: Optional[float] = None
    tmin_days_ge_70f: Optional[float] = None
    tmin_days_le_0f: Optional[float] = None
    tmin_days_le_32f: Optional[float] = None
    tmin_jja: Optional[float] = None
    pr_annual: Optional[float] = None
    pr_days_above_nonzero_99th: Optional[float] = None
    name: Optional[str] = None
    fips: Optional[str] = None
    state_abbr: Optional[str] = None


@app.get("/base_tiles/{z}/{x}/{y}.pbf")
async def get_base_tile(z: int, x: int, y: int):
    """Reverse proxy for frontend to avoid leaking API key."""
    tile_url = (
        f"https://api.maptiler.com/tiles/v3-lite/{z}/{x}/{y}.pbf?key={MAPTILER_API_KEY}"
    )

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(tile_url)
            if response.status_code == 200:
                return Response(
                    content=response.content, media_type="application/x-protobuf"
                )
            else:
                raise HTTPException(
                    status_code=response.status_code, detail="Error fetching tile"
                )
        except httpx.RequestError:
            raise HTTPException(status_code=500, detail="Server error fetching tile")


@app.get("/climate-variables")
def get_climate_variables(
    county_id: Optional[int] = Query(None, description="Filter by County ID"),
    gwl: Optional[float] = Query(None, description="Filter by Global Warming Level"),
    var: Optional[str] = Query(
        None,
        description="If provided and valid, return only this column (plus required columns)",
    ),
    relative: bool = Query(
        True,
        description="Display values as change from 1991-2020 normals, as opposed to absolute metrics. Default is True (values are deltas)",
    ),
):
    """
    Retrieve climate variables data.

    - If `var` is provided and is one of the valid columns, the response will only include
      `id`, `county_id`, `gwl`, and the selected column.
    - Otherwise, all columns are returned.
    """
    # Build filters for the query
    filters = []
    if county_id is not None:
        filters.append(climate_variables.c.county_id == county_id)
    if gwl is not None:
        filters.append(climate_variables.c.gwl == gwl)

    # Build the join clause
    if relative:
        join_clause = climate_variables.join(
            counties, climate_variables.c.county_id == counties.c.id
        )
    else:
        join_clause = climate_variables.join(
            counties, climate_variables.c.county_id == counties.c.id
        ).join(climate_normals, counties.c.fips == climate_normals.c.county_fips)

    # If a valid var is provided, select only a subset of columns.
    if var and var in VALID_COLUMNS:
        if not relative and var in VALID_NORMALS:
            if var == "pr_annual":
                # pr_annual: normals (in inches) adjusted by percent change
                var_column = (
                    climate_normals.c.pr_annual
                    * (1 + (climate_variables.c.pr_annual / 100.0))
                ).label("pr_annual")
            else:
                var_column = (
                    getattr(climate_variables.c, var) + getattr(climate_normals.c, var)
                ).label(var)
        else:
            var_column = getattr(climate_variables.c, var)
        columns_to_select = [
            climate_variables.c.id,
            climate_variables.c.county_id,
            climate_variables.c.gwl,
            var_column,
            counties.c.name,
            counties.c.fips,
            counties.c.state_abbr,
        ]
    else:
        full_columns = []
        for col in climate_variables.columns:
            if not relative and col.name in VALID_NORMALS:
                if col.name == "pr_annual":
                    # Calculate pr_annual in inches using the normal and the percent change.
                    full_columns.append(
                        (
                            climate_normals.c.pr_annual
                            * (1 + (climate_variables.c.pr_annual / 100.0))
                        ).label("pr_annual")
                    )
                else:
                    full_columns.append(
                        (col + getattr(climate_normals.c, col.name)).label(col.name)
                    )
            else:
                full_columns.append(col)
        columns_to_select = full_columns + [
            counties.c.name,
            counties.c.fips,
            counties.c.state_abbr,
        ]

    stmt = select(*columns_to_select).select_from(join_clause)

    if filters:
        stmt = stmt.where(and_(*filters))

    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()

    if not results:
        raise HTTPException(
            status_code=404, detail="No data found for the provided filters"
        )

    # When var is provided, dynamically create a subset response model.
    if var and var in VALID_COLUMNS:
        # Create a model with required fields and the additional column named as `var`
        model_fields = {
            "id": (int, ...),
            "county_id": (int, ...),
            "gwl": (float, ...),
            var: (Optional[float], None),
            "name": (Optional[str], None),
            "fips": (Optional[str], None),
            "state_abbr": (Optional[str], None),
        }
        ClimateDataSubset = create_model("ClimateDataSubset", **model_fields)
        # Convert each row into a dictionary and then into the dynamic model.
        response = [ClimateDataSubset(**dict(row._mapping)) for row in results]
        return response
    else:
        # Return the full data using the full model.
        response = [ClimateData(**dict(row._mapping)) for row in results]
        return response


@app.get("/climate-normals")
def get_climate_normals(
    county_fips: Optional[str] = Query(None, description="Filter by County FIPS"),
    var: Optional[str] = Query(
        None,
        description="If provided and valid, return only this column (plus required columns)",
    ),
):
    """
    Retrieve climate normals data.

    - If `var` is provided and is one of the valid columns, the response will only include
      `id`, `county_id`, `gwl`, and the selected column.
    - Otherwise, all columns are returned.
    """
    # Build filters for the query
    filters = []
    if county_fips is not None:
        filters.append(climate_normals.c.county_fips == county_fips)

    # If a valid var is provided, select only a subset of columns.
    if var and var in VALID_NORMALS:
        columns_to_select = [
            climate_normals.c.county_fips,
            getattr(climate_normals.c, var),
        ]
        stmt = select(*columns_to_select)
    else:
        stmt = select(climate_normals)

    if filters:
        stmt = stmt.where(and_(*filters))

    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()

    if not results:
        raise HTTPException(
            status_code=404, detail="No data found for the provided filters"
        )

    # When var is provided, dynamically create a subset response model.
    if var and var in VALID_COLUMNS:
        # Create a model with required fields and the additional column named as `var`
        ClimateNormalSubset = create_model(
            "ClimateNormalSubset",
            county_fips=(str, ...),
            **{var: (Optional[float], None)},
        )
        # Convert each row into a dictionary and then into the dynamic model.
        response = [ClimateNormalSubset(**dict(row._mapping)) for row in results]
        return response
    else:
        # Return the full data using the full model.
        response = [ClimateData(**dict(row._mapping)) for row in results]
        return response


if __name__ == "__main__":
    uvicorn.run("climate_vars:app", host="0.0.0.0", port=8000, reload=True)
