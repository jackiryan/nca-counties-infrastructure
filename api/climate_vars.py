from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
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
    select,
    and_,
)
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (use specific domains in production)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL", "")

engine = create_engine(DATABASE_URL)
metadata = MetaData()

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

VALID_COLUMNS = {col.name for col in climate_variables.columns}


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
    county_name: Optional[str] = None
    county_fips: Optional[str] = None
    state_abbr: Optional[str] = None


@app.get("/climate-variables")
def get_climate_variables(
    county_id: Optional[int] = Query(None, description="Filter by County ID"),
    gwl: Optional[float] = Query(None, description="Filter by Global Warming Level"),
    var: Optional[str] = Query(
        None,
        description="If provided and valid, return only this column (plus required columns)",
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

    # If a valid var is provided, select only a subset of columns.
    if var and var in VALID_COLUMNS:
        columns_to_select = [
            climate_variables.c.id,
            climate_variables.c.county_id,
            climate_variables.c.gwl,
            getattr(climate_variables.c, var),
        ]
        stmt = select(*columns_to_select)
    else:
        stmt = select(climate_variables)

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
        ClimateDataSubset = create_model(
            "ClimateDataSubset",
            id=(int, ...),
            county_id=(int, ...),
            gwl=(float, ...),
            **{var: (Optional[float], None)}
        )
        # Convert each row into a dictionary and then into the dynamic model.
        response = [ClimateDataSubset(**dict(row._mapping)) for row in results]
        return response
    else:
        # Return the full data using the full model.
        response = [ClimateData(**dict(row._mapping)) for row in results]
        return response


if __name__ == "__main__":
    uvicorn.run("climate_vars:app", host="0.0.0.0", port=8000, reload=True)
