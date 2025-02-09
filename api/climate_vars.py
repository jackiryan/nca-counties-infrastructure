from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
import os
from pydantic import BaseModel
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    Numeric,
    select,
    and_,
)
import uvicorn

app = FastAPI()

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


@app.get("/climate-variables", response_model=List[ClimateData])
def get_climate_variables(
    county_id: Optional[int] = Query(None, description="Filter by County ID"),
    gwl: Optional[float] = Query(None, description="Filter by Global Warming Level"),
):
    """
    Get climate variables filtered by county id and GWL
    """
    stmt = select(climate_variables)

    # Build list of filters (if provided)
    filters = []
    if county_id is not None:
        filters.append(climate_variables.c.county_id == county_id)
    if gwl is not None:
        filters.append(climate_variables.c.gwl == gwl)

    if filters:
        stmt = stmt.where(and_(*filters))

    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()

    if not results:
        raise HTTPException(
            status_code=404, detail="No data found for the provided filters"
        )
    return results


if __name__ == "__main__":
    uvicorn.run("climate_vars:app", host="0.0.0.0", port=8000, reload=True)
