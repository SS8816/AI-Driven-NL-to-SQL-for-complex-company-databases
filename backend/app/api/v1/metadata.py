"""
Metadata API Endpoints
Provides app metadata like country codes, schema lists, etc.
"""

from typing import Dict, List
from fastapi import APIRouter

from app.utils.country_codes import (
    COUNTRY_NAME_TO_CODE,
    COUNTRY_CODE_TO_NAME,
    format_country_dropdown_option
)

router = APIRouter(prefix="/metadata", tags=["Metadata"])


@router.get("/countries", response_model=Dict[str, str])
async def get_country_mappings():
    """
    Get country code to name mappings.

    Returns:
        Dictionary of code -> name mappings
    """
    return COUNTRY_CODE_TO_NAME


@router.get("/countries/formatted", response_model=List[Dict[str, str]])
async def get_formatted_countries():
    """
    Get formatted country list for dropdowns.

    Returns:
        List of dicts with code, name, and display format
    """
    countries = []
    for name, code in sorted(COUNTRY_NAME_TO_CODE.items()):
        countries.append({
            "code": code,
            "name": name,
            "display": format_country_dropdown_option(name, code)
        })
    return countries
