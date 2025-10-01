import re


def get_companies_query(input_company_id: int | None = None) -> str:
    if input_company_id is not None:
        where_clauses = f"a.Company_ID = {input_company_id}"
    else:
        where_clauses = "1=1"
    sql_string = f"""
        select distinct a.Company_ID, 0 as status
        from dbo.CompanyTiles as a
        join dbo.company as b on a.Company_ID = b.Company_ID
        where a.Tile_ID in (select Tile_ID from dbo.Tiles where Tile_Name in ('Employees','Jobs'))
        and b.Status not in ('Delete', 'Inactive') 
        and a.Disabled=0
        and {where_clauses}
        order by 1
    """
    return str(sql_string)

