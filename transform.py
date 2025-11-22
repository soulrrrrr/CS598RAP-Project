import re

def wrap_with_iceberg_cte(sql_query, base_path='data/iceberg/tpcds'):
    """
    Instead of directly replacing text within the Query, generate a CTE (WITH ... AS ...) header.
    This resolves issues where `iceberg_scan` cannot be easily used in the SELECT list or FROM clauses directly.
    
    Args:
        sql_query (str): The original SQL query.
        base_path (str): The base path to the Iceberg data (e.g., 'data/iceberg/tpcds').
    """
    
    # The 24 TPC-DS tables (25 including dbgen_version)
    tpcds_tables = [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales", 
        "customer", "customer_address", "customer_demographics", "date_dim", 
        "household_demographics", "income_band", "inventory", "item", 
        "promotion", "reason", "ship_mode", "store", "store_returns", 
        "store_sales", "time_dim", "warehouse", "web_page", "web_returns", 
        "web_sales", "web_site", "dbgen_version"
    ]

    # 1. Scan the query to identify which tables are used
    used_tables = set()
    for table in tpcds_tables:
        # Use regex to ensure whole word matching (avoids matching 'item' inside 'lineitem')
        if re.search(r"\b" + table + r"\b", sql_query, re.IGNORECASE):
            used_tables.add(table)

    if not used_tables:
        return sql_query

    # 2. Construct the CTE definition block
    # Format: table_name AS (SELECT * FROM iceberg_scan('path/table_name', ...))
    cte_definitions = []
    for table in used_tables:
        # Use the passed base_path here
        cte_sql = f"{table} AS (SELECT * FROM iceberg_scan('{base_path}/{table}', allow_moved_paths = true))"
        cte_definitions.append(cte_sql)

    # 3. Assemble the final SQL
    # A simple approach is used here: check which tables are needed and prepend the CTEs.
    
    cte_block = "WITH " + ",\n".join(cte_definitions)
    
    # Add a newline for safety, then append the original Query
    final_query = f"{cte_block}\n{sql_query}"

    return final_query