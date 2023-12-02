import os
import json

#****************************************************************************** update these variables
output_directory = "/home/user/scratch-salesforce/dbt_prep/staging/facebook/" # where you want dbt files to live
platform = "facebook"
abbreviation = "_fb"
catalog_file_path = "/home/user/scratch-salesforce/catalogs/catalog_fb_20231129.json"
#****************************************************************************** no more input needed
# function that formats the columns names into the appropriate dbt logic
# also, the code checks if the column is the last in the list. If so, it does not need the trailing comma
def generate_dbt_code(column_name, is_last_column=False):
    if is_last_column:
        return f'JSON_EXTRACT_SCALAR(a.data, "$.{column_name}") AS {column_name.lower()}'
    else:
        return f'JSON_EXTRACT_SCALAR(a.data, "$.{column_name}") AS {column_name.lower()},'

# Load the catalog JSON file
with open(catalog_file_path, 'r') as catalog_file:
    catalog_data = json.load(catalog_file)

# Extract table names and their column names 
for stream in catalog_data.get('streams', []):
    tap_stream_id = stream.get('tap_stream_id', 'Unknown Stream')
    schema = stream.get('schema', {}).get('properties', {})
    column_names = list(schema.keys())

    # ************************************************************************ SQL files 
    # Create .sql file for each table name
    file_name = f"{output_directory}{tap_stream_id.lower()}{abbreviation}.sql"
    with open(file_name, 'w') as f:
        # Write config block
        f.write("{{config(\n")
        f.write("    materialized = 'table',\n")
        f.write("    enabled = true,\n")
        f.write(f"    alias = '{tap_stream_id.lower()}',\n")
        f.write('    tags=["dbt_run:realtime"]\n')
        f.write(")}}\n\n")
        
        # Write dbt logic for each column
        f.write("SELECT\n")
        for index, column_name in enumerate(column_names):
            dbt_logic = generate_dbt_code(column_name, index == len(column_names) - 1)
            f.write(f"    {dbt_logic}\n")
        
        # Write FROM clause
        f.write(f"FROM {{{{source('raw_{platform}', '{tap_stream_id.lower()}')}}}} a")
        f.write("\nQUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY createddate DESC) = 1 \n")    
# ************************************************************************ yml file
# create schema file
schema_file_path = os.path.join(output_directory, "schema.yml")

with open(schema_file_path, 'w') as yml_file:
    yml_file.write("version: 2\n\n")
    yml_file.write("sources:\n")
    yml_file.write(f"- name: raw_{platform}\n")
    yml_file.write('  database: "{{ env_var(\'DBT_DATALAKE_PROJECT\') }}"\n')
    yml_file.write(f'  schema: "{{%- if env_var(\'DBT_CI_SCHEMA_ID\', \'\') != \'\' -%}} raw_{platform}_{{ env_var(\'DBT_CI_SCHEMA_ID\') }} {{%- else -%}} raw_{platform} {{%- endif -%}}"\n')
    yml_file.write("  tables:\n")
    
    # Writing the sources tables. Use catalog file to pull necessary information
    for stream in catalog_data.get('streams', []):
        table_name = stream.get('tap_stream_id', 'Unknown Stream')
        yml_file.write(f"  - name: {table_name}\n")

    yml_file.write("\nmodels:\n")

    # Writing the models with primary key (key_properties)
    for stream in catalog_data.get('streams', []):
        table_name = stream.get('tap_stream_id', 'Unknown Stream')
        key_properties = stream.get('key_properties', [])

    # If multiple key_properties, use the unique_combination_of_columns test
        if len(key_properties) > 1:
            yml_file.write(f"  - name: {table_name}{abbreviation}\n")
            yml_file.write("    tests:\n")
            yml_file.write("      - dbt_utils.unique_combination_of_columns:\n")
            yml_file.write("          combination_of_columns:\n")
            for key in key_properties:
                yml_file.write(f"            - {key}\n")
        else:
            primary_key = key_properties[0] if key_properties else "Id"  # Default to "Id" if not provided
            yml_file.write(f"  - name: {table_name}{abbreviation}\n")
            yml_file.write("    columns:\n")
            yml_file.write(f"      - name: {primary_key}\n")
            yml_file.write("        tests:\n")
            yml_file.write("         - not_null\n")
            yml_file.write("         - unique\n")
