import json
import pandas as pd
import os

# Directory containing your JSON data files
data_directory = '/home/user/scratch-salesforce/dbt_prep/linkedin_json_desc'  # Replace with the directory path

# Initialize an empty list to store all data
all_data = []

# Iterate over each text file in the directory
for filename in os.listdir(data_directory):
    if filename.endswith(".txt"):
        file_path = os.path.join(data_directory, filename)
        
        # Read JSON data from the text file
        with open(file_path, 'r') as json_file:
            json_data = json.load(json_file)

        # Extract field names
        field_names = list(json_data.get("properties", {}).keys())

        # Create a list of dictionaries for this table
        table_data = [{"table_name": os.path.splitext(filename)[0], "field_name": field} for field in field_names]

        # Add the table data to the list
        all_data.extend(table_data)

# Create a DataFrame from all the data
df = pd.DataFrame(all_data)

# Define the Excel file path
#excel_file_path = 'linkedin_data_dictionary.xlsx'  # Replace with the desired path and filename


# Define the CSV file path
csv_file_path = 'linkedin_data_dictionary.csv'  # Replace with the desired path and filename

# Write the DataFrame to a CSV file
df.to_csv(csv_file_path, index=False)


# Write the DataFrame to an Excel file
#df.to_excel(excel_file_path, index=False)

#print(f"Data has been saved to {excel_file_path}")
