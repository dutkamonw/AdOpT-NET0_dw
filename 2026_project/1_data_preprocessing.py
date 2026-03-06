
##############################################################################################################
# This script is for data preparation (preprocessing + processsing), which includes:
# 1) Run ETL functions to extract, transform, and load (ETL) raw data into duck
# 2) Run data selection functions to filter the data based on defined parameters
# 3) Combine all selected data into one table for easier querying and analysis  
# 4) Create ship routes data using SCgraph
#############################################################################################################

from pathlib import Path
from user_defined_function import etl_eea, etl_climate_trace, combine_emitters, etl_co2_storage, etl_port, select_emitters, select_co2_storage, select_ports, combine_all_selected, create_ship_routes

################### Define file paths and filter parameters ###################

## Set up all paths relative to this script's location
script_dir = Path(__file__).resolve().parent

## ----- Define file path -----
raw_dir = script_dir / 'inputs' / 'raw'
file_path_eea = str(raw_dir / 'EEA_CO2.xlsx')         # raw excel file (query from Industrial_dataset_v_15_2025_12_15.DB)
file_path_climate_trace = str(raw_dir / 'climate_trace')   # folder with multiple raw csv files
file_path_co2_storage = str(raw_dir / 'storage.xlsx')           # manual create excel file
file_path_port = str(raw_dir / 'port.csv')                      # editted csv file from World Port Index
file_path_area = str(raw_dir / 'Area_Boundary.geojson')         # manual create geojson file

## ----- Define filter parameters -----
selected_subsectors = ['steel', 'cement', 'waste']
emission_cutoff = 200000   # tCO2 per year
storage_cutoff = 15000000  # tCO2



################### Run user defined functions ###################
## -----1)  Run ETL functions -----
etl_eea(file_path_eea)
etl_climate_trace(file_path_climate_trace)
etl_port(file_path_port)
etl_co2_storage(file_path_co2_storage)

## ----- 2) Run data selection functions -----
combine_emitters()
select_emitters(file_path_area, emission_cutoff, selected_subsectors)
select_co2_storage(file_path_area, storage_cutoff)
select_ports()

## ----- 3) Combine all selected data into one table -----
combine_all_selected()

## ----- 4)  Create ship routes data using SCGraph ------
create_ship_routes()


# Print the table name from database.duckdb
import duckdb
db_path = script_dir / 'database.duckdb'
con = duckdb.connect(str(db_path))
tables = con.execute("SHOW TABLES").fetchall()
print(tables)
con.close()