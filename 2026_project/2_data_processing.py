
##############################################################################################################
# This script is for data processing, which includes:

# network_topology_prep
# 1) Build an N x N distance matrix and connection matrix from ship_routes.distance_km and export as CSV
# 2) Build an N x N distance matrix and connection matrix from pipeline_routes.distance_km and export as CSV

# Update model input files (created template from initial model setup)
# 3) Update NodeLocations.csv for model input
# 4) Update ConfigModel.json to set optimization objective and solver options
# 5) Update Topology.json to set nodes, carriers, and investment periods based on the data in database.duckdb
# 6) Update period1/Networks.json

# Copy existing technology and network data from Adopt-net0 database for model input
# 7) Copy technology JSON files from Adopt-net0 database to a destination folder for model input
# 8) Copy network data JSON files from Adopt-net0 database to a destination folder for

# Create new technology JSON files for emitters based on user input in an excel file
# 9) Create emitter_technology JSON files based on user input in excel file

##################################################################################################

import json
from pathlib import Path
import duckdb
from user_defined_function import copy_network_data_from_db, copy_technology_from_db, create_matrix, create_node_location, copy_technology_from_db, create_emitter_technology

##################################################################################################

## Set up all paths relative to this script's location
script_dir = Path(__file__).resolve().parent

## ------ Define file paths ------

output_path_data_processed = script_dir / '2_data_processed'
path_model_input = script_dir / '3_model_inputs'
db_path = script_dir / 'database.duckdb'    

# Create data_processed folder if it doesn't exist
output_path_data_processed.mkdir(parents=True, exist_ok=True)


####################################################################################################



################ 1) Build an N x N matrix from ship_routes.distance_km #####################
# The union of all ports from `from_port` and `to_port`

# ----- Define parameter -----
table_name='ship_routes'
col_start='from_port'
col_end='to_port'
value='distance_km'
output_path = output_path_data_processed / 'network_topology_prep' / f"{table_name}_matrix_{value}.csv"

# Create network_topology_prep folder if it doesn't exist
output_path.parent.mkdir(parents=True, exist_ok=True)

# ----- Run function -----
matrix = create_matrix(table_name, col_start, col_end, value, output_path)
print("---------------------")
print(f"Created matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")

# --- Create connection matrix -----
# binary adjacency: 1 if route exists, else 0
matrix_binary = (matrix > 0).astype(int)
output_path = output_path_data_processed / 'network_topology_prep' / f"{table_name}_matrix_connection.csv"
matrix_binary.to_csv(output_path, index_label='BINARY', encoding='utf-8')
print(f"Created connection matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")




################## 2) Build an N x N distance matrix and connection matrix from pipeline_network.distance_km ###################

# ----- Define parameter -----
table_name='pipeline_network'
col_start='from_name'
col_end='to_name'
value='distance_km'
output_path = output_path_data_processed / 'network_topology_prep' / f"{table_name}_matrix_{value}.csv"

# ----- Run function to create distance matrix -----
matrix = create_matrix(table_name, col_start, col_end, value, output_path)
print(f"Created matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")

# --- Create connection matrix -----
# binary adjacency: 1 if route exists, else 0
matrix_binary = (matrix > 0).astype(int)
output_path = output_path_data_processed / 'network_topology_prep' / f"{table_name}_matrix_connection.csv"
matrix_binary.to_csv(output_path, index_label='BINARY', encoding='utf-8')
print(f"Created connection matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")




################## 3) Update NodeLocations.csv for model input #######################
# Query the database to get unique node locations from combined_selected table in database.duckdb

# ----- Define parameter -----
type = ['emitter', 'port', 'storage']   # List of types of node to be included
altitude = 10  # assign altitude value to all nodes

# ----- Run function ------
create_node_location(type, altitude, path_model_input)
print("Updated NodeLocation")
print("---------------------")




###################### 4) Update ConfigModel.json  ##########################

with open(path_model_input / "ConfigModel.json", "r") as json_file:
    configuration = json.load(json_file)
# Set optimization objective (select from existing options in ConfigModel.json)
# find the minimum cost system at minimum emissions (minimizes net emissions in the first step and cost as a second step)
configuration["optimization"]["objective"]["value"] = "emissions_minC"

# Set value to define MIP gap for the optimization solver
# typically 1%-5% for large problems, lower for more accuracy but longer solve time
configuration["solveroptions"]["mipgap"]["value"] = 0.02

with open(path_model_input / "ConfigModel.json", "w") as json_file:
    json.dump(configuration, json_file, indent=4)

print("Updated ConfigModel.json")
print("---------------------")





##################### 5) Update Topology.json  #############################
# Get data from combined_selected table in database.duckdb
con = duckdb.connect(str(db_path))
node_name = con.execute("SELECT DISTINCT name FROM combined_selected").fetchall()
subsector = con.execute("SELECT DISTINCT subsector FROM combined_selected").fetchall()
con.close()

# Extract names from tuples to a list of node names
node_name_list = [name[0] for name in node_name]

# List of carriers to be included in the model (must match with the carriers defined in the model)
carrier_list = ["electricity", "heat", "CO2captured"]

# Add subsectors_list into carriers_list, drop null values if there are any
subsector_list = [subsector[0] for subsector in subsector if subsector[0] is not None]
carrier_list.extend(subsector_list)

# List of investment periods (Current model only has one period)
periods_list = ["period1"]

# ---- Update Topology.json
with open(path_model_input / "Topology.json", "r") as json_file:
    topology = json.load(json_file)

topology["nodes"] = node_name_list
topology["carriers"] = carrier_list
topology["investment_periods"] = periods_list
#topology["start_date"] =  "2040-01-01 00:00"
#topology["end_date"] = "2050-12-31 23:00"
#topology["resolution"] = "1h"

with open(path_model_input / "Topology.json", "w") as json_file:
    json.dump(topology, json_file, indent=4)
print("Updated Topology.json")
print("---------------------")


########################## 6) Update period1/Networks.json  ##########################

network_data_list = [
    "CO2_Pipeline",
    "CO2Ship"
]

with open(path_model_input / "period1" / "Networks.json", "r") as json_file:
    networks = json.load(json_file)

networks["new"] = network_data_list

with open(path_model_input / "period1" / "Networks.json", "w") as json_file:
    json.dump(networks, json_file, indent=4)
print("Updated Networks.json")
print("---------------------")






########################## 7) Copy technology JSON files from Adopt-net0 database to a destination folder ##########################
# Define the list of technology to be copied from the Adopt-net0 database.
technology_list = [
    "MEA_large",
    "MEA_medium",
    "MEA_small",
    "PermanentStorage_CO2_simple"
]
output_path = output_path_data_processed / "technology_data_prep"

# Run function
copy_technology_from_db(technology_list, output_path)



########################## 8) Create emitter_technology JSON files based on user input in excel file ##############################
# Define the input excel file path and output folder path for the generated technology JSON files.
input_path_excel = script_dir / '1_raw' / 'technology_emitter.xlsx'
output_path = output_path_data_processed / "technology_data_prep"

# Run function
print("----------------------")
create_emitter_technology(input_path_excel, output_path)




########################## 9) Copy network data JSON files from Adopt-net0 database ##########################
# network_data_list should match with the network names defined in Networks.json
output_path = output_path_data_processed / "network_data_prep"

# Run function
print("----------------------")
copy_network_data_from_db(network_data_list, output_path)
