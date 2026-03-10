##############################################################################################################
# This script is for data processing, which includes:
# 1) Build an N x N matrix and binary matrix from ship_routes.distance_km and export as CSV
# 2) Build an N x N matrix from pipeline_routes.distance_km
# 3) Create NodeLocations.csv for model input
##################################################################################################

from pathlib import Path
from user_defined_function import create_matrix, create_node_location

## ------ Define file paths ------

output_path_data_processed = Path(__file__).resolve().parent / '2_data_processed'
# Create data_processed folder if it doesn't exist
output_path_data_processed.mkdir(parents=True, exist_ok=True)

######## 1) Build an N x N matrix from ship_routes.distance_km ##########
# The union of all ports from `from_port` and `to_port`

# ----- Define parameter -----
table_name='ship_routes'
col_start='from_port'
col_end='to_port'
value='distance_km'
output_path = output_path_data_processed / f"{table_name}_matrix_{value}.csv"

# ----- Run function -----
matrix = create_matrix(table_name, col_start, col_end, value, output_path)
print("---------------------")
print(f"Created matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")

# --- Create binary (or weighted) adjacency matrix -----
# binary adjacency: 1 if route exists, else 0
matrix_binary = (matrix > 0).astype(int)
output_path = output_path_data_processed / f"{table_name}_matrix_binary.csv"
matrix_binary.to_csv(output_path, index_label='BINARY')
print(f"Created binary matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")



######## 2) Build an N x N matrix from pipeline_network.distance_km ##########

# ----- Define parameter -----
table_name='pipeline_network'
col_start='from_name'
col_end='to_name'
value='distance_km'
output_path = output_path_data_processed / f"{table_name}_matrix_{value}.csv"

# ----- Run function -----
matrix = create_matrix(table_name, col_start, col_end, value, output_path)
print("---------------------")
print(f"Created matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")

# --- Create binary (or weighted) adjacency matrix -----
# binary adjacency: 1 if route exists, else 0
matrix_binary = (matrix > 0).astype(int)
output_path = output_path_data_processed / f"{table_name}_matrix_binary.csv"
matrix_binary.to_csv(output_path, index_label='BINARY')
print(f"Created binary matrix for '{table_name}' and exported to CSV in {output_path} ")
print("---------------------")



######### 3) Create NodeLocations.csv for model input ##########
# Query the database to get unique node locations from combined_selected table in database.duckdb

# ----- Define parameter -----
type = ['emitter', 'port', 'storage']   # List of types of node to be included
altitude = 10  # assign altitude value to all nodes

# ----- Run function ------
create_node_location(type, altitude)
print("Created NodeLocation.csv in inputs")
print("---------------------")