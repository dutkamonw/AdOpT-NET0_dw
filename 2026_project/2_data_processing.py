##############################################################################################################
# This script is for data processing, which includes:
# 1) Build an N x N matrix from ship_routes.distance_km and export as CSV
# 2) Build an N x N matrix from pipeline_routes.distance_km
# 3) Creating NodeLocations.csv for model input
##################################################################################################
from pathlib import Path
from user_defined_function import create_matrix

######## 1) Build an N x N matrix from ship_routes.distance_km ##########
# Note: The union of all ports from `from_port` and `to_port`

# ----- Define parameter -----
table_name='ship_routes'
col_start='from_port'
col_end='to_port'
value='distance_km'
output_path = Path(__file__).resolve().parent / 'data_processed' / f"{table_name}_matrix_{value}.csv"

# --- Run function --
matrix = create_matrix(table_name, col_start, col_end, value, output_path)
print(f"Created matrix for '{table_name}' and exported to CSV in {output_path} ")

######### 2) Create binary (or weighted) adjacency matrix for pipeline routes ##########
# binary adjacency: 1 if route exists, else 0
matrix_binary = (matrix > 0).astype(int)
output_path = Path(__file__).resolve().parent / 'data_processed' / f"{table_name}_matrix_binary.csv"
matrix_binary.to_csv(output_path, index_label='BINARY')