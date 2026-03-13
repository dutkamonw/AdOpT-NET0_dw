########### This files contains user-defined functions, including: ##########

### The functions are called in 1_data_preprocessing.py: ###
# 1. ETL pipeline for loading all cleaned data into duckdb database (EEA, Climate TRACE, CO2 storage, port)
# 2. Data selection for 2026 project case study based on defined parameters (query from database.duckdb and filter in python)
# 3. Combines all selected emitters, storage, and ports data into a single table
# 4. Create shipping route based on selected ports by SCGraph's marnet geograph API
# 5. (a) Calculate straight line distance based on haversine formula (the greatest circle distance)
# 5. (b) Create pipeline network based on selected emitters and ports using straight line (haversine) and Prim's algorithm (for clustering)

### The functions are called in 2_data_processing.py for model input: ###
# 6. Create N x N matrix from database
# 7. Create NodeLocations.csv from database
# 8. Copy technology JSON files from adopt-net0 database
# 9. Create technology JSON files for emitters based on Excel config (if needed, not in current scope)
# 10. Copy network data JSON files from adopt-net0 database

################################################################################

import csv
import pandas as pd
import numpy as np
import duckdb
from pyproj import Proj
import glob
import datetime as dt
import pycountry
import geopandas as gpd
from pathlib import Path
from shapely.geometry import LineString
from scgraph.geographs.marnet import marnet_geograph
import json

# Database path relative to this module's location
DB_PATH = str(Path(__file__).resolve().parent / 'database.duckdb')

############ 1. ETL pipeline for loading all cleaned data into duckdb database (EEA, Climate TRACE, CO2 storage, port) ################

## ----- Create mapping function to standardise subsector name (only interested sectors) -----
def map_subsector(code):
    """ Map source's subsector code to subsector category: steel, cement, waste, or None."""
    if pd.isna(code):
        return None
    code_str = str(code)
    # Refineries: 1(a), 'oil-and-gas-refining'
    if code_str.startswith('1(a)') or code_str.startswith('oil-and-gas-refining'):
        return 'refineries'
    # Gasification: 1(b)
    elif code_str.startswith('1(b)'):
        return 'gasification'
    # Steel: 2(a), 2(b), 2(c)*, 2(d), 'iron-and-steel'
    elif code_str.startswith('2(a)') or code_str.startswith('2(b)') or code_str.startswith('2(c)') or code_str.startswith('2(d)') or code_str.startswith('iron-and-steel'):
        return 'steel'
    # Cement: 3(c)*, 'cement'
    elif code_str.startswith('3(c)') or code_str.startswith('cement'):
        return 'cement'
    # Petrochemical: 4(a), 'petrochemical'
    elif code_str.startswith('4(a)') or code_str.startswith('petrochemical-steam-cracking'):
        return 'petrochemical'
    # Waste: 5(b)
    elif code_str.startswith('5(b)'):
        return 'waste'
    else:
        return None

## ----- Create mapping function to standardise country name to iso2 -----
def map_iso3_to_iso2(code):
    """Map ISO-3 country code to ISO-2, returning None for invalid/missing codes."""
    if pd.isna(code):
        return None
    country = pycountry.countries.get(alpha_3=str(code).strip().upper())
    return country.alpha_2 if country else None


def map_country_to_iso2(country_value):
    """Map full country name (or ISO code) to ISO-2, returning None for invalid/missing values."""
    if pd.isna(country_value):
        return None

    value = str(country_value).strip()
    if not value:
        return None

    # Normalize input to handle case differences and hidden spaces.
    value_norm = " ".join(value.replace("\u00a0", " ").split()).casefold()

    # Handle common names/variants that may not resolve reliably via pycountry.
    aliases = {
        'turkey': 'TR',
        'turkiye': 'TR',
        'türkiye': 'TR',
        'turkiye (turkey)': 'TR',
        'republic of turkey': 'TR',
    }
    alias_hit = aliases.get(value_norm)
    if alias_hit:
        return alias_hit

    # Already ISO-2
    if len(value) == 2 and value.isalpha():
        return value.upper()

    # ISO-3 input
    iso3_match = pycountry.countries.get(alpha_3=value.upper())
    if iso3_match:
        return iso3_match.alpha_2

    # Full country name (or common alias recognized by pycountry)
    try:
        return pycountry.countries.lookup(value).alpha_2
    except LookupError:
        return None


## ----- ETL for EEA data -----
def etl_eea(file_path_eea):
    """ETL process for EEA data: Extract, Transform, Load."""
    
    # Import EEA excel file in raw folder
    eea = pd.read_excel(file_path_eea)

    ### Transformation ###
    # Rename columns
    eea.rename(columns={'nameOfFeature': 'name','pointGeometryLat': 'latitude', 'pointGeometryLon': 'longitude', 'reportingYear' : 'year', 'countryCode': 'iso2'}, inplace=True)
    # Convert latitude, longitude, and emission to numeric
    eea['latitude'] = pd.to_numeric(eea['latitude'], errors='coerce')
    eea['longitude'] = pd.to_numeric(eea['longitude'], errors='coerce')
    eea['totalPollutantQuantityKg'] = pd.to_numeric(eea['totalPollutantQuantityKg'], errors='coerce')
    # Add emission column in tCO2 per year
    eea['emission_TPA'] = eea['totalPollutantQuantityKg'] / 1000
    # Add subsector column based on mainActivityCode
    eea['subsector'] = eea['mainActivityCode'].apply(map_subsector)
    # Add data_source column
    eea['data_source'] = 'eea'

    ### Store the data in database.duckdb, if exists, replace it ###
    con = duckdb.connect(DB_PATH)
    con.register('eea', eea)
    con.execute("CREATE OR REPLACE TABLE eea AS SELECT * FROM eea")
    con.close()

## ----- ETL for Climate TRACE data -----
def etl_climate_trace(file_path_climate_trace):
    """ETL process for Climate TRACE data: Extract, Transform, Load."""
    # Import all Climate TRACE csv file in the folder
    climate_trace = glob.glob(file_path_climate_trace + "/*.csv")
    climate_trace = pd.concat((pd.read_csv(file, low_memory=False, encoding='utf-8') for file in climate_trace), ignore_index=True)
    
    ### Transformation ###
    # Rename columns
    climate_trace.rename(columns={'source_name' : 'name', 'subsector': 'source_subsector', 'lat': 'latitude', 'lon': 'longitude', 'emissions_quantity' : 'emission_TPA'}, inplace=True)
    # Convert latitude, longitude, and emission to numeric 
    climate_trace['latitude'] = pd.to_numeric(climate_trace['latitude'], errors='coerce')
    climate_trace['longitude'] = pd.to_numeric(climate_trace['longitude'], errors='coerce')
    climate_trace['emission_TPA'] = pd.to_numeric(climate_trace['emission_TPA'], errors='coerce')
    # Add 'iso2' column by converting 'iso3_country' to iso2
    climate_trace['iso2'] = climate_trace['iso3_country'].apply(map_iso3_to_iso2)
    # Create 'year' column based on 'start_time'
    climate_trace['year'] = pd.to_datetime(climate_trace['start_time'], errors='coerce').dt.year
    # Sum emission by 'name', 'year', 'latitude', and 'longitude' to combine multiple entries for the same source in the same year, if any
    climate_trace = climate_trace.groupby(['name', 'year', 'latitude', 'longitude'], as_index=False).agg({'emission_TPA': 'sum', 'source_subsector': 'first', 'iso2': 'first'})
    # Add subsector column based on source_subsector
    climate_trace['subsector'] = climate_trace['source_subsector'].apply(map_subsector)
    # Add data_source column
    climate_trace['data_source'] = 'climate_trace'

    ### Store the data in database.duckdb, if exists, replace it ###
    con = duckdb.connect(DB_PATH)
    con.register('climate_trace', climate_trace)
    con.execute("CREATE OR REPLACE TABLE climate_trace AS SELECT * FROM climate_trace")
    con.close()


## ----- Combine all emitter data into one table for study -----
def combine_emitters():
    """Combine emitter data from EEA and Climate TRACE"""
    con = duckdb.connect(DB_PATH)
    combined = con.execute("SELECT name, iso2, latitude, longitude, year, emission_TPA, subsector, data_source FROM eea UNION ALL SELECT name, iso2, latitude, longitude, year, emission_TPA, subsector, data_source FROM climate_trace").fetchdf()
    con.register('emitters_all', combined)
    con.execute("CREATE OR REPLACE TABLE emitters_all AS SELECT * FROM emitters_all")
    con.close()


## ----- ETL for CO2 Storage data -----
def etl_co2_storage(file_path_co2_storage):
    """ETL process for CO2 Storage data: Extract, Transform, Load."""
    
    # Import CO2 Storage excel file in raw folder
    co2_storage = pd.read_excel(file_path_co2_storage)
    
    ### Transformation ###
    # Convert x, y, and TOTAL_CAPACITY_BASE_MT to numeric
    co2_storage['x'] = pd.to_numeric(co2_storage['x'], errors='coerce')
    co2_storage['y'] = pd.to_numeric(co2_storage['y'], errors='coerce')
    co2_storage['TOTAL_CAPACITY_BASE_MT'] = pd.to_numeric(co2_storage['TOTAL_CAPACITY_BASE_MT'], errors='coerce')
    # Covert 'TOTAL_CAPACITY_BASE_MT' in MtCO2 to 'capacity' in tCO2
    co2_storage['capacity_T'] = co2_storage['TOTAL_CAPACITY_BASE_MT'] * 1000000
    # Convert x, y to latitude, longitude where needed
    mask = co2_storage['latitude'].isna() & co2_storage['EPSG'].notna() & co2_storage['x'].notna() & co2_storage['y'].notna()
    for idx in co2_storage[mask].index:
        try:
            proj = Proj(f"epsg:{int(co2_storage.loc[idx, 'EPSG'])}")
            lon, lat = proj(co2_storage.loc[idx, 'x'], co2_storage.loc[idx, 'y'], inverse=True)
            co2_storage.loc[idx, 'latitude'] = lat
            co2_storage.loc[idx, 'longitude'] = lon
        except:
            pass
    
    ### Store the data in database.duckdb, if exists, replace it ###
    con = duckdb.connect(DB_PATH)
    con.register('co2_storage', co2_storage)
    con.execute("CREATE OR REPLACE TABLE co2_storage AS SELECT * FROM co2_storage")
    con.close()

## ----- ETL for port data -----
def etl_port(file_path_port):
    """ETL process for port data: Extract, Transform, Load."""
    # Import port excel file in raw folder
    port = pd.read_csv(file_path_port, encoding='utf-8')
    
    ### Transformation ###
    # Rename columns
    port.rename(columns={'Main Port Name': 'name', 'Latitude': 'latitude', 'Longitude': 'longitude'}, inplace=True)
    # Convert latitude and longitude to numeric
    port['latitude'] = pd.to_numeric(port['latitude'], errors='coerce')
    port['longitude'] = pd.to_numeric(port['longitude'], errors='coerce')

    ### Store the data in database.duckdb, if exists, replace it ###
    con = duckdb.connect(DB_PATH)
    con.register('port', port)
    con.execute("CREATE OR REPLACE TABLE port AS SELECT * FROM port")
    con.close()


##################   2. Data selection for case study #####################

## ----- Select emitters ------
def select_emitters(file_path_area, emission_cutoff, selected_subsectors):
    """Select emitters based on area, emission cutoff, and subsectors."""
    # Import area from geojson
    area = gpd.read_file(file_path_area)
    # Get data from emitters table
    con = duckdb.connect(DB_PATH)
    emitters = con.execute("SELECT name, latitude, longitude, year, emission_TPA, subsector, data_source, iso2 FROM emitters_all").fetchdf()
    con.close()

    ### Filtering ###
    # Keep only selected subsectors
    emitters = emitters[emitters['subsector'].isin(selected_subsectors)]
    # Drop climate_trace data where iso2 is EU country
    eu_countries = ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 
                    'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE']
    emitters = emitters[~((emitters['data_source'] == 'climate_trace') & (emitters['iso2'].isin(eu_countries)))]
    # Drop data older than (current_year - 4) to keep only active data
    current_year = dt.datetime.now().year
    emitters = emitters[emitters['year'] > current_year - 4]
    # Keep only latest year for each name
    emitters = emitters.sort_values('year', ascending=False).groupby('name', as_index=False).first()
    # Clip data within area (spatial filter)
    emitters_gdf = gpd.GeoDataFrame(
        emitters, 
        geometry=gpd.points_from_xy(emitters['longitude'], emitters['latitude']),
        crs='EPSG:4326')
    # Ensure area has same CRS
    if area.crs != emitters_gdf.crs:
        area = area.to_crs(emitters_gdf.crs)
    # Clip data within area (spatial filter)
    emitters_selected = gpd.sjoin(emitters_gdf, area, how='inner', predicate='within')
    # Keep only emission >= emission_cutoff tCO2
    emitters_selected = emitters_selected[emitters_selected['emission_TPA'] >= emission_cutoff]
    # After final selection, drop iso2 == Tunisia (TN) and Algeria (DZ) as all storage sites are full
    emitters_selected = emitters_selected[~emitters_selected['iso2'].isin(['TN', 'DZ'])]

    ### Store the selected emitters data in database.duckdb, if exists, replace it ###
    # Drop geometry column (not supported by DuckDB)
    emitters_selected_df = emitters_selected.drop(columns=['geometry', 'index_right'], errors='ignore').copy()
    # Add point type column for downstream joins/exports
    emitters_selected_df['type'] = 'emitter'
    con = duckdb.connect(DB_PATH)
    con.register('emitters_selected', emitters_selected_df)
    con.execute("CREATE OR REPLACE TABLE emitters_selected AS SELECT * FROM emitters_selected")
    con.close()
    

## ----- Select co2_storage ------
def select_co2_storage(file_path_area, storage_cutoff):
    """Select CO2 storage sites based on area and capacity cutoff. Need to define 'group' for clustering in raw file"""
    # Import area from geojson
    area = gpd.read_file(file_path_area)
    # Get data from co2_storage table 
    con = duckdb.connect(DB_PATH)
    co2_storage = con.execute("SELECT * FROM co2_storage").fetchdf()
    con.close()

    ### Filtering ###
    # Drop missing coordinate and capacity data
    co2_storage = co2_storage.dropna(subset=['latitude', 'longitude', 'capacity_T'])
    # Clip data within area (spatial filter)
    co2_storage_gdf = gpd.GeoDataFrame(
        co2_storage, 
        geometry=gpd.points_from_xy(co2_storage['longitude'], co2_storage['latitude']),
        crs='EPSG:4326')
    # Ensure area has same CRS
    if area.crs != co2_storage_gdf.crs:
        area = area.to_crs(co2_storage_gdf.crs)
    # Clip data within area (spatial filter)
    co2_storage_selected = gpd.sjoin(co2_storage_gdf, area, how='inner', predicate='within')
    # Keep only storage with capacity >= storage_cutoff
    co2_storage_selected = co2_storage_selected[co2_storage_selected['capacity_T'] >= storage_cutoff]
    # Sum capacity by 'group' and keep other data from the row with largest capacity
    co2_storage_selected = co2_storage_selected.sort_values('capacity_T', ascending=False)
    agg_dict = {col: 'first' for col in co2_storage_selected.columns if col != 'capacity_T'}
    agg_dict['capacity_T'] = 'sum'
    co2_storage_selected = co2_storage_selected.groupby('group', as_index=False).agg(agg_dict)
    
    ### Store the selected co2 storage data in database.duckdb, if exists, replace it ###
    # Drop geometry column (not supported by DuckDB)
    co2_storage_selected_df = co2_storage_selected.drop(columns=['geometry', 'index_right'], errors='ignore').copy()
    co2_storage_selected_df['type'] = 'storage'
    # Keep only necessary columns
    co2_storage_selected_df = co2_storage_selected_df[['group', 'name', 'iso2', 'latitude', 'longitude', 'capacity_T', 'type', 'data_source']]

    con = duckdb.connect(DB_PATH)
    con.register('co2_storage_selected', co2_storage_selected_df)
    con.execute("CREATE OR REPLACE TABLE co2_storage_selected AS SELECT * FROM co2_storage_selected")
    con.close() 

## ----- Select ports ------
def select_ports():
    """Select ports based on 'selected' column."""
    con = duckdb.connect(DB_PATH)
    port = con.execute("SELECT * FROM port").fetchdf()
    con.close()

    ### Filtering ###
    # Keep only ports where Selected == 'yes'
    port_selected = port[port['Selected'] == 'yes']
    # Rename columns
    port_selected.rename(columns={'Screening': 'screening', 'Country Code': 'country'}, inplace=True)
    # Change country field (full name in source file) to iso2
    port_selected['iso2'] = port_selected['country'].apply(map_country_to_iso2)
    # Safety fallback for known exact text in source files
    turkey_mask = port_selected['country'].astype(str).str.strip().str.casefold().eq('turkey')
    port_selected.loc[turkey_mask, 'iso2'] = 'TR'
    # Add column 'Type' == port
    port_selected['type'] = 'port'
    # Keep only necessary columns
    port_selected = port_selected[['name', 'screening', 'iso2', 'latitude', 'longitude', 'type']]

    ### Store the selected port data in database.duckdb, if exists, replace it ###
    con = duckdb.connect(DB_PATH)
    con.register('port_selected', port_selected)
    con.execute("CREATE OR REPLACE TABLE port_selected AS SELECT * FROM port_selected")
    con.close()


############# 3. Combines all seleted emiiters, storage, and ports data #####################
def combine_all_selected(output_path):
    """Combine all selected emitters, storage, and ports data into one table"""
    con = duckdb.connect(DB_PATH)
    query = """
    SELECT
        CAST(NULL AS VARCHAR) AS "group",
        name,
        iso2,
        latitude,
        longitude,
        emission_TPA,
        CAST(NULL AS DOUBLE) AS capacity_T,
        subsector,
        data_source,
        type,
        CAST(NULL AS VARCHAR) AS screening,
        year
    FROM emitters_selected
    UNION ALL
    SELECT
        "group",
        name,
        iso2,
        latitude,
        longitude,
        CAST(NULL AS DOUBLE) AS emission_TPA,
        capacity_T,
        CAST(NULL AS VARCHAR) AS subsector,
        data_source,
        type,
        CAST(NULL AS VARCHAR) AS screening,
        CAST(NULL AS BIGINT) AS year
    FROM co2_storage_selected
    UNION ALL
    SELECT
        CAST(NULL AS VARCHAR) AS "group",
        name,
        iso2,
        latitude,
        longitude,
        CAST(NULL AS DOUBLE) AS emission_TPA,
        CAST(NULL AS DOUBLE) AS capacity_T,
        CAST(NULL AS VARCHAR) AS subsector,
        CAST(NULL AS VARCHAR) AS data_source,
        type,
        screening,
        CAST(NULL AS BIGINT) AS year
    FROM port_selected
    """
    # Store the combined data in database.duckdb, if exists, replace it
    combined_selected = con.execute(query).fetchdf()
    con.register('combined_selected', combined_selected)
    con.execute("CREATE OR REPLACE TABLE combined_selected AS SELECT * FROM combined_selected")
    con.close()

    return combined_selected

    # Export combined_selected to excel for manual checking
    combined_selected.to_excel(Path(output_path) / 'combined_selected.xlsx', index=False)

############# 4. Create ship route data for selected ports #####################
def create_ship_routes(output_path):
    con = duckdb.connect(DB_PATH)
    # Get selected ports
    ports_selected = con.execute("SELECT * FROM combined_selected WHERE type = 'port'").fetchdf()
    # Create sink ports
    sink_ports = (
    ports_selected[ports_selected['screening'] == 'sink'].reset_index(drop=True))
    # Create emitter ports
    emitter_ports = (
    ports_selected[ports_selected['screening'] != 'sink'].reset_index(drop=True))

    ### Create ship routes between emitter ports and sink ports using marnet geograph
    routes = []
    port_records = emitter_ports.to_dict('records')
    sink_records = sink_ports.to_dict('records')

    # Note: Depending on the number of ports, this nested loop could result in a large number of API calls to marnet_geograph.
    for port in port_records:
        for sink in sink_records:
            # Get shortest path and distance from port to sink using marnet_geograph
            result = marnet_geograph.get_shortest_path(
                origin_node={"latitude": port["latitude"], "longitude": port["longitude"]},
                destination_node={"latitude": sink["latitude"], "longitude": sink["longitude"]},
                output_units='km')
            # Extract coordinates and distance from the result
            coords = result["coordinate_path"]
            distance = result["length"]
            # Convert to LineString (lon, lat)
            line = LineString([(lon, lat) for lat, lon in coords])
            # Append route information to the list        
            routes.append({
                "from": port["name"],
                "to": sink["name"],
                "distance_km": distance,
                "geometry": line})

    ## Store the ship routes data in database.duckdb, if exists, replace it. Geometry is stored as WKT string for compatibility with DuckDB and potential use in mapping applications. The original LineString geometry is retained in the code for any future use that may require geometric operations before storage.
    # Store ship routes in single table with geometry as WKT for dual use (metrics + mapping)
    routes_df = pd.DataFrame(routes)
    routes_df.insert(0, 'route_id', range(1, len(routes_df) + 1))
    # Convert geometry to WKT string for database storage
    routes_df['geometry_wkt'] = routes_df['geometry'].apply(
        lambda geom: geom.wkt if geom is not None else None)
    # Prepare final table
    ship_routes = routes_df[['route_id', 'from', 'to', 'distance_km', 'geometry_wkt']].rename(
        columns={'from': 'from_port', 'to': 'to_port'})
    # Export to excel for manual checking
    ship_routes.to_excel(Path(output_path) / 'ship_routes.xlsx', index=False)
    
    # Load into database
    con.register('ship_routes', ship_routes)
    con.execute("CREATE OR REPLACE TABLE ship_routes AS SELECT * FROM ship_routes")
    con.close()

############# 5. Create pipeline network for selected emitters and ports using straight line distance #####################

## Function to calculate straight line distance based on haversine formula, which accounts for the curvature of the Earth. The distance is returned in kilometers.
def distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance in kilometers between two points on the Earth specified in decimal degrees."""
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lat2, lon2 = np.radians(lat2), np.radians(lon2)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371.0 * (2.0 * np.arcsin(np.sqrt(a)))  # kilometers


## Function to create pipeline network
def create_pipeline_network(output_path):
    """
    Build a CO2 pipeline network from combined_selected data.

    Edge types produced:
      emitter_to_emitter   – MST edges connecting emitters within the same cluster
      emitter_to_port      – MST edges connecting an emitter directly to its cluster's loading port;
                             every loading port (screening != 'sink') has at least one such edge
      emitter_to_alternative – shortest cross-cluster emitter-emitter bridge, one per cluster pair
      emitter_to_terminal  – emitter → sink port (screening == 'sink'); added for an emitter only
                             when the terminal distance is shorter than its shortest other edge;
                             every terminal gets at least one such edge
      terminal_to_storage  – sink port → storage site (nearest terminal per storage)

    Clustering: each emitter is assigned to its nearest loading port; Prim's MST is then run on the
    combined set of {emitters in cluster} + {their loading port} so the MST naturally decides
    whether each emitter connects directly to the port or chains through another emitter.
    """
    con = duckdb.connect(DB_PATH)
    combined_selected = con.execute(
        'SELECT name, type, latitude, longitude, screening FROM "combined_selected"'
    ).fetchdf()

    emitter  = combined_selected[combined_selected["type"] == "emitter"].reset_index(drop=True)
    port_all = combined_selected[combined_selected["type"] == "port"].reset_index(drop=True)
    storage  = combined_selected[combined_selected["type"] == "storage"].reset_index(drop=True)

    # Loading ports (non-sink) vs. terminal ports (sink)
    port = port_all[port_all["screening"] != "sink"].reset_index(drop=True)
    sink = port_all[port_all["screening"] == "sink"].reset_index(drop=True)

    emit_lat = emitter["latitude"].to_numpy()
    emit_lon = emitter["longitude"].to_numpy()
    port_lat = port["latitude"].to_numpy() if len(port) > 0 else np.array([])
    port_lon = port["longitude"].to_numpy() if len(port) > 0 else np.array([])
    sink_lat = sink["latitude"].to_numpy() if len(sink) > 0 else np.array([])
    sink_lon = sink["longitude"].to_numpy() if len(sink) > 0 else np.array([])

    cols = ["edge_type", "from_name", "from_latitude", "from_longitude",
            "to_name", "to_latitude", "to_longitude", "distance_km"]

    # ── Distance matrices ────────────────────────────────────────────────────
    dist_ep = (distance(emit_lat[:, None], emit_lon[:, None], port_lat[None, :], port_lon[None, :])
               if len(port) > 0 else np.empty((len(emitter), 0)))
    dist_ee = distance(emit_lat[:, None], emit_lon[:, None], emit_lat[None, :], emit_lon[None, :])
    dist_es = (distance(emit_lat[:, None], emit_lon[:, None], sink_lat[None, :], sink_lon[None, :])
               if len(sink) > 0 else None)

    # ── Cluster assignment: each emitter → index of nearest loading port ─────
    b = dist_ep.argmin(axis=1) if len(port) > 0 else np.zeros(len(emitter), dtype=int)

    # ── Prim's MST per cluster (emitters + their loading port) ───────────────
    # Nodes 0..n_e-1 map to emit_indices; node n_e is the loading port.
    # Starting from the port ensures the MST is rooted there, so the edges
    # produced naturally represent CO2 flowing toward the port.
    mst_rows = []

    def _prim_cluster(emit_indices, p_idx):
        # emit_indices: indices of emitters in the cluster (referring to rows in emitter DataFrame)
        n_e = len(emit_indices)
        n   = n_e + 1
        # Prim's algorithm with adjacency matrix given by dist_ee for emitter-emitter edges and dist_ep for emitter-port edges
        in_tree  = np.zeros(n, dtype=bool)
        min_cost = np.full(n, np.inf)
        parent   = np.full(n, -1, dtype=int)
        min_cost[n_e] = 0.0  # root: loading port

        for _ in range(n):
            # pick cheapest not-yet-in-tree node
            u = -1; best = np.inf
            # Note: the port node (n_e) is included in this loop and can be picked when its min_cost is lowest. 
            # Subsequent emitters will then connect either to the port or to other emitters based on the MST logic.
            for v in range(n):
                if not in_tree[v] and min_cost[v] < best:
                    best, u = min_cost[v], v
            if u == -1:
                break
            in_tree[u] = True
            # add edge (parent[u], u) to MST result, if u is not the root
            if parent[u] != -1:
                pu = parent[u]
                if u < n_e and pu < n_e:
                    # emitter–emitter edge
                    i, j = emit_indices[u], emit_indices[pu]
                    mst_rows.append({
                        "edge_type":      "emitter_to_emitter",
                        "from_name":      emitter.iloc[i]["name"],
                        "from_latitude":  emit_lat[i], "from_longitude": emit_lon[i],
                        "to_name":        emitter.iloc[j]["name"],
                        "to_latitude":    emit_lat[j], "to_longitude":   emit_lon[j],
                        "distance_km":    dist_ee[i, j],
                    })
                else:
                    # emitter–port edge (one of u/pu is n_e, the other is an emitter)
                    ei = emit_indices[u] if u < n_e else emit_indices[pu]
                    mst_rows.append({
                        "edge_type":      "emitter_to_port",
                        "from_name":      emitter.iloc[ei]["name"],
                        "from_latitude":  emit_lat[ei], "from_longitude": emit_lon[ei],
                        "to_name":        port.iloc[p_idx]["name"],
                        "to_latitude":    port_lat[p_idx], "to_longitude":   port_lon[p_idx],
                        "distance_km":    dist_ep[ei, p_idx],
                    })

            # relax edges from u
            for v in range(n):
                # Skip if already in tree or if u and v are both the port node (no self-loop)
                if in_tree[v]:
                    continue
                # Determine cost of edge (u, v) based on whether u and v are emitters or the port
                if u < n_e and v < n_e:
                    cost = dist_ee[emit_indices[u], emit_indices[v]]
                # One of u/v is the port node (n_e) and the other is an emitter: cost from dist_ep
                elif u < n_e and v == n_e:
                    cost = dist_ep[emit_indices[u], p_idx]
                # The case of u == n_e and v == n_e is not valid (no self-loop on port)
                elif u == n_e and v < n_e:
                    cost = dist_ep[emit_indices[v], p_idx]
                else:
                    continue
                # Relax edge (u, v) if cost is lower
                if cost < min_cost[v]:
                    min_cost[v] = cost
                    parent[v] = u
    # Run Prim's MST for each cluster
    if len(port) > 0:
        for cid in range(len(port)):
            # Find emitters in this cluster (those whose nearest port is cid)
            members = np.where(b == cid)[0].tolist()
            # If no emitters are assigned to this port, connect the port directly to its closest emitter (even though it's not in the same cluster by the nearest-port rule).
            if len(members) == 0:
                # Port has no emitters cluster-assigned: connect its closest emitter directly
                e_idx = int(dist_ep[:, cid].argmin())
                mst_rows.append({
                    "edge_type":      "emitter_to_port",
                    "from_name":      emitter.iloc[e_idx]["name"],
                    "from_latitude":  emit_lat[e_idx], "from_longitude": emit_lon[e_idx],
                    "to_name":        port.iloc[cid]["name"],
                    "to_latitude":    port_lat[cid], "to_longitude":   port_lon[cid],
                    "distance_km":    dist_ep[e_idx, cid],
                })
            else:
                _prim_cluster(members, cid)

    mst_df             = pd.DataFrame(mst_rows, columns=cols) if mst_rows else pd.DataFrame(columns=cols)
    emitter_to_port    = mst_df[mst_df["edge_type"] == "emitter_to_port"].reset_index(drop=True)
    emitter_to_emitter = mst_df[mst_df["edge_type"] == "emitter_to_emitter"].reset_index(drop=True)

    # ── emitter_to_alternative: one bridge per cluster pair (shortest) ───────
    alt_candidates = []
    seen_pairs = set()
    for i in range(len(emitter)):
        # Find emitters in different clusters (b) and calculate distance to them using dist_ee; pick the closest one as alternative edge candidate for this emitter.
        other = np.where(b != b[i])[0]
        if len(other) == 0:
            continue
        # Pick the closest emitter in a different cluster as alternative edge candidate
        j = int(other[dist_ee[i, other].argmin()])
        key = (min(i, j), max(i, j))
        # Add this pair as an alternative edge candidate if we haven't already added an alternative edge for this cluster pair.
        # The seen_pairs set ensures we only add one alternative edge per cluster pair, even if multiple emitters in the same cluster have the same closest emitter in the other cluster.
        if key not in seen_pairs:
            seen_pairs.add(key)
            alt_candidates.append({
                "edge_type":      "emitter_to_alternative",
                "from_name":      emitter.iloc[i]["name"],
                "from_latitude":  emit_lat[i], "from_longitude": emit_lon[i],
                "to_name":        emitter.iloc[j]["name"],
                "to_latitude":    emit_lat[j], "to_longitude":   emit_lon[j],
                "distance_km":    dist_ee[i, j],
                "_from_cluster":  int(b[i]),
                "_to_cluster":    int(b[j]),
            })
    # Among the candidate alternative edges, keep only the shortest one per cluster pair to avoid redundancy.
    if alt_candidates:
        alt_df = pd.DataFrame(alt_candidates)
        alt_df["_cluster_pair"] = alt_df.apply(
            lambda r: (min(r["_from_cluster"], r["_to_cluster"]),
                       max(r["_from_cluster"], r["_to_cluster"])), axis=1
        )
        alt_df = alt_df.loc[alt_df.groupby("_cluster_pair")["distance_km"].idxmin()]
        emitter_to_alt = alt_df[cols].reset_index(drop=True)
    else:
        emitter_to_alt = pd.DataFrame(columns=cols)

    # ── emitter_to_terminal: emitter → nearest sink port ─────────────────────
    # Rule: add only when d(emitter→terminal) < emitter's shortest other edge.
    # Guarantee: every terminal (sink port) has at least one emitter_to_terminal.
    emitter_to_terminal = pd.DataFrame(columns=cols)
    if len(sink) > 0 and dist_es is not None:
        # Per-emitter minimum distance across all edges built so far
        all_other = pd.concat([emitter_to_port, emitter_to_emitter, emitter_to_alt], ignore_index=True)
        from_min = all_other.groupby("from_name")["distance_km"].min()
        to_min   = all_other.groupby("to_name")["distance_km"].min()
        # For each emitter, find the nearest terminal and compare the distance to that terminal with the emitter's shortest other edge.
        # If the terminal is closer, add an edge from the emitter to that terminal.
        term_rows = []
        for e_idx in range(len(emitter)):
            e_name = emitter.iloc[e_idx]["name"]
            s_idx  = int(dist_es[e_idx].argmin())
            d_term = dist_es[e_idx, s_idx]
            d_other = min(from_min.get(e_name, np.inf), to_min.get(e_name, np.inf))
            if d_term < d_other:
                term_rows.append({
                    "edge_type":      "emitter_to_terminal",
                    "from_name":      e_name,
                    "from_latitude":  emit_lat[e_idx], "from_longitude": emit_lon[e_idx],
                    "to_name":        sink.iloc[s_idx]["name"],
                    "to_latitude":    sink_lat[s_idx], "to_longitude":   sink_lon[s_idx],
                    "distance_km":    d_term,
                })
        emitter_to_terminal = pd.DataFrame(term_rows, columns=cols) if term_rows else pd.DataFrame(columns=cols)

        # Guarantee every terminal has at least one emitter_to_terminal
        covered = set(emitter_to_terminal["to_name"]) if not emitter_to_terminal.empty else set()
        for s_idx in range(len(sink)):
            if sink.iloc[s_idx]["name"] not in covered:
                e_idx = int(dist_es[:, s_idx].argmin())
                emitter_to_terminal = pd.concat([emitter_to_terminal, pd.DataFrame([{
                    "edge_type":      "emitter_to_terminal",
                    "from_name":      emitter.iloc[e_idx]["name"],
                    "from_latitude":  emit_lat[e_idx], "from_longitude": emit_lon[e_idx],
                    "to_name":        sink.iloc[s_idx]["name"],
                    "to_latitude":    sink_lat[s_idx], "to_longitude":   sink_lon[s_idx],
                    "distance_km":    dist_es[e_idx, s_idx],
                }])], ignore_index=True)

    # ── terminal_to_storage: nearest terminal → each storage site ────────────
    terminal_to_storage = pd.DataFrame(columns=cols)
    if len(sink) > 0 and len(storage) > 0:
        stor_lat  = storage["latitude"].to_numpy()
        stor_lon  = storage["longitude"].to_numpy()
        dist_stor = distance(stor_lat[:, None], stor_lon[:, None], sink_lat[None, :], sink_lon[None, :])
        ts = []
        for st_idx in range(len(storage)):
            sk_idx = int(dist_stor[st_idx].argmin())
            ts.append({
                "edge_type":      "terminal_to_storage",
                "from_name":      sink.iloc[sk_idx]["name"],
                "from_latitude":  sink_lat[sk_idx], "from_longitude": sink_lon[sk_idx],
                "to_name":        storage.iloc[st_idx]["name"],
                "to_latitude":    stor_lat[st_idx], "to_longitude":   stor_lon[st_idx],
                "distance_km":    dist_stor[st_idx, sk_idx],
            })
        terminal_to_storage = pd.DataFrame(ts, columns=cols)

    # ── Combine & save ────────────────────────────────────────────────────────
    pipeline_network = pd.concat(
        [emitter_to_port, emitter_to_emitter, emitter_to_alt,
         emitter_to_terminal, terminal_to_storage],
        ignore_index=True
    )

    # Make sure all distance_km are numeric
    pipeline_network['distance_km'] = pd.to_numeric(pipeline_network['distance_km'], errors='coerce')

    # Fill 0.000001 for 'from_name' == 'to_name'
    same_name_mask = pipeline_network["from_name"] == pipeline_network["to_name"]
    pipeline_network.loc[same_name_mask, "distance_km"] = 0.000001
    # Export to excel for manual checking
    pipeline_network.to_excel(Path(output_path) / 'pipeline_network.xlsx', index=False)

    con.register('pipeline_network', pipeline_network)
    con.execute("CREATE OR REPLACE TABLE pipeline_network AS SELECT * FROM pipeline_network")
    con.close()



############# 6. Create reusable N x N matrix from database table #####################

def create_matrix(table_name, col_start, col_end, value, output_path):
    """ 
    Create a square matrix (DataFrame) from a database table
    table_name : Name of the database table to query.
    col_start : Name of the column to use for the matrix row indices.
    col_end : Name of the column to use for the matrix column indices.
    value : Name of the column to use for the matrix cell values.
    output_path : Path to save the resulting CSV file.
    """
    # Quote identifiers so custom table/column names work safely.
    def _q(identifier):
        return '"' + str(identifier).replace('"', '""') + '"'

    query = (
        f"SELECT {_q(col_start)} AS node_start, "
        f"{_q(col_end)} AS node_end, "
        f"{_q(value)} AS cell_value "
        f"FROM {_q(table_name)}"
    )

    con = duckdb.connect(DB_PATH)
    data = con.execute(query).fetchdf()
    con.close()
    # Convert cell_value to numeric, coercing errors to NaN to handle any non-numeric entries.
    data['cell_value'] = pd.to_numeric(data['cell_value'], errors='coerce')
    
    data = data.dropna(subset=['node_start', 'node_end'])

    nodes = sorted(set(data['node_start']).union(set(data['node_end'])))
    matrix = pd.DataFrame(index=nodes, columns=nodes)

    # Fill the matrix with values from the data; missing entries will remain NaN for now.
    for row in data.itertuples(index=False):
        matrix.at[row.node_start, row.node_end] = row.cell_value

    # Fill any remaining NaN values with 0
    matrix = matrix.fillna(0)

    # Ensure all matrix values are numeric
    matrix = matrix.astype(float)

    # Ensure index and columns are string type to preserve Unicode
    matrix.index = matrix.index.astype(str)
    matrix.columns = matrix.columns.astype(str)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    matrix.to_csv(output_path, index_label="NODE", encoding='utf-8')

    return matrix



############# 7. Create NodeLocations.csv from combined_selected #####################
def create_node_location(type, altitude, path_model_input):
    con = duckdb.connect(DB_PATH)
    nodes = con.execute("SELECT name, type, longitude, latitude FROM combined_selected").fetchdf()
    con.close()

    selected = nodes[nodes['type'].isin(type)]

    node_locations = selected[['name', 'longitude', 'latitude']].copy()
    node_locations.rename(columns={'longitude': 'lon', 'latitude': 'lat'}, inplace=True)
    node_locations['alt'] = altitude
    
    # Handle duplicates by keeping the row with valid (non-NULL) coordinates.
    # If all duplicates have the same coordinates, just keep the first.
    node_locations = node_locations.drop_duplicates(subset=['name'], keep='first')
    
    node_locations = node_locations.set_index('name')

    output_path = path_model_input / 'NodeLocations.csv'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Match expected model format: ;lon;lat;alt with node names in first column.
    # QUOTE_NONNUMERIC wraps string index values (node names) in quotes so names
    # containing commas are not mis-split by comma-aware viewers (e.g. Excel).
    node_locations.to_csv(output_path, sep=';', index=True, index_label='',
                          encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)







############# 8. Copy technology JSON files from adopt-net0 database  #####################
def copy_technology_from_db(technology_list, output_path):
    """
    Copy technology JSON files from the adopt_net0 database to a destination folder.
    technology_list : list of names of technologies to copy. The name must match the JSON filename (without .json extension).
    output_path : Folder where the JSON files will be copied to.
    """
    import shutil

    # The adopt_net0 template library is expected to be located at:
    template_root = Path(__file__).resolve().parent.parent / 'adopt_net0' / 'database' / 'templates' / 'technology_data'
    output_path = Path(output_path)
    # Create the output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Build a lookup of available template files by their filename.
    available = {p.stem: p for p in template_root.rglob('*.json')}

    # For each technology name in the input list, check if a corresponding JSON file exists in the template library and copy it to the output folder.
    for name in technology_list:
        if name in available:
            src = available[name]
            shutil.copy2(src, output_path / src.name)
            print(f"Copied: {src.name}  -->  {output_path}")







############# 9. Create emitter technology JSON files for industrial sectors #####################
def create_emitter_technology(input_path, output_path):
    """Create emitter technology JSON files from Excel config."""
    
    destination_folder = Path(output_path)
    df = pd.read_excel(input_path).dropna(how="all")

    # Iterate through each row of the DataFrame and create a JSON file for each technology configuration.
    # The JSON structure is based on the expected format for technology data in the adopt_net0.
    for row in df.to_dict("records"):

        data = {
            "tec_type": row["tec_type"],
            "comment": "This file is auto-generated from user input in excel file.",
            "size_min": 0,
            "size_max": row["size_max"],
            "size_is_int": 0,   
            "size_based_on": "output",
            "decommission": 0,
            "Economics": {
                "capex_model": 1,
                "unit_capex": 0,
                "opex_variable": 0,
                "opex_fixed": 0,
                "discount_rate": row["discount_rate"],
                "lifetime": int(row["lifetime"]),
                "decommission_cost": 0,
            },
            "Performance": {
                "performance_function_type": 1,
                "main_output_carrier": row["main_output_carrier"],
                "output_carrier": [row["output_carrier"]],
                "output_ratios": {},
                "emission_factor": row["emission_factor"],
                "min_part_load": 0,
                "ccs": {
                    "possible": 1,
                    "co2_concentration": row["co2_concentration"],
                    "ccs_type": "MEA_large",
                },
                "ramping_rate": -1,
                "standby_power": -1,
                "min_uptime": -1,
                "min_downtime": -1,
                "SU_time": -1,
                "SD_time": -1,
                "SU_load": -1,
                "SD_load": -1,
                "max_startups": -1,
            },
            "Units": {
                "size": "t/h",
                "output_carrier": {"CO2captured": "t/h"},
            },
        }

        filename = Path(row["filename"]).with_suffix(".json")
        output_file = destination_folder / filename

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"Created: {output_file}")





############# 10. Copy network data JSON files from adopt_net0 database #####################
def copy_network_data_from_db(network_data_list, output_path):
    """
    Copy network data JSON files from the adopt_net0 database to a destination folder.
    network_data_list : list of names of network data files to copy. The name must match the JSON filename (without .json extension).
    output_path : Folder where the JSON files will be copied to.
    """
    import shutil

    # The adopt_net0 template library is expected to be located at:
    template_root = Path(__file__).resolve().parent.parent / 'adopt_net0' / 'database' / 'templates' / 'network_data'
    output_path = Path(output_path)
    # Create the output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Build a lookup of available template files by their filename.
    available = {p.stem: p for p in template_root.rglob('*.json')}

    # For each network data name in the input list, check if a corresponding JSON file exists in the template library and copy it to the output folder.
    for name in network_data_list:
        if name in available:
            src = available[name]
            shutil.copy2(src, output_path / src.name)
            print(f"Copied: {src.name}  -->  {output_path}")



