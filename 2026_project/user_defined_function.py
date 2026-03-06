########### This files contains user-defined functions, including: ##########
### Note: these functions are called in 1_data_preprocessing.py ###
# 1. ETL pipeline for the raw data into duckdb database
# 2. Data selection for case study
# 3. Combines all seleted emiiters, storage, and ports data
# 4. Create shipping route based on selected ports



import pandas as pd
import duckdb
from pyproj import Proj
import glob
import datetime as dt
import pycountry
import geopandas as gpd
from pathlib import Path
from shapely.geometry import LineString
from scgraph.geographs.marnet import marnet_geograph

# Database path relative to this module's location
DB_PATH = str(Path(__file__).resolve().parent / 'database.duckdb')

############ 1. ETL pipeline for loading the raw data into duckdb database ################

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
    # Import all Climate TRACE csv file in the folder
    climate_trace = glob.glob(file_path_climate_trace + "/*.csv")
    climate_trace = pd.concat((pd.read_csv(file, low_memory=False) for file in climate_trace), ignore_index=True)
    
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
    con = duckdb.connect(DB_PATH)
    combined = con.execute("SELECT name, iso2, latitude, longitude, year, emission_TPA, subsector, data_source FROM eea UNION ALL SELECT name, iso2, latitude, longitude, year, emission_TPA, subsector, data_source FROM climate_trace").fetchdf()
    con.register('emitters_all', combined)
    con.execute("CREATE OR REPLACE TABLE emitters_all AS SELECT * FROM emitters_all")
    con.close()


## ----- ETL for CO2 Storage data -----
def etl_co2_storage(file_path_co2_storage):
    # Import CO2 Storage excel file in raw folder
    co2_storage = pd.read_excel(file_path_co2_storage)
    
    ### Transformation ###
    # Convert x, y, and TOTAL_CAPACITY_BASE_MT to numeric
    co2_storage['x'] = pd.to_numeric(co2_storage['x'], errors='coerce')
    co2_storage['y'] = pd.to_numeric(co2_storage['y'], errors='coerce')
    co2_storage['TOTAL_CAPACITY_BASE_MT'] = pd.to_numeric(co2_storage['TOTAL_CAPACITY_BASE_MT'], errors='coerce')
    # Covert 'TOTAL_CAPACITY_BASE_MT' in MtCO2 to 'capacity' in tCO2
    co2_storage['capacity'] = co2_storage['TOTAL_CAPACITY_BASE_MT'] * 1000000
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
    # Import port excel file in raw folder
    port = pd.read_csv(file_path_port)
    
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
    # Import area from geojson
    area = gpd.read_file(file_path_area)
    # Get data from co2_storage table 
    con = duckdb.connect(DB_PATH)
    co2_storage = con.execute("SELECT * FROM co2_storage").fetchdf()
    con.close()

    ### Filtering ###
    # Drop missing coordinate and capacity data
    co2_storage = co2_storage.dropna(subset=['latitude', 'longitude', 'capacity'])
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
    co2_storage_selected = co2_storage_selected[co2_storage_selected['capacity'] >= storage_cutoff]
    # Sum capacity by 'group' and keep other data from the row with largest capacity
    co2_storage_selected = co2_storage_selected.sort_values('capacity', ascending=False)
    agg_dict = {col: 'first' for col in co2_storage_selected.columns if col != 'capacity'}
    agg_dict['capacity'] = 'sum'
    co2_storage_selected = co2_storage_selected.groupby('group', as_index=False).agg(agg_dict)
    
    ### Store the selected co2 storage data in database.duckdb, if exists, replace it ###
    # Drop geometry column (not supported by DuckDB)
    co2_storage_selected_df = co2_storage_selected.drop(columns=['geometry', 'index_right'], errors='ignore').copy()
    co2_storage_selected_df['type'] = 'storage'
    # Keep only necessary columns
    co2_storage_selected_df = co2_storage_selected_df[['group', 'name', 'iso2', 'latitude', 'longitude', 'capacity', 'type', 'data_source']]

    con = duckdb.connect(DB_PATH)
    con.register('co2_storage_selected', co2_storage_selected_df)
    con.execute("CREATE OR REPLACE TABLE co2_storage_selected AS SELECT * FROM co2_storage_selected")
    con.close() 

## ----- Select ports ------
def select_ports():
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
def combine_all_selected():
    con = duckdb.connect(DB_PATH)
    query = """
    SELECT
        CAST(NULL AS VARCHAR) AS "group",
        name,
        iso2,
        latitude,
        longitude,
        emission_TPA,
        CAST(NULL AS DOUBLE) AS capacity,
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
        capacity,
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
        CAST(NULL AS DOUBLE) AS capacity,
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

############# 4. Create ship route data for selected ports #####################
def create_ship_routes():
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
    # Load into database
    con.register('ship_routes', ship_routes)
    con.execute("CREATE OR REPLACE TABLE ship_routes AS SELECT * FROM ship_routes")
    con.close()


############# 5. Create reusable square matrix from database table #####################
def create_matrix(table_name, col_start, col_end, value, output_path):
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

    data = data.dropna(subset=['node_start', 'node_end'])

    nodes = sorted(set(data['node_start']).union(set(data['node_end'])))
    matrix = pd.DataFrame(index=nodes, columns=nodes)

    for row in data.itertuples(index=False):
        matrix.at[row.node_start, row.node_end] = row.cell_value

    for node in nodes:
        matrix.at[node, node] = 0

    matrix = matrix.fillna(0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    matrix.to_csv(output_path, index_label="NODE")

    return matrix



