# Install & Import required libraries
# pip install streamlit folium streamlit-folium pandas shapely


import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
from shapely.geometry import Point, shape
import json
from pyproj import Proj, transform

# Import dataframes
df_eea = pd.read_csv("export/df_eea_clipped.csv")
df_climate_trace = pd.read_csv("export/df_climate_trace_clipped.csv")
df_storage = pd.read_csv("export/df_storage.csv")

# Import OGIM layer (GeoJson)
gdf_pipelines = gpd.read_file('export/OGIM_pipelines.geojson')
gdf_platforms = gpd.read_file('export/OGIM_platforms.geojson')
gdf_terminals = gpd.read_file('export/OGIM_LNG_facilities.geojson')

# Copy and prepare EEA dataframe
df_eea_prep = df_eea.copy()
df_eea_prep = df_eea_prep.rename(columns={'pointGeometryLat': 'lat','pointGeometryLon': 'lon'})
df_eea_prep['source_id'] = df_eea_prep['Facility_INSPIRE_ID']
df_eea_prep['source_name'] = df_eea_prep.get('nameOfFeature', 'Unknown')
df_eea_prep['subsector'] = df_eea_prep.get('mainActivityCode', 'Unknown')


# Copy and prepare storage dataframe
df_storage_prep = df_storage.copy()
# Drop anaomaly caoacity values where >= 1000 MT
df_storage_prep['TOTAL_CAPACITY_BASE_MT'] = pd.to_numeric(df_storage_prep['TOTAL_CAPACITY_BASE_MT'], errors='coerce')
df_storage_prep.loc[df_storage_prep['TOTAL_CAPACITY_BASE_MT'] >= 1000, 'TOTAL_CAPACITY_BASE_MT'] = pd.NA
# Renaming Storage dataframe columns and preparing for mapping
df_storage_prep['source_id'] = df_storage_prep['DATA_SOURCE'].astype(str)
df_storage_prep['source_name'] = df_storage_prep['NAME'].astype(str)
df_storage_prep['subsector'] = df_storage_prep['TYPE'].astype(str)
df_storage_prep['lat'] = df_storage_prep['lat'].astype(float)
df_storage_prep['lon'] = df_storage_prep['lon'].astype(float)
df_storage_prep['emissions_tco2'] = 0  # Storage sites don't have emissions
df_storage_prep['CONFIDENCE_TIER'] = df_storage_prep['CONFIDENCE_TIER'].astype(int)
df_storage_prep["RESERVOIR_CAT"] = pd.to_numeric(
    df_storage_prep["RESERVOIR_CAT"], errors="coerce"
).fillna(0).astype(int)


##############################################################################################################

# Dashboard configuration
st.set_page_config(layout="wide", page_title="CO2 Emissions Sources")

# Initialize session state
if 'sync_table' not in st.session_state:
    st.session_state.sync_table = False
if 'eea_codes' not in st.session_state:
    st.session_state.eea_codes = None
if 'ct_subsectors' not in st.session_state:
    st.session_state.ct_subsectors = None
if 'show_storage' not in st.session_state:
    st.session_state.show_storage = True
if 'storage_confidence_tiers' not in st.session_state:
    st.session_state.storage_confidence_tiers = None
if 'storage_reservoir_cats' not in st.session_state:
    st.session_state.storage_reservoir_cats = None

# Color
EEA_COLOR_MAP = {
    "red": ["2(a)", "2(c)", "2(c)(i)", "2(d)", "3(c)", "3(c)(i)", "3(c)(ii)", "3(c)(iii)", "5(b)"],
    "darkblue": ["4(a)", "4(a)(i)", "4(a)(ii)", "4(a)(iii)", "4(a)(iv)", "4(a)(vi)", "4(a)(viii)"],
    "darkorange": ["1(b)"],
    "darkgreen": ["1(a)", "1(c)"], 
    "darkgrey": ["1(d)", "1(e)"]
}

CLIMATE_TRACE_COLOR_MAP = {
    "cement": "pink",
    "iron-and-steel": "pink",
    "petrochemical-steam-cracking": "lightblue",
    "oil-and-gas-production": "orange",
    "electricity-generation": "lightgreen",
    "oil-and-gas-refining": "lightgreen"
}


STORAGE_COLOR_MAP = {
    0: "#636363", 
    1: "#0F0189",  
    2: "#2922F4",  
    3: "#00B3FF",  
    4: "#41B5F8E2",   
    5: "#46E8F3E1"   
}

def eea_color(code):
    for color, codes in EEA_COLOR_MAP.items():
        if code in codes:
            return color
    return "black"

def ct_color(subsector):
    return CLIMATE_TRACE_COLOR_MAP.get(subsector, "black")

def storage_color(reservoir_cat):
    return STORAGE_COLOR_MAP.get(reservoir_cat, "gray")

def storage_confidence_tier_color(confidence_tier):
    return STORAGE_COLOR_MAP.get(confidence_tier, "gray")


##############################################################################################################
# Sidebar

# Legend in sidebar
with st.sidebar.expander("Legend", expanded=True):
    st.markdown("""
    <style>
    .legend-item { margin: 5px 0; }
    .color-circle { 
        display: inline-block; 
        width: 15px; 
        height: 15px; 
        border-radius: 50%; 
        margin-right: 8px;
        vertical-align: middle;
    }
    </style>
    
    <b>EEA Dataset</b><br>
    <div class="legend-item"><span class="color-circle" style="background-color: red;"></span>Cement/Steel/Waste | 2,3,5</div>
    <div class="legend-item"><span class="color-circle" style="background-color: darkblue;"></span>Petrochem | 4(a)</div>
    <div class="legend-item"><span class="color-circle" style="background-color: darkorange;"></span>Gasification | 1(b)</div>
    <div class="legend-item"><span class="color-circle" style="background-color: darkgreen;"></span>Refining/Power | 1(a), 1(c)</div>
        <div class="legend-item"><span class="color-circle" style="background-color: darkgrey;"></span>Coke&Coal | 1(d), 1(e)</div>
    <br>
    <b>Climate Trace Dataset</b><br>
    <div class="legend-item"><span class="color-circle" style="background-color: pink;"></span>Cement / Iron & Steel</div>
    <div class="legend-item"><span class="color-circle" style="background-color: lightblue;"></span>Petrochemical</div>
    <div class="legend-item"><span class="color-circle" style="background-color: orange;"></span>Oil & Gas Production</div>
    <div class="legend-item"><span class="color-circle" style="background-color: lightgreen;"></span>Refining/Electricity</div>
    <br>
    <b>Storage Dataset - By Reservoir Category</b><br>
    <div class="legend-item"><span class="color-circle" style="background-color: #0F0189;"></span>Category 1</div>
    <div class="legend-item"><span class="color-circle" style="background-color: #2922F4;"></span>Category 2</div>
    <div class="legend-item"><span class="color-circle" style="background-color: #00B3FF;"></span>Category 3</div>
    <div class="legend-item"><span class="color-circle" style="background-color: #41B5F8E2;"></span>Category 4</div>
    <br>
    <b>Storage Dataset - By Confidence Tier</b><br>
    <div class="legend-item"><span class="color-circle" style="background-color: #0F0189;"></span>Tier 1</div>
    <div class="legend-item"><span class="color-circle" style="background-color: #2922F4;"></span>Tier 3</div>
    <div class="legend-item"><span class="color-circle" style="background-color: #00B3FF;"></span>Tier 4</div>
    <div class="legend-item"><span class="color-circle" style="background-color: #41B5F8E2;"></span>Tier 5</div>
    """, unsafe_allow_html=True)


# Sector filters
st.sidebar.header("Filters")

# EEA Activity Code Filter
with st.sidebar.expander("EEA Main Activity Code", expanded=True):
    all_eea_codes = sorted(df_eea_prep["mainActivityCode"].dropna().unique())
    
    if st.session_state.eea_codes is None:
        st.session_state.eea_codes = all_eea_codes
    
    eea_codes_selected = []
    for code in all_eea_codes:
        if st.checkbox(code, value=True, key=f"eea_{code}"):
            eea_codes_selected.append(code)
    
    st.session_state.eea_codes = eea_codes_selected

# Climate Trace Subsector Filter
with st.sidebar.expander("Climate Trace Subsector", expanded=True):
    all_ct_subsectors = sorted(df_climate_trace["subsector"].dropna().unique())
    
    if st.session_state.ct_subsectors is None:
        st.session_state.ct_subsectors = all_ct_subsectors
    
    ct_subsectors_selected = []
    for subsector in all_ct_subsectors:
        if st.checkbox(subsector, value=True, key=f"ct_{subsector}"):
            ct_subsectors_selected.append(subsector)
    
    st.session_state.ct_subsectors = ct_subsectors_selected

# Storage Dataset Filter
with st.sidebar.expander("Storage Dataset", expanded=True):
    show_storage = st.checkbox("Show Storage Sites", value=True, key="show_storage_checkbox")
    st.session_state.show_storage = show_storage
    
    if show_storage:
        # Color mode selector
        if 'storage_color_mode' not in st.session_state:
            st.session_state.storage_color_mode = "RESERVOIR_CAT"
        
        st.markdown("**Color by:**")
        storage_color_mode = st.radio(
            "Select coloring mode",
            options=["RESERVOIR_CAT", "CONFIDENCE_TIER"],
            key="storage_color_mode_radio",
            label_visibility="collapsed"
        )
        st.session_state.storage_color_mode = storage_color_mode
        
        st.markdown("**Confidence Tier**")
        all_confidence_tiers = sorted(df_storage_prep['CONFIDENCE_TIER'].dropna().unique())
        
        if st.session_state.storage_confidence_tiers is None:
            st.session_state.storage_confidence_tiers = all_confidence_tiers
        
        confidence_tiers_selected = []
        for tier in all_confidence_tiers:
            if st.checkbox(f"Tier {int(tier)}", value=True, key=f"storage_tier_{int(tier)}"):
                confidence_tiers_selected.append(int(tier))
        
        st.session_state.storage_confidence_tiers = confidence_tiers_selected
        
        st.markdown("**Reservoir Category**")
        all_reservoir_cats = sorted(df_storage_prep['RESERVOIR_CAT'].dropna().unique())
        
        if st.session_state.storage_reservoir_cats is None:
            st.session_state.storage_reservoir_cats = all_reservoir_cats
        
        reservoir_cats_selected = []
        for cat in all_reservoir_cats:
            if st.checkbox(f"Category {int(cat)}", value=True, key=f"storage_cat_{int(cat)}"):
                reservoir_cats_selected.append(int(cat))
        
        st.session_state.storage_reservoir_cats = reservoir_cats_selected

st.sidebar.markdown("---")

# Emission cut off filter
min_emission = st.sidebar.number_input(
    "Cut-off emissions (tCO2 / year)",
    value=100_000,
    step=50_000
)

# Circle size scaling factor
radius_scale = st.sidebar.slider(
    "Emission Circle size scaling factor",
    min_value=0.5,
    max_value=10.0,
    value=1.5,
    step=0.1
)

# Storage size scaling factor
storage_radius_scale = st.sidebar.slider(
    "Storage Circle size scaling factor",
    min_value=0.5,
    max_value=10.0,
    value=2.0,
    step=0.1
)

st.sidebar.markdown("---")


##########################################################################################################
# Apply filters to dataframes

df_eea_f = df_eea_prep[
    (df_eea_prep["mainActivityCode"].isin(st.session_state.eea_codes)) &
    (df_eea_prep["emissions_tco2"] >= min_emission)
].copy()

df_climate_trace_f = df_climate_trace[
    (df_climate_trace["subsector"].isin(st.session_state.ct_subsectors)) &
    (df_climate_trace["emissions_tco2"] >= min_emission)
].copy()

df_eea_f['dataset'] = 'EEA'
df_climate_trace_f['dataset'] = 'Climate Trace'

# Apply filters to storage data
if st.session_state.show_storage:
    df_storage_f = df_storage_prep[
        (df_storage_prep['CONFIDENCE_TIER'].isin(st.session_state.storage_confidence_tiers)) &
        (df_storage_prep['RESERVOIR_CAT'].isin(st.session_state.storage_reservoir_cats))
    ].copy()
    df_storage_f['dataset'] = 'Storage'
else:
    df_storage_f = pd.DataFrame()

df_map = pd.concat([df_eea_f, df_climate_trace_f, df_storage_f], ignore_index=True)



##########################################################################################################
# Map

def scaled_radius(emission):
    return max(3, (emission / 1e6) * radius_scale)

m = folium.Map(location=[45, 10], zoom_start=4, tiles="cartodbpositron")

# Add drawing tools
draw = Draw(
    export=True,
    draw_options={
        'polyline': False,
        'rectangle': True,
        'polygon': True,
        'circle': False,
        'marker': False,
        'circlemarker': False,
    },
    edit_options={'edit': True}
)
draw.add_to(m)

# Add measurement tool
folium.plugins.MeasureControl(position='bottomleft', primary_length_unit='kilometers').add_to(m)

# Create separate layers for EEA, Climate Trace, Storage, and OGIM
eea_layer = folium.FeatureGroup(name="EEA")
ct_layer = folium.FeatureGroup(name="Climate Trace")
storage_layer = folium.FeatureGroup(name="Storage")
pipelines_layer = folium.FeatureGroup(name="Oil & Gas Pipelines")
platforms_layer = folium.FeatureGroup(name="Oil & Gas Platforms")
terminals_layer = folium.FeatureGroup(name="LNG Facilities")

# Add EEA markers
for _, r in df_eea_f.iterrows():
    folium.CircleMarker(
        location=[r.lat, r.lon],
        radius=scaled_radius(r.emissions_tco2),
        color=eea_color(r.mainActivityCode),
        fill=True,
        fill_opacity=0.8,
        popup=f"""
        <b>{r.source_name}</b><br>
        Source ID: {r.source_id}<br>
        Sector: {r.mainActivityName}<br>
        Activity: {r.mainActivityCode}<br>
        Emissions: {r.emissions_tco2:,.0f} tCO2/yr
        """
    ).add_to(eea_layer)

# Add Climate Trace markers
for _, r in df_climate_trace_f.iterrows():
    folium.CircleMarker(
        location=[r.lat, r.lon],
        radius=scaled_radius(r.emissions_tco2),
        color=ct_color(r.subsector),
        fill=True,
        fill_opacity=0.7,
        popup=f"""
        <b>{r.source_name}</b><br>
        Source ID: {r.source_id}<br>
        Sector: {r.sector}<br>
        Subsector: {r.subsector}<br>
        Emissions: {r.emissions_tco2:,.0f} tCO2/yr
        """
    ).add_to(ct_layer)

# Add Storage markers
for _, r in df_storage_f.iterrows():
    # Skip rows with missing coordinates
    if pd.isna(r.lat) or pd.isna(r.lon):
        continue
    
    # Get capacity value for sizing
    capacity = r.get('TOTAL_CAPACITY_BASE_MT', 0)
    if pd.isna(capacity) or capacity == 0:
        capacity = 1  # Default minimum  
    
    confidence_tier = int(r.get('CONFIDENCE_TIER', 1))
    reservoir_cat = int(r.get('RESERVOIR_CAT', 1))
    
    # Calculate radius based on capacity directly
    storage_radius = max(5, (capacity / 10) * storage_radius_scale)
    
    # Determine color based on selected mode
    if st.session_state.storage_color_mode == "CONFIDENCE_TIER":
        marker_color = storage_confidence_tier_color(confidence_tier)
        color_label = f"Tier {confidence_tier}"
    else:  # RESERVOIR_CAT
        marker_color = storage_color(reservoir_cat)
        color_label = f"Category {reservoir_cat}"
    
    # Prepare storage details
    name = r.get('NAME', 'N/A')
    status = r.get('STATUS', 'N/A')
    type_val = r.get('TYPE', 'N/A')
    total_capacity = r.get('TOTAL_CAPACITY_BASE_MT', 'N/A')
    remaining_capacity = r.get('REMAINING_CAPACITY_MT', 'N/A')
    remaining_ratio = r.get('REMAINING_RATIO', 'N/A')
    
    folium.CircleMarker(
        location=[r.lat, r.lon],
        radius=storage_radius,
        color=marker_color,
        fill=True,
        fill_opacity=0.7,
        popup=f"""
        <b>{name}</b><br>
        Type: {type_val}<br>
        Status: {status}<br>
        Confidence Tier: {confidence_tier}<br>
        Reservoir Category: {reservoir_cat}<br>
        Total Capacity (Base): {total_capacity} MT<br>
        Remaining Capacity: {remaining_capacity} MT<br>
        Remaining Ratio: {remaining_ratio}
        """
    ).add_to(storage_layer)

# Add OGIM layers to map
if not gdf_pipelines.empty:
    for _, row in gdf_pipelines.iterrows():
        try:
            if row.geometry.geom_type == 'LineString':
                folium.PolyLine(
                    locations=[(coord[1], coord[0]) for coord in row.geometry.coords],
                    color='black',
                    weight=1,
                    opacity=0.7,
                    popup=f"<b>Pipeline</b><br>{row.get('properties', 'Pipeline')}"
                ).add_to(pipelines_layer)
        except:
            pass

if not gdf_platforms.empty:
    for _, row in gdf_platforms.iterrows():
        try:
            if row.geometry.geom_type == 'Point':
                fac_name = row.get('FAC_NAME', 'N/A')
                fac_type = row.get('FAC_TYPE', 'N/A')
                fac_status = row.get('FAC_STATUS', 'N/A')
                operator = row.get('OPERATOR', 'N/A')
                popup_text = f"""
                <b>{fac_name}</b><br>
                Type: {fac_type}<br>
                Status: {fac_status}<br>
                Operator: {operator}
                """
                folium.RegularPolygonMarker(
                    location=[row.geometry.y, row.geometry.x],
                    number_of_sides=3,
                    radius=4,
                    color='grey',
                    fill_color='grey',
                    fill_opacity=0.8,
                    popup=popup_text
                ).add_to(platforms_layer)
        except:
            pass

if not gdf_terminals.empty:
    for _, row in gdf_terminals.iterrows():
        try:
            if row.geometry.geom_type == 'Point':
                fac_name = row.get('FAC_NAME', 'N/A')
                fac_type = row.get('FAC_TYPE', 'N/A')
                fac_status = row.get('FAC_STATUS', 'N/A')
                operator = row.get('OPERATOR', 'N/A')
                popup_text = f"""
                <b>{fac_name}</b><br>
                Type: {fac_type}<br>
                Status: {fac_status}<br>
                Operator: {operator}
                """
                lon, lat = row.geometry.x, row.geometry.y
                # Create small rectangle around point (500m buffer approx)
                rect_size = 0.05
                folium.Rectangle(
                    bounds=[[lat-rect_size, lon-rect_size], [lat+rect_size, lon+rect_size]],
                    color='black',
                    fill=True,
                    fill_color='black',
                    fill_opacity=0.7,
                    weight=1,
                    popup=popup_text
                ).add_to(terminals_layer)
        except:
            pass

eea_layer.add_to(m)
ct_layer.add_to(m)
storage_layer.add_to(m)
pipelines_layer.add_to(m)
platforms_layer.add_to(m)
terminals_layer.add_to(m)
folium.LayerControl(collapsed=False).add_to(m)

###################################################################################################
# Dashboard Layout
st.title("CO2 Emission Sources")

# Display map and capture drawing
st.subheader("Map")
map_output = st_folium(m, height=600, width=None, returned_objects=["all_drawings"])

# Extract drawn shapes
drawn_shapes = map_output.get("all_drawings", [])

# Filter by drawn shapes
df_filtered = df_map.copy()
if drawn_shapes:
    st.info(f"Drawing detected: {len(drawn_shapes)} shape(s) drawn on map")
    
    points_in_shapes = []
    for shape_data in drawn_shapes:
        try:
            geom = shape(shape_data["geometry"])
            for idx, row in df_map.iterrows():
                point = Point(row['lon'], row['lat'])
                if geom.contains(point):
                    points_in_shapes.append(idx)
        except:
            continue
    
    if points_in_shapes:
        df_filtered = df_map.loc[list(set(points_in_shapes))].copy()
        st.success(f"Found {len(df_filtered)} sources within drawn area")


st.markdown("### Number of points on map")

# Total points
total_points = len(df_filtered)

# Total per dataset
dataset_counts = (
    df_filtered
    .groupby("dataset")
    .size()
    .rename("count")
    .reset_index()
)

# Per sector (EEA: mainActivityCode, Climate Trace: subsector)
sector_counts = (
    df_filtered
    .assign(sector_group=lambda d: d["sector"].fillna(d.get("subsector")))
    .groupby(["dataset", "sector_group"])
    .size()
    .rename("count")
    .reset_index()
)

# Display totals
st.markdown(f"**Total points on map:** {total_points:,}")

st.markdown("**By dataset:**")
st.dataframe(dataset_counts, use_container_width=True, hide_index=True)

st.markdown("**By sector / subsector:**")
st.dataframe(
    sector_counts.sort_values("count", ascending=False),
    use_container_width=True,
    hide_index=True
)

st.markdown("---")

# Table
st.subheader("Data Table")

# Sync button
col1, col2 = st.columns([3, 1])
with col1:
    if st.button("Sync Table with Filters & Map Selection"):
        st.session_state.sync_table = True
        st.rerun()
with col2:
    if st.button("Reset to All Data"):
        st.session_state.sync_table = False
        st.rerun()

# Prepare table
if st.session_state.sync_table:
    df_table = df_filtered.copy()
    st.info("Table is synced with current filters and map selection")
else:
    df_all = pd.concat([
        df_eea_prep.assign(dataset="EEA"),
        df_climate_trace.assign(dataset="Climate Trace"),
        df_storage_prep.assign(dataset="Storage")
    ], ignore_index=True)
    df_table = df_all.copy()
    st.info("Table shows all data (click 'Sync' to apply filters)")

# Define columns to display
display_cols = ["dataset", "source_id", "source_name", "subsector", "emissions_tco2", "TOTAL_CAPACITY_BASE_MT", "lat", "lon"]
available_cols = [col for col in display_cols if col in df_table.columns]

# Display table
st.dataframe(
    df_table[available_cols].sort_values("emissions_tco2", ascending=False).reset_index(drop=True),
    use_container_width=True,
    height=400
)

# Summary
st.write(f"Showing {len(df_table):,} sources")

# Export button
if st.button("Export Table to CSV"):
    import os
    export_path = os.path.join("export", "export_from_dashboard.csv")
    df_table[available_cols].to_csv(export_path, index=False)
    st.success(f"Data saved to {export_path}")