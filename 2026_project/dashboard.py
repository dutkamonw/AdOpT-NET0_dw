import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st
from pathlib import Path
import math


st.set_page_config(layout="wide", page_title="CO2 Network Dashboard")

PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = PROJECT_DIR / "database.duckdb"

EDGE_COLORS = {
    "emitter_to_port": [25, 156, 2, 180],
    "emitter_to_emitter": [0, 0, 0, 170],
    "emitter_to_alternative": [255, 54, 235, 170],
    "emitter_to_terminal": [5, 174, 240, 170],
    "terminal_to_storage": [8, 37, 255, 170],
}


def _format_value(value):
    if pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:,.3f}".rstrip("0").rstrip(".")
    return str(value)


def _build_hover_text(df: pd.DataFrame, excluded_cols: set[str]) -> pd.Series:
    cols = [c for c in df.columns if c not in excluded_cols]

    def _row_to_text(row):
        parts = []
        for col in cols:
            val = _format_value(row[col])
            if val == "":
                continue
            parts.append(f"<b>{col}</b>: {val}")
        return "<br/>".join(parts)

    return df.apply(_row_to_text, axis=1)


def _clean_coords(df: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
    out = df.copy()
    out[lat_col] = pd.to_numeric(out[lat_col], errors="coerce")
    out[lon_col] = pd.to_numeric(out[lon_col], errors="coerce")
    out = out.dropna(subset=[lat_col, lon_col])
    out = out[(out[lat_col].between(-90, 90)) & (out[lon_col].between(-180, 180))]
    return out


@st.cache_data
def load_data():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    pipeline = con.execute("SELECT * FROM pipeline_network").fetchdf()
    combined = con.execute("SELECT * FROM combined_selected").fetchdf()
    con.close()

    emitters = combined[combined["type"] == "emitter"].copy()
    ports = combined[combined["type"] == "port"].copy()
    storage = combined[combined["type"] == "storage"].copy()

    return pipeline, emitters, ports, storage


def prepare_pipeline_layer(
    pipeline_df: pd.DataFrame, color_by_edge_type: bool = True
) -> tuple[pd.DataFrame, pdk.Layer]:
    p = _clean_coords(pipeline_df, "from_latitude", "from_longitude")
    p = _clean_coords(p, "to_latitude", "to_longitude")

    p["path"] = p.apply(
        lambda r: [
            [r["from_longitude"], r["from_latitude"]],
            [r["to_longitude"], r["to_latitude"]],
        ],
        axis=1,
    )
    if color_by_edge_type:
        p["color"] = p["edge_type"].map(EDGE_COLORS).apply(
            lambda v: v if isinstance(v, list) else [100, 100, 100, 160]
        )
    else:
        p["color"] = [[0, 0, 0, 170]] * len(p)
        p["edge_type"] = "Pipeline"

    excluded = {
        "from_latitude",
        "from_longitude",
        "to_latitude",
        "to_longitude",
        "path",
        "color",
    }
    p["hover_text"] = _build_hover_text(p, excluded)

    layer = pdk.Layer(
        "PathLayer",
        p,
        get_path="path",
        get_color="color",
        get_width=4,
        width_min_pixels=2,
        pickable=True,
        auto_highlight=True,
    )
    return p, layer


def prepare_point_layer(df: pd.DataFrame, color: list[int], radius_col: str = None, base_radius: int = 20000, scale_factor: float = 1.0) -> tuple[pd.DataFrame, pdk.Layer]:
    """Prepare point layer with optional data-driven sizing.
    
    Args:
        df: DataFrame with latitude/longitude
        color: RGBA color
        radius_col: Column name for data-driven radius (optional)
        base_radius: Fixed radius if radius_col is None
        scale_factor: Multiplier for radius_col values
    """
    points = _clean_coords(df, "latitude", "longitude")
    points["color"] = [color] * len(points)
    
    if radius_col and radius_col in points.columns:
        # Data-driven sizing
        points["radius"] = pd.to_numeric(points[radius_col], errors="coerce").fillna(0) * scale_factor
    else:
        # Fixed sizing
        points["radius"] = base_radius

    excluded = {"latitude", "longitude", "color", "radius"}
    points["hover_text"] = _build_hover_text(points, excluded)

    layer = pdk.Layer(
        "ScatterplotLayer",
        points,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_line_color=[0, 0, 0, 180],
        line_width_min_pixels=1,
        get_radius="radius",
        pickable=True,
        auto_highlight=True,
    )
    return points, layer


def prepare_port_layer(df: pd.DataFrame, color: list[int], size: float) -> tuple[pd.DataFrame, pdk.Layer]:
    """Create triangle markers for ports using PolygonLayer."""
    ports = _clean_coords(df, "latitude", "longitude")
    
    # Create triangles for each port
    triangles = []
    for _, row in ports.iterrows():
        lon, lat = row["longitude"], row["latitude"]
        # Create equilateral triangle pointing up (adjust size in degrees)
        # Size in degrees (approximate meters to degrees conversion at mid-latitudes)
        offset = size / 111000  # roughly convert meters to degrees
        
        triangle = {
            "polygon": [
                [lon, lat + offset],  # top
                [lon - offset * 0.866, lat - offset * 0.5],  # bottom left
                [lon + offset * 0.866, lat - offset * 0.5],  # bottom right
            ],
            "color": color,
        }
        # Add all other columns for hover
        for col in ports.columns:
            if col not in ["latitude", "longitude"]:
                triangle[col] = row[col]
        triangles.append(triangle)
    
    triangle_df = pd.DataFrame(triangles)
    
    excluded = {"polygon", "color"}
    triangle_df["hover_text"] = _build_hover_text(triangle_df, excluded)
    
    layer = pdk.Layer(
        "PolygonLayer",
        triangle_df,
        get_polygon="polygon",
        get_fill_color="color",
        get_line_color=[0, 0, 0, 200],
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
    )
    return ports, layer


def prepare_storage_label_layer(storage_df: pd.DataFrame) -> pdk.Layer:
    storage = _clean_coords(storage_df, "latitude", "longitude")
    if "capacity_T" in storage.columns:
        storage["capacity_label"] = pd.to_numeric(storage["capacity_T"], errors="coerce") / 1_000_000
        storage["capacity_label"] = storage["capacity_label"].fillna(0).map(lambda x: f"{x:.1f} MtCO2")
    else:
        storage["capacity_label"] = "0.0 MtCO2"

    return pdk.Layer(
        "TextLayer",
        storage,
        get_position="[longitude, latitude]",
        get_text="capacity_label",
        get_size=20,
        get_color=[255, 0, 0, 255],
        get_angle=0,
        get_text_anchor="start",
        get_alignment_baseline="center",
        get_pixel_offset=[25, 0],
        pickable=False,
    )


st.title("CCS Network")
st.caption(f"Data source: {DB_PATH}")



# Size controls
st.sidebar.subheader("Emitter:")

emitter_size_mode = st.sidebar.selectbox(
    "Emitter size based on",
    options=["None", "emission_TPA"],
    index=0,
)
if emitter_size_mode == "emission_TPA":
    emitter_scale = st.sidebar.slider(
        "Emitter Scale (x emission_TPA)",
        min_value=0.0,
        max_value=0.04,
        value=0.01,
        step=0.001,
    )
    emitter_base_radius = 20000
else:
    emitter_scale = 1.0
    emitter_base_radius = st.sidebar.slider(
        "Emitter Point Size (None mode)",
        min_value=2000,
        max_value=60000,
        value=20000,
        step=1000,
    )

st.sidebar.markdown("**Storage:**")
storage_size_mode = st.sidebar.selectbox(
    "Storage size based on",
    options=["None", "capacity_T"],
    index=0,
)
if storage_size_mode == "capacity_T":
    storage_scale = st.sidebar.slider(
        "Storage Scale (x capacity_T)",
        min_value=0.0001,
        max_value=0.0005,
        value=0.0001,
        step=0.0001,
    )
    storage_base_radius = 20000
else:
    storage_scale = 1.0
    storage_base_radius = st.sidebar.slider(
        "Storage Point Size (None mode)",
        min_value=2000,
        max_value=60000,
        value=20000,
        step=1000,
    )

st.sidebar.markdown("**Port:**")
port_size = st.sidebar.slider("Port Triangle Size (meters)", min_value=500, max_value=20000, value=5000, step=500)

# Layer visibility controls
st.sidebar.subheader("Show/Hide Layers")
show_pipeline = st.sidebar.checkbox("Pipeline Network", value=True)
pipeline_color_by_type = st.sidebar.checkbox("Pipeline color by edge type", value=True)
show_emitters = st.sidebar.checkbox("Emitters", value=True)
show_ports = st.sidebar.checkbox("Ports", value=True)
show_storage = st.sidebar.checkbox("Storage", value=True)

pipeline_df, emitters_df, ports_df, storage_df = load_data()

# Prepare all layers
pipeline_clean, pipeline_layer = prepare_pipeline_layer(
    pipeline_df, color_by_edge_type=pipeline_color_by_type
)

emitter_radius_col = "emission_TPA" if emitter_size_mode == "emission_TPA" else None
emitters_clean, emitters_layer = prepare_point_layer(
    emitters_df,
    [214, 39, 40, 220],
    radius_col=emitter_radius_col,
    base_radius=emitter_base_radius,
    scale_factor=emitter_scale,
)

ports_clean, ports_layer = prepare_port_layer(ports_df, [16, 207, 48, 220], port_size)

storage_radius_col = "capacity_T" if storage_size_mode == "capacity_T" else None
storage_clean, storage_layer = prepare_point_layer(
    storage_df,
    [20, 31, 240, 210],
    radius_col=storage_radius_col,
    base_radius=storage_base_radius,
    scale_factor=storage_scale,
)
storage_text_layer = prepare_storage_label_layer(storage_df)

# Build layers list based on visibility
layers = []
if show_pipeline:
    layers.append(pipeline_layer)
if show_emitters:
    layers.append(emitters_layer)
if show_ports:
    layers.append(ports_layer)
if show_storage:
    layers.append(storage_layer)
    layers.append(storage_text_layer)  # Always show labels with storage points

all_lats = pd.concat(
    [
        emitters_clean["latitude"],
        ports_clean["latitude"],
        storage_clean["latitude"],
        pipeline_clean["from_latitude"],
        pipeline_clean["to_latitude"],
    ],
    ignore_index=True,
)
all_lons = pd.concat(
    [
        emitters_clean["longitude"],
        ports_clean["longitude"],
        storage_clean["longitude"],
        pipeline_clean["from_longitude"],
        pipeline_clean["to_longitude"],
    ],
    ignore_index=True,
)

if all_lats.empty or all_lons.empty:
    center_lat, center_lon = 45.0, 10.0
else:
    center_lat = float(all_lats.mean())
    center_lon = float(all_lons.mean())

view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=4, pitch=0)

tooltip = {
    "html": "{hover_text}",
    "style": {
        "backgroundColor": "#111111",
        "color": "#f5f5f5",
        "fontSize": "12px",
    },
}

deck = pdk.Deck(
    map_style="light",
    initial_view_state=view_state,
    layers=layers,
    tooltip=tooltip,
)

# In-page legend shown above the map for quick interpretation.
st.markdown("#### Legend")
if pipeline_color_by_type:
    pipeline_legend_items = [
        '<div><span style="display:inline-block; width:22px; height:0; border-top:3px solid rgba(25,156,2,0.9); margin-right:6px;"></span>Emitter to Port</div>',
        '<div><span style="display:inline-block; width:22px; height:0; border-top:3px solid rgba(0,0,0,0.9); margin-right:6px;"></span>Emitter to Emitter</div>',
        '<div><span style="display:inline-block; width:22px; height:0; border-top:3px solid rgba(255,54,235,0.9); margin-right:6px;"></span>Emitter to Alternative</div>',
        '<div><span style="display:inline-block; width:22px; height:0; border-top:3px solid rgba(5,174,240,0.9); margin-right:6px;"></span>Emitter to Terminal</div>',
        '<div><span style="display:inline-block; width:22px; height:0; border-top:3px solid rgba(8,37,255,0.9); margin-right:6px;"></span>Terminal to Storage</div>',
    ]
else:
    pipeline_legend_items = [
        '<div><span style="display:inline-block; width:22px; height:0; border-top:3px solid rgba(0,0,0,0.9); margin-right:6px;"></span>Pipeline</div>',
    ]

pipeline_legend_html = "".join(pipeline_legend_items)

st.markdown(
    f"""
<div style="display:flex; flex-wrap:wrap; gap:12px 20px; align-items:center; margin-bottom:10px;">
  <div><span style="display:inline-block; width:12px; height:12px; border-radius:50%; background-color:rgba(214,39,40,0.9); border:1px solid #000; margin-right:6px;"></span>Emitters</div>
  <div><span style="display:inline-block; width:0; height:0; border-left:7px solid transparent; border-right:7px solid transparent; border-bottom:12px solid rgba(16,207,48,0.9); margin-right:6px; vertical-align:middle;"></span>Ports</div>
  <div><span style="display:inline-block; width:12px; height:12px; border-radius:50%; background-color:rgba(20,31,240,0.9); border:1px solid #000; margin-right:6px;"></span>Storage</div>{pipeline_legend_html}
</div>
""",
    unsafe_allow_html=True,
)

st.pydeck_chart(deck, use_container_width=True)

st.markdown("### Counts")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Emitters", f"{len(emitters_clean):,}")
col2.metric("Ports", f"{len(ports_clean):,}")
col4.metric("Storage", f"{len(storage_clean):,}")
total_points = len(emitters_clean) + len(ports_clean) + len(storage_clean)
col3.metric("Total Points", f"{total_points:,}")
col5.metric("Pipeline", f"{len(pipeline_clean):,}")
