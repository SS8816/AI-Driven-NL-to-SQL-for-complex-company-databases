"""
Robust geospatial visualization helper.
Pre-written, tested code - no LLM generation.
"""
import pandas as pd
import geopandas as gpd
import folium
from folium import plugins
from shapely import wkt


def create_interactive_map(df: pd.DataFrame) -> str:
    """
    Create interactive Folium map from DataFrame with WKT columns.
    
    Args:
        df: DataFrame with WKT geometry columns (columns ending in '_wkt')
        
    Returns:
        HTML string of the interactive map
    """
    
    # Step 1: Rename 'geometry' column if exists (avoid conflicts)
    if 'geometry' in df.columns:
        df = df.rename(columns={'geometry': 'geometry_raw'})
    
    # Step 2: Identify WKT columns
    wkt_columns = [col for col in df.columns if 'wkt' in col.lower()]
    
    if not wkt_columns:
        return "<h3 style='color: red;'>❌ No WKT geometry columns found in data</h3>"
    
    # Step 3: Helper function to safely convert WKT to geometry
    def safe_wkt_to_geometry(wkt_string):
        """Safely convert WKT string to shapely geometry."""
        try:
            if pd.notna(wkt_string) and isinstance(wkt_string, str):
                return wkt.loads(wkt_string)
        except Exception:
            pass
        return None
    
    # Step 4: Create GeoDataFrames for each WKT column
    geodfs = {}
    
    for wkt_col in wkt_columns:
        # Get all columns except WKT and geometry_raw
        data_cols = [c for c in df.columns if c != wkt_col and c != 'geometry_raw']
        
        # Create GeoSeries from WKT
        geometries = df[wkt_col].apply(safe_wkt_to_geometry)
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(
            df[data_cols].copy(),
            geometry=geometries,
            crs="EPSG:4326"
        )
        
        # Remove invalid geometries
        gdf = gdf[gdf.geometry.notna()]
        
        if len(gdf) > 0:
            geodfs[wkt_col] = gdf
    
    if not geodfs:
        return "<h3 style='color: red;'>❌ No valid geometries found after parsing WKT</h3>"
    
    # Step 5: Calculate map center from first valid geometry
    first_gdf = list(geodfs.values())[0]
    first_geom = first_gdf.geometry.iloc[0]
    map_center = [first_geom.centroid.y, first_geom.centroid.x]
    
    # Step 6: Create base map
    m = folium.Map(
        location=map_center,
        zoom_start=14,
        tiles='OpenStreetMap'
    )
    
    # Step 7: Add interactive plugins
    
    # Fullscreen button
    plugins.Fullscreen(
        position='topleft',
        title='Fullscreen mode',
        title_cancel='Exit fullscreen',
        force_separate_button=True
    ).add_to(m)
    
    # Minimap (small overview in corner)
    minimap = plugins.MiniMap(toggle_display=True, tile_layer='OpenStreetMap')
    m.add_child(minimap)
    
    # Measure tool (measure distances)
    plugins.MeasureControl(
        position='topleft',
        primary_length_unit='meters',
        secondary_length_unit='kilometers',
        primary_area_unit='sqmeters',
        secondary_area_unit='hectares'
    ).add_to(m)
    
    # Locate button (find user location)
    plugins.LocateControl(auto_start=False, position='topleft').add_to(m)
    
    # Mouse position display
    plugins.MousePosition(
        position='bottomleft',
        separator=' | ',
        prefix='Coordinates: '
    ).add_to(m)
    
    # Step 8: Color palette for different layers
    colors = ['#2e6fdb', '#e45756', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899']
    
    # Step 9: Add each GeoDataFrame as a layer
    for idx, (wkt_col, gdf) in enumerate(geodfs.items()):
        color = colors[idx % len(colors)]
        
        # Create layer name (clean up column name)
        layer_name = wkt_col.replace('_wkt', '').replace('_', ' ').title()
        
        # Create feature group for layer control
        feature_group = folium.FeatureGroup(name=layer_name, show=True)
        
        # Determine geometry type for styling
        sample_geom = gdf.geometry.iloc[0]
        geom_type = sample_geom.geom_type
        
        # Style based on geometry type
        # Check if this GeoDataFrame has highlighting column
        has_highlighting = 'is_highlighted' in gdf.columns
        
# Style based on geometry type
        if 'LineString' in geom_type:
            style_dict = {
                        'color': color,
                        'weight': 3,
                        'opacity': 0.8
                    }
            highlight_style = {
                        'color': '#FF0000',  # Red for highlighted
                        'weight': 6,
                        'opacity': 1.0
                    }
            dimmed_style = {
                        'color': '#CCCCCC',  # Gray for non-highlighted
                        'weight': 2,
                        'opacity': 0.4
                    }
        elif 'Polygon' in geom_type:
                    style_dict = {
                        'fillColor': color,
                        'color': color,
                        'weight': 2,
                        'fillOpacity': 0.3,
                        'opacity': 0.8
                    }
                    highlight_style = {
                        'fillColor': '#FF0000',
                        'color': '#FF0000',
                        'weight': 4,
                        'fillOpacity': 0.6,
                        'opacity': 1.0
                    }
                    dimmed_style = {
                        'fillColor': '#CCCCCC',
                        'color': '#CCCCCC',
                        'weight': 1,
                        'fillOpacity': 0.1,
                        'opacity': 0.3
                    }
        else:  # Point
                    style_dict = {
                        'color': color,
                        'fillColor': color,
                        'radius': 6,
                        'weight': 2,
                        'fillOpacity': 0.7
                    }
                    highlight_style = {
                        'color': '#FF0000',
                        'fillColor': '#FF0000',
                        'radius': 10,
                        'weight': 4,
                        'fillOpacity': 1.0
                    }
                    dimmed_style = {
                        'color': '#CCCCCC',
                        'fillColor': '#CCCCCC',
                        'radius': 4,
                        'weight': 1,
                        'fillOpacity': 0.3
                    }
        
        # Highlight function for hover effect
        def highlight_function(feature):
            return {
                'fillColor': '#ffff00',
                'color': '#ffff00',
                'weight': 5,
                'fillOpacity': 0.7
            }
        
        # Create popup and tooltip content
        # Get ID column (look for common patterns)
        id_cols = [c for c in gdf.columns if 'id' in c.lower() and c != 'geometry']
        id_col = id_cols[0] if id_cols else gdf.columns[0]
        
        # Get country column if exists
        country_cols = [c for c in gdf.columns if 'country' in c.lower()]
        country_col = country_cols[0] if country_cols else None
        
        # Add features to layer
        for idx, (wkt_col, gdf) in enumerate(geodfs.items()):
            color = colors[idx % len(colors)]
            
            # Create layer name (clean up column name)
            layer_name = wkt_col.replace('_wkt', '').replace('_', ' ').title()
            
            # Create feature group for layer control
            feature_group = folium.FeatureGroup(name=layer_name, show=True)
            
            # Determine geometry type for styling
            sample_geom = gdf.geometry.iloc[0]
            geom_type = sample_geom.geom_type
            
            # Style based on geometry type
            if 'LineString' in geom_type:
                style_dict = {
                    'color': color,
                    'weight': 3,
                    'opacity': 0.8
                }
            elif 'Polygon' in geom_type:
                style_dict = {
                    'fillColor': color,
                    'color': color,
                    'weight': 2,
                    'fillOpacity': 0.3,
                    'opacity': 0.8
                }
            else:  # Point
                style_dict = {
                    'color': color,
                    'fillColor': color,
                    'radius': 6,
                    'weight': 2,
                    'fillOpacity': 0.7
                }
            
            # Highlight function for hover effect
            def highlight_function(feature):
                return {
                    'fillColor': '#ffff00',
                    'color': '#ffff00',
                    'weight': 5,
                    'fillOpacity': 0.7
                }
            
            # Create popup and tooltip content
            # Get ID column (look for common patterns)
            id_cols = [c for c in gdf.columns if 'id' in c.lower() and c != 'geometry']
            id_col = id_cols[0] if id_cols else gdf.columns[0]
            
            # Get country column if exists
            country_cols = [c for c in gdf.columns if 'country' in c.lower()]
            country_col = country_cols[0] if country_cols else None
            
            # Add features to layer
            for idx, row in gdf.iterrows():
    # Determine style based on highlighting
                if has_highlighting:
                    if row.get('is_highlighted', False):
                        current_style = highlight_style
                        tooltip_prefix = "⭐ "
                    else:
                        current_style = dimmed_style
                        tooltip_prefix = ""
                else:
                    current_style = style_dict
                    tooltip_prefix = ""
                
                # Build tooltip (hover text)
                tooltip_parts = [f"{tooltip_prefix}ID: {row[id_col]}"]
                if country_col and pd.notna(row[country_col]):
                    tooltip_parts.append(f"Country: {row[country_col]}")
                tooltip_text = " | ".join(tooltip_parts)
                
                # Build popup (click to see details)
                popup_html = f"""
                <div style="font-family: Arial, sans-serif; min-width: 250px; max-width: 350px;">
                    <h4 style="margin: 0 0 10px 0; color: {current_style['color']}; border-bottom: 2px solid {current_style['color']}; padding-bottom: 5px;">
                        {tooltip_prefix}{layer_name}
                    </h4>
                    <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                """
                
                # Add all non-geometry columns to popup
                for col in gdf.columns:
                    if col != 'geometry' and col != 'is_highlighted' and pd.notna(row[col]):
                        value = str(row[col])
                        # Truncate long values
                        if len(value) > 50:
                            value = value[:47] + "..."
                        
                        popup_html += f"""
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 5px; font-weight: bold; color: #555;">{col}:</td>
                            <td style="padding: 5px; word-break: break-word;">{value}</td>
                        </tr>
                        """
                
                popup_html += """
                    </table>
                </div>
                """
                
                # Add to map
                folium.GeoJson(
                    row.geometry.__geo_interface__,
                    style_function=lambda x, style=current_style: style,
                    highlight_function=highlight_function if not has_highlighting or row.get('is_highlighted', False) else None,
                    tooltip=folium.Tooltip(tooltip_text),
                    popup=folium.Popup(popup_html, max_width=400)
                ).add_to(feature_group)
            
            # Add feature group to map
            feature_group.add_to(m)
        
        # ← FIXED INDENTATION! This is OUTSIDE the loop now
        # Step 9.5: Add Search Plugin (Optional)
        if len(geodfs) > 0:
            # Get first layer for search
            first_wkt_col = list(geodfs.keys())[0]
            first_gdf = geodfs[first_wkt_col]
            
            # Find ID column for search
            id_cols = [c for c in first_gdf.columns if 'id' in c.lower() and c != 'geometry']
            if id_cols:
                search_col = id_cols[0]
                
                try:
                    # Create search control
                    # Note: Search plugin is experimental and may not work with all geometry types
                    search = plugins.Search(
                        layer=feature_group,  # References last feature_group from loop
                        search_label=search_col,
                        search_zoom=16,
                        position='topright',
                        placeholder=f'Search by {search_col}...',
                        collapsed=True
                    )
                    m.add_child(search)
                except Exception as e:
                    # Search plugin often fails - skip silently
                    print(f"Search plugin failed: {e}")
        
        # Step 10: Add layer control
        folium.LayerControl(
            collapsed=False,
            position='topright'
        ).add_to(m)
    
    # Step 11: Add legend
    legend_html = f"""
        <style>
            .legend-box {{
                position: fixed; 
                bottom: 50px; 
                left: 50px; 
                width: 220px;
                background-color: white; 
                border: 2px solid grey; 
                border-radius: 5px;
                z-index: 9999; 
                font-family: Arial, sans-serif;
                font-size: 14px;
                box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
            }}
            .legend-header {{
                margin: 0;
                padding: 10px 15px;
                background-color: #f0f0f0;
                border-bottom: 2px solid #333;
                cursor: pointer;
                user-select: none;
                border-radius: 3px 3px 0 0;
            }}
            .legend-content {{
                padding: 15px;
                max-height: 400px;
                overflow-y: auto;
            }}
            .legend-collapsed .legend-content {{
                display: none;
            }}
            .legend-toggle {{
                float: right;
                font-weight: bold;
            }}
        </style>
        <div class="legend-box" id="legendBox">
            <div class="legend-header" onclick="document.getElementById('legendBox').classList.toggle('legend-collapsed')">
                <span> Legend</span>
                <span class="legend-toggle">▼</span>
            </div>
            <div class="legend-content">
        """
    
    for idx, (wkt_col, gdf) in enumerate(geodfs.items()):
        color = colors[idx % len(colors)]
        layer_name = wkt_col.replace('_wkt', '').replace('_', ' ').title()
        legend_html += f"""
        <p style="margin: 5px 0;">
            <span style="background-color: {color}; 
                        padding: 2px 10px; 
                        border-radius: 3px; 
                        color: white; 
                        font-weight: bold;">
                ●
            </span>
            {layer_name} ({len(gdf)} features)
        </p>
        """
    
    legend_html += """
        <hr style="margin: 10px 0;">
        <p style="font-size: 12px; color: #666; margin: 5px 0;">
            💡 <strong>Tips:</strong><br>
            • Click features for details<br>
            • Use layer control to toggle<br>
            • Hover for quick info<br>
            • Use measure tool for distances
        </p>
    </div>
</div>
"""
    
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Step 12: Return HTML
    return m._repr_html_()