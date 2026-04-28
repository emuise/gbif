from sickle import Sickle
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import shapely
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm
import json
import time
from geopy.exc import GeocoderServiceError

# Connect to Lunaris OAI endpoint
sickle = Sickle('https://www.lunaris.ca/oai/')

def normalize_record(record):
    """
    Parses a Sickle record into a consistent dictionary, 
    extracting all possible DataCite spatial metadata.
    """
    try:
        tree = ET.fromstring(record.raw)
    except ET.ParseError:
        return None

    # Standard DataCite and OAI namespaces
    ns = {
        'datacite': 'http://datacite.org/schema/kernel-4',
        'oai': 'http://www.openarchives.org/OAI/2.0/'
    }

    # Initialize the consistent object
    normalized = {
        'id': record.header.identifier,
        'doi': None,
        'titles': [t.text for t in tree.findall('.//datacite:title', ns)],
        'places': [],
        'points': [],
        'boxes': [],
        'polygons': [],
        'rights': []
    }

    rights_elements = tree.findall('.//datacite:rights', ns)
    for r in rights_elements:
        uri = r.get('rightsURI')
        text = r.text
        normalized['rights'].append({
            'license_name': text,
            'license_uri': uri
        })

    # Extract DOI if available
    identifier = tree.find('.//datacite:identifier[@identifierType="DOI"]', ns)
    if identifier is not None:
        normalized['doi'] = identifier.text

    # Locate the geoLocations block
    geo_locations = tree.findall('.//datacite:geoLocation', ns)

    for geo in geo_locations:
        # 1. Extract Places (Text descriptions)
        place = geo.findtext('datacite:geoLocationPlace', namespaces=ns)
        if place:
            normalized['places'].append(place)

        # 2. Extract Points (Single coordinates)
        point = geo.find('datacite:geoLocationPoint', ns)
        if point is not None:
            lat = point.findtext('datacite:pointLatitude', namespaces=ns)
            lon = point.findtext('datacite:pointLongitude', namespaces=ns)
            if lat and lon:
                normalized['points'].append({'lat': float(lat), 'lon': float(lon)})

        # 3. Extract Boxes (Bounding boxes)
        box = geo.find('datacite:geoLocationBox', ns)
        if box is not None:
            try:
                normalized['boxes'].append({
                    'w': float(box.findtext('datacite:westBoundLongitude', namespaces=ns)),
                    'e': float(box.findtext('datacite:eastBoundLongitude', namespaces=ns)),
                    's': float(box.findtext('datacite:southBoundLatitude', namespaces=ns)),
                    'n': float(box.findtext('datacite:northBoundLatitude', namespaces=ns))
                })
            except (TypeError, ValueError):
                pass

        # 4. Extract Polygons (Complex shapes)
        poly = geo.find('datacite:geoLocationPolygon', ns)
        if poly is not None:
            poly_points = []
            pts = poly.findall('datacite:polygonPoint', ns)
            for p in pts:
                p_lat = p.findtext('datacite:pointLatitude', namespaces=ns)
                p_lon = p.findtext('datacite:pointLongitude', namespaces=ns)
                if p_lat and p_lon:
                    poly_points.append((float(p_lat), float(p_lon)))
            if poly_points:
                normalized['polygons'].append(poly_points)

    return normalized



def save_batch(batch_data, batch_number):

    df = pd.DataFrame(batch_data)
    # Convert to a PyArrow table
    table = pa.Table.from_pandas(df)
    # Save as a temp file
    pq.write_table(table, f'harvest/shards/harvest_batch_{batch_number}.parquet')

# Harvest using datacite to ensure we get structured coordinates
records = sickle.ListRecords(metadataPrefix='oai_datacite')

n_records = int(records.resumption_token.complete_list_size)

# Inside your loop
batch_size = 5000
current_batch = []
batch_count = 0

fullsavename = 'harvest/lunaris_full_harvest.parquet'

if(not os.path.exists(fullsavename)):


    os.makedirs(os.path.join("harvest", "shards"), exist_ok = True)

    for i in range(n_records):
        if i % 100 == 0:
            print(f"Processed {i} records")


        record = records.next()
        data = normalize_record(record)
        
        if data:
            current_batch.append(data)
            
        if len(current_batch) >= batch_size:
            save_batch(current_batch, batch_count)
            current_batch = []
            batch_count += 1
            print(f"Saved batch {batch_count}")

    save_batch(current_batch, batch_count)

# Final Step: Combine all batches into one master file
all_files = [os.path.join("harvest", "shards", f) for f in os.listdir(os.path.join("harvest", "shards")) if f.startswith('harvest_batch_')]
harvest = pa.concat_tables([pq.read_table(f) for f in all_files])
pq.write_table(harvest, fullsavename)


bcb = gpd.read_file(os.path.join("data", "shps", "bcb_wgs84.gpkg"))
bcb_geom = bcb.geometry.iloc[0]

# fig, ax = plt.subplots(figsize = (10, 10))
# bcb.plot(ax = ax, color = "lightgray", edgecolor = "white", alpha = 0.5)

shapely.prepare(bcb_geom)

# POINTS
# Create an array of 'Empty' points as a placeholder for nulls
points_geoms = np.array([shapely.Point() for _ in range(len(harvest))])

coords = harvest['points'].to_numpy()

# Only fill in the ones that have valid coordinate arrays
valid_idx = np.array([len(c) > 0 for c in coords])

# 1. Get the valid rows
valid_data = coords[valid_idx]

# 2. Extract lats and lons from the dictionaries
# We wrap them in a list of lists: [[lon1, lat1], [lon2, lat2], ...]
# Note: Shapely expects (x, y) which is (longitude, latitude)
extracted_coords = [[d[0]['lon'], d[0]['lat']] for d in valid_data]

# 3. Create the points
points_geoms = np.array([shapely.Point() for _ in range(len(harvest))])
points_geoms[valid_idx] = shapely.points(extracted_coords)

mask_points = shapely.intersects(bcb_geom, points_geoms)

# BOXES
# Initialize empty geometries
boxes_geoms = np.array([shapely.Polygon() for _ in range(len(harvest))])

raw_boxes = harvest['boxes'].to_numpy()
valid_box_idx = np.array([len(b) > 0 for b in raw_boxes])

if valid_box_idx.any():
    # Extract coordinates in order: minx (w), miny (s), maxx (e), maxy (n)
    extracted_boxes = [
        [b[0]['w'], b[0]['s'], b[0]['e'], b[0]['n']] 
        for b in raw_boxes[valid_box_idx]
    ]
    # Vectorized box creation
    boxes_geoms[valid_box_idx] = shapely.box(*np.array(extracted_boxes).T)

mask_boxes = shapely.intersects(bcb_geom, boxes_geoms)

# POLYGONS
# Initialize empty geometries
poly_geoms = np.array([shapely.Polygon() for _ in range(len(harvest))])

raw_polys = harvest['polygons'].to_numpy()
valid_poly_idx = np.array([len(p) > 0 for p in raw_polys])

if valid_poly_idx.any():
    # Swap (lat, lon) to (lon, lat) for each point in the polygon
    # p[0] accesses the first polygon in the list for that row
    formatted_polys = [
        [(pt[1], pt[0]) for pt in p[0]] 
        for p in raw_polys[valid_poly_idx]
    ]
    
    # Create polygons (this is not as easily vectorized as points/boxes)
    poly_geoms[valid_poly_idx] = [shapely.Polygon(p) for p in formatted_polys]


mask_polygons = shapely.intersects(bcb_geom, poly_geoms)

harvest_df = harvest.to_pandas()

overrides = {
    "[Untitled]; Alberta; Canada": "Alberta; Canada"
}


final_geom_mask = mask_points | mask_boxes | mask_polygons

needs_geocoding_mask = (harvest_df['places'].apply(lambda x: len(x) > 0)) & (~final_geom_mask)

leftover_places = harvest_df.loc[needs_geocoding_mask, 'places'].explode().dropna().unique()

# i am going to make a csv to manually adjust some lookups
# should save a lot of processing time
# and also be a complete pita

places_sorted = pd.Series(leftover_places, name = "places").sort_values()

places_sorted.to_csv("harvest/leftover_places.csv", index = False, encoding = "utf-8-sig")

# after here, i manually removed and adjusted many place names to work better with the geocoder
# i did this multiple times, double checking as i went to ensure that objects are geolocated
# to at least the province, within canada

updated = pd.read_csv("harvest/leftover_places-manual.csv")

to_lookup = updated['to_lookup'].unique()

to_lookup = to_lookup[to_lookup != "SKIP"]

geolocator = Nominatim(user_agent="evanmuise@gmail.com", timeout = 10)

# Set min_delay_seconds to 2
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

# --- 1. Run Geocoding with Progress Bar & Periodic Saves ---
geocoded_map = {}
save_path = "harvest/geocoded_cache.json"

if os.path.exists(save_path):
    with open(save_path, 'r') as f:
        geocoded_map = json.load(f)
    print(f"Loaded {len(geocoded_map)} existing records from cache.")
else:
    print("No cache found. Starting from scratch.")

# Filter leftover_places to only those NOT already in our map
places_to_process = [p for p in to_lookup if p not in geocoded_map]

print(f"Resuming: {len(places_to_process)} unique locations remaining...")

# --- 1. Run Geocoding ---
for i, name in enumerate(tqdm(places_to_process, total=len(places_to_process))):
    success = False
    retries = 0
    max_retries = 3

    while not success and retries < max_retries:
        try:
            
            location = geocode(name)
            print(name)
            print(location)
            
            # API responded. Save the result (either coordinates or None if not found)
            geocoded_map[name] = {"name": location.raw['display_name'], "lat": location.latitude, "lon": location.longitude} if location else None
            success = True
            
        except GeocoderServiceError as e:
            # Service error (Rate limit/Timeout). DO NOT save to geocoded_map.
            retries += 1
            wait_time = 60 * retries
            print(f"\nService error for '{name}': {e}. Sleeping {wait_time}s...")
            time.sleep(wait_time)
            
        except Exception as e:
            # Hard error (Logic/Query). Save as None so we don't try this specific string again.
            print(f"\nUnexpected error for '{name}': {e}")
            geocoded_map[name] = None
            success = True 

    # If we exhausted retries and never succeeded, just skip this name for now.
    # It won't be in geocoded_map, so it will be in 'places_to_process' next run.
    if not success:
        print(f"\nSkipping '{name}' after {max_retries} failed attempts. Will retry on next script run.")
        continue

    # Periodic backup
    if i % 50 == 0:
        with open(save_path, 'w') as f:
            json.dump(geocoded_map, f)

# Final save
with open(save_path, 'w') as f:
    json.dump(geocoded_map, f)


geocoded_df = pd.DataFrame({
    'to_lookup': list(geocoded_map.keys()),
    'geocoded_values': list(geocoded_map.values())
})

pd.merge(updated, geocoded_df, on = "to_lookup", how = "left").to_csv("harvest/leftover_geocoded.csv", index = False, encoding = "utf-8-sig")

geocoded_df.to_csv("harvest/leftover_geocoded.csv", index = False, encoding = "utf-8-sig")

# --- 2. Identify which names actually overlap BC ---
# We use a set for O(1) lookup speed later
bc_confirmed_names = set()

for name, data in geocoded_map.items():
    if data:
        # Note: Shapely uses (Longitude, Latitude)
        pt = shapely.geometry.Point(data['lon'], data['lat'])
        if bcb_geom.contains(pt):
            bc_confirmed_names.add(name)

print(f"Found {len(bc_confirmed_names)} location strings that map to BC.")

# --- 3. Join back to the Original DataFrame ---

# Create the mask: Check if any item in the 'places' list is in our confirmed set
mask_geocoded = harvest_df['places'].apply(
    lambda x: any(p in bc_confirmed_names for p in x) if x else False
)

# Combine with your previous masks (ensure they are all same length)
# Note: mask_points, mask_boxes, etc. should be aligned with harvest_df
final_bc_mask = final_geom_mask | mask_geocoded

# --- 4. Final Result ---
filtered_harvest = harvest_df[final_bc_mask].copy()

print(f"Total records in BC after all filters: {len(filtered_harvest)}")

# Optional: Add a column showing WHY it was kept (for debugging)
filtered_harvest['keep_reason'] = np.select(
    [mask_points[final_bc_mask], mask_boxes[final_bc_mask], mask_geocoded[final_bc_mask]],
    ['point_overlap', 'box_overlap', 'place_name_geocoded'],
    default='polygon_overlap'
)
