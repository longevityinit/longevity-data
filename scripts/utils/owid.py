import requests

def download_owid_chart_data(chart_slug: str, etag: str = None):
    """
    Downloads CSV and JSON metadata. 
    Returns None if the server returns 304 Not Modified.
    Otherwise, returns (csv_bytes, raw_metadata_dict, new_etag).
    """
    base_url = f"https://ourworldindata.org/grapher/{chart_slug}"
    headers = {'If-None-Match': etag} if etag else {}
    
    print(f"Fetching data from {base_url}.csv...")
    csv_response = requests.get(f"{base_url}.csv", headers=headers, timeout=30)

    # If the data hasn't changed, stop here and save bandwidth
    if csv_response.status_code == 304:
        return None

    csv_response.raise_for_status()

    print(f"Fetching metadata from {base_url}.metadata.json...")
    meta_response = requests.get(f"{base_url}.metadata.json", timeout=30)
    meta_response.raise_for_status()
    
    new_etag = csv_response.headers.get('ETag')
    return csv_response.content, meta_response.json(), new_etag