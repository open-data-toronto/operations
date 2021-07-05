import requests
import logging


def convert_dtypes_to_ckan(agol_fields):
    esri_to_ckan_map = {
        "esriFieldTypeInteger": "int",
        "esriFieldTypeSmallInteger": "int",
        "esriFieldTypeDouble": "float",
        "esriFieldTypeSingle": "float",
        "esriFieldTypeString": "text",
        "esriFieldTypeDate": "timestamp",
        "esriFieldTypeGeometry": "text",
        "esriFieldTypeOID": "int",
        "esriFieldTypeBlob": None,
        "esriFieldTypeGlobalID": None,
        "esriFieldTypeRaster": None,
        "esriFieldTypeGUID": None,
        "esriFieldTypeXML": None,
    }

    ckan_fields = []

    for f in agol_fields:
        esri_type = f["type"]

        assert (
            esri_type in esri_to_ckan_map and esri_to_ckan_map[esri_type] is not None
        ), f"No data type map specified for {esri_type}"

        ckan_fields.append({"id": f["name"], "type": esri_to_ckan_map[esri_type]})

    return ckan_fields

def get_features(query_url):
    logging.info(f"  getting features from AGOL")

    features = []
    overflow = True
    offset = 0
    query_string_params = {
        "where": "1=1",
        "outFields": "*",
        "outSR": 4326,
        "f": "geojson",
        "resultType": "standard",
        "resultOffset": offset,
    }
    
    while overflow is True:
        # As long as there are more records to get on another request
        
        # make a request url 
        params = "&".join([ f"{k}={v}" for k,v in query_string_params.items() ])
        url = "https://" + "/".join([ url_part for url_part in f"{query_url}/query?{params}".replace("https://", "").split("/") if url_part ])
        logging.info(url)

        # make the request
        res = requests.get(url)
        assert res.status_code == 200, f"Status code response: {res.status_code}"
        
        # parse the response
        geojson = json.loads(res)
        # get the properties out of each returned object
        #data = [ object["properties"] for object in geojson["features"] ]
        features.append( geojson["features"] )
        
        # prepare the next request, if needed
        overflow = "properties" in geojson and "exceededTransferLimit" in geojson["properties"] and geojson["properties"]["exceededTransferLimit"] is True
        offset = offset + len(geojson["features"])
        query_string_params["resultOffset"] = offset

    logging.info("Returned {} AGOL records with these attributes: {}".format(str(len(features)), str(features[0].keys()) ))
    return features

def get_data(endpoint):
    res = requests.get(endpoint).json()

    features = res.get("features")
    data = [f["attributes"] for f in features]

    if res.get("exceededTransferLimit") is True:
        paginate = True
        resultOffset = len(data)

        while paginate:
            res = requests.get(f"{endpoint}&resultOffset={resultOffset}").json()
            features = res.get("features")
            data.extend([f["attributes"] for f in features])

            resultOffset += len(data)
            if res.get("exceededTransferLimit") is None:
                paginate = False

    return data

def get_fields(query_url):
    print(f"  getting fields from AGOL")

    query_string_params = {
        "where": "1=1",
        "outFields": "*",
        "outSR": 4326,
        "f": "json",
        "resultRecordCount": 1
    }
    
    params = "&".join([ f"{k}={v}" for k,v in query_string_params.items() ])
    url = "https://" + "/".join([ url_part for url_part in f"{query_url}/query?{params}".replace("https://", "").split("/") if url_part ])
    res = requests.get(url)
    assert res.status_code == 200, f"Status code response: {res.status_code}"
    
    fields = json.loads(res)["features"][0].keys() 

    return fields


#def get_fields(endpoint):
#    res = requests.get(f"{endpoint}&resultRecordCount=1").json()
#    fields = res.get("fields")
#
#    return fields


def remove_geo_columns(df):
    data = df.copy()

    geo_columns = [
        "lat",
        "latitude",
        "lon",
        "long",
        "longitude",
        "x",
        "y",
        "shape__area",
        "shape_area",
        "shape_length",
        "shape__length",
    ]

    data = data.drop(
        [c for c in data.columns.values if c.lower() in geo_columns], axis=1
    )

    return data
