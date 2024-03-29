import requests
import logging
import json

# init some vars to be used throughout this file
# maps agol data type to ckan data type
AGOL_CKAN_TYPE_MAP =  {
    "sqlTypeFloat": "float",
    "sqlTypeNVarchar": "text",
    "sqlTypeInteger": "int",
    "sqlTypeOther": "text",
    "sqlTypeTimestamp2": "timestamp",
    "sqlTypeDouble": "float",
    "esriFieldTypeString": "text",
    "esriFieldTypeDate": "timestamp",
    "esriFieldTypeInteger": "int",
    "esriFieldTypeOID": "int",
    "esriFieldTypeDouble": "float"
}

# list of attributes that are redundant when geometry is present in a response
DELETE_FIELDS = [
    "longitude",
    "latitude",
    "shape__length",
    "shape__area",
    "lat",
    "long",
    "lon",
    "x",
    "y",
    "index_"
]

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
        geojson = json.loads(res.text)
        # get the properties out of each returned object
        #data = [ object["properties"] for object in geojson["features"] ]
        features.extend( geojson["features"] )
        
        # prepare the next request, if needed
        overflow = "properties" in geojson and "exceededTransferLimit" in geojson["properties"] and geojson["properties"]["exceededTransferLimit"] is True
        offset = offset + len(geojson["features"])
        query_string_params["resultOffset"] = offset

    logging.info("Returned {} AGOL records".format(str(len(features)) ))
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
    
    ckan_fields = []
    
    for field in res.json()["fields"]:
        if field["name"].lower() not in DELETE_FIELDS:
            ckan_fields.append({
                "id": field["name"],
                "type": AGOL_CKAN_TYPE_MAP[field["type"]],
            })

    logging.info("Utils parsed the following fields: {}".format(ckan_fields))
        
    return ckan_fields 


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
