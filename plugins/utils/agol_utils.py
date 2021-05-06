import requests


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


def get_fields(endpoint):
    res = requests.get(f"{endpoint}&resultRecordCount=1").json()
    fields = res.get("fields")

    return fields


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
