"""Functions yielding rows (as dicts) for custom read jobs"""

import json
import requests
import logging
import csv
import io
from datetime import datetime, date, timedelta
from airflow.models import Variable
from utils import misc_utils
import openpyxl



def test_reader():
    data = [
        {"row1": 1, "row2": "text", "row3": 2.11},
        {"row1": 1, "row2": "text", "row3": 3.1},
        {"row1": 2, "row2": "text", "row3": 3.1},
        {"row1": 1, "row2": "text", "row3": 3.1},
    ]

    for i in data:
        yield i


def bodysafe():
    # custom logics to produce a flat list of dicts from the nested bodysafe input

    url = "https://secure.toronto.ca/opendata/bs_od/full_list/v1?format=json"
    user_key = Variable.get("secure_toronto_opendata_USER_KEY")
    srv_key = Variable.get("bodysafe_secure_toronto_opendata_SRV_KEY")

    headers = {
        "SRV-KEY": srv_key,
        "USER-KEY": user_key,
    }

    raw_input = json.loads(requests.get(url, headers=headers).text)

    for item in raw_input:
        for service in item["json"].get("services", None) or []:
            # if theres no inspections, append the data to the output
            if not service.get("inspections", None):
                yield {
                    "estId": item["json"]["estId"],
                    "estName": item["json"]["estName"],
                    "addrFull": item["json"]["addrFull"],
                    "srvType": service["srvType"],
                    "insStatus": None,
                    "insDate": None,
                    "observation": None,
                    "infCategory": None,
                    "defDesc": None,
                    "infType": None,
                    "actionDesc": None,
                    "OutcomeDate": None,
                    "OutcomeDesc": None,
                    "fineAmount": None,
                    "geometry": json.dumps(
                        {
                            "type": "Point",
                            "coordinates": [item["json"]["lon"], item["json"]["lat"]],
                        }
                    ),
                }

            for inspection in service.get("inspections", None) or []:
                # if theres no infractions, append the data to the output
                if not inspection.get("infractions", None):
                    yield {
                        "estId": item["json"]["estId"],
                        "estName": item["json"]["estName"],
                        "addrFull": item["json"]["addrFull"],
                        "srvType": service["srvType"],
                        "insStatus": inspection["insStatus"],
                        "insDate": inspection["insDate"],
                        "observation": inspection["observation"],
                        "infCategory": None,
                        "defDesc": None,
                        "infType": None,
                        "actionDesc": None,
                        "OutcomeDate": None,
                        "OutcomeDesc": None,
                        "fineAmount": None,
                        "geometry": json.dumps(
                            {
                                "type": "Point",
                                "coordinates": [
                                    item["json"]["lon"],
                                    item["json"]["lat"],
                                ],
                            }
                        ),
                    }

                for infraction in inspection.get("infractions", None) or []:
                    # if theres no infractions details, append the data to the output
                    if not infraction.get("infDtl", None):
                        yield {
                            "estId": item["json"]["estId"],
                            "estName": item["json"]["estName"],
                            "addrFull": item["json"]["addrFull"],
                            "srvType": service["srvType"],
                            "insStatus": inspection["insStatus"],
                            "insDate": inspection["insDate"],
                            "observation": inspection["observation"],
                            "infCategory": infraction["infCategory"],
                            "defDesc": None,
                            "infType": None,
                            "actionDesc": None,
                            "OutcomeDate": None,
                            "OutcomeDesc": None,
                            "fineAmount": None,
                            "geometry": json.dumps(
                                {
                                    "type": "Point",
                                    "coordinates": [
                                        item["json"]["lon"],
                                        item["json"]["lat"],
                                    ],
                                }
                            ),
                        }

                    for detail in infraction["infDtl"]:
                        # append infraction detail info, as available, to the output
                        yield {
                            "estId": item["json"]["estId"],
                            "estName": item["json"]["estName"],
                            "addrFull": item["json"]["addrFull"],
                            "srvType": service["srvType"],
                            "insStatus": inspection["insStatus"],
                            "insDate": inspection["insDate"],
                            "observation": inspection["observation"],
                            "infCategory": infraction["infCategory"],
                            "defDesc": detail.get("defDesc", None),
                            "infType": detail.get("infType", None),
                            "actionDesc": detail.get("actionDesc", None),
                            "OutcomeDate": detail.get("outcomeDate", None),
                            "OutcomeDesc": detail.get("outcomeDesc", None),
                            "fineAmount": detail.get("fineAmount", None),
                            "geometry": json.dumps(
                                {
                                    "type": "Point",
                                    "coordinates": [
                                        item["json"]["lon"],
                                        item["json"]["lat"],
                                    ],
                                }
                            ),
                        }


def toronto_beaches_water_quality():
    url = "https://secure.toronto.ca/opendata/adv_od/beach_results/v1?format=json&startDate=2000-01-01&endDate=9999-01-01"
    user_key = Variable.get("secure_toronto_opendata_USER_KEY")
    srv_key = Variable.get(
        "toronto-beaches-water-quality_secure_toronto_opendata_SRV_KEY"
    )

    headers = {
        "SRV-KEY": srv_key,
        "USER-KEY": user_key,
    }

    raw_input = json.loads(requests.get(url, headers=headers).text)

    for item in raw_input:
        yield {
            "beachId": item["beachId"],
            "beachName": item["beachName"],
            "siteName": item["siteName"],
            "collectionDate": item["collectionDate"],
            "eColi": item["eColi"],
            #"comments": item["comments"],
            "geometry": json.dumps(
                {"type": "Point", "coordinates": [item["lon"], item["lat"]]}
            ),
        }


def toronto_beaches_observations():
    url = "https://secure.toronto.ca/opendata/adv_od/route_observations/v1?format=json"
    user_key = Variable.get("secure_toronto_opendata_USER_KEY")
    srv_key = Variable.get(
        "toronto-beaches-water-quality_secure_toronto_opendata_SRV_KEY"
    )

    headers = {
        "SRV-KEY": srv_key,
        "USER-KEY": user_key,
    }

    raw_input = json.loads(requests.get(url, headers=headers).text)

    for item in raw_input:
        yield {
            "dataCollectionDate": item["dataCollectionDate"],
            "beachName": item["beachName"],
            "windSpeed": item["wind_speed"],
            "windDirection": item["windDirection"],
            "airTemp": item["airTemp"],
            "rain": item["rain"],
            "rainAmount": item["rainAmount"],
            "waterTemp": item["waterTemp"],
            "waterFowl": item["waterFowl"],
            "waveAction": item["waveAction"],
            "waterClarity": item["waterClarity"],
            "turbidity": item["turbidity"],
        }


def _tobids_get_records(entity, filters):
    # input list of dicts containing raw records from tobids, output formatted records
    chunk_size = 1000

    has_more = True
    offset = 0
    all_raw = []

    while has_more:
        url = (
            f"https://secure.toronto.ca/c3api_data/v2/DataAccess.svc/pmmd_solicitations/{entity}"
            + f"?$format=application/json;odata.metadata=none&$count=true&$top={chunk_size}&$skip={offset}"
            + f"&$filter={filters}"
        )
        logging.info(f"Requesting data from {url}")
        records = json.loads(requests.get(url).content)
        all_raw += records["value"]
        
        logging.info(f"Processing in batch, start from {offset}")
        has_more = len(all_raw) < records["@odata.count"]
        offset += chunk_size

    return all_raw


def _tobids_parse_records(records, field_mapping, awarded_field=None):
    # input list of dicts containing raw records from tobids, output formatted records
    for record in records:
        clean_record = {}
        for in_field, out_field in field_mapping.items():
            if in_field in record.keys():
                if isinstance(record[in_field], list):
                    clean_record[out_field] = ",".join(record[in_field])
                else:
                    clean_record[out_field] = record[in_field].replace("\n", " ")
            else:
                clean_record[out_field] = None
        
        if awarded_field:
            counter = 0
            for entry in record[awarded_field]:
            # Use counter to create unique key for each record
                counter += 1
        
                # address
                address_pieces = []
                for address_piece in ["street", "city", "province", "country", "postalCode"]:
                    if record.get(address_piece, False):
                        address_pieces.append(record[address_piece])
                clean_record["Supplier Address"] = ", ".join(address_pieces)

                for in_field, out_field in field_mapping.items():
                    if in_field in entry.keys():
                        clean_record[out_field] = entry[in_field]

                yield clean_record


        elif not awarded_field:
            yield clean_record


def tobids_all_open_solicitations():
    entity = "feis_solicitation_published"
    filters = "Ready_For_Posting%20eq%20%27Yes%27%20and%20Status%20eq%20%27Open%27%20"
    raw = _tobids_get_records(entity, filters)

    field_mapping = {
        'Solicitation_Document_Number': 'Document Number',
        'Solicitation_Document_Type': 'RFx (Solicitation) Type',
        'Solicitation_Form_Type': 'NOIP (Notice of Intended Procurement) Type',
        'Issue_Date': 'Issue Date',
        'Closing_Date': 'Submission Deadline',
        'High_Level_Category': 'High Level Category',
        'Solicitation_Document_Description': 'Solicitation Document Description',
        'Client_Division': 'Division',
        'Buyer_Name': 'Buyer Name',
        'Buyer_Email': 'Buyer Email',
        'Buyer_Phone_Number': 'Buyer Phone Number',
        "Wards": 'Wards',
    }

    yield from _tobids_parse_records(raw, field_mapping)


def tobids_awarded_contracts():

    entity = "feis_solicitation_published"
    filters = "Ready_For_Posting%20eq%20%27Yes%27%20and%20Status%20eq%20%27Awarded%27%20"
    raw = _tobids_get_records(entity, filters)

    field_mapping = {
        "Solicitation_Document_Number": "Document Number",
        "Solicitation_Document_Type": "RFx (Solicitation) Type",
        "High_Level_Category": "High Level Category",
        "Successful_Bidder": "Successful Supplier",
        "Award_Amount": "Award",
        "Date_Awarded": "Award Authority Obtained Date",
        "Client_Division": "Division",
        "Buyer_Name": "Buyer Name",
        "Buyer_Email": "Buyer Email",
        "Buyer_Phone_Number": "Buyer Phone Number",
        "Solicitation_Document_Description": "Solicitation Document Description",
        "Wards": "Wards",
    }
        
    yield from _tobids_parse_records(raw, field_mapping, "Awarded_Suppliers")


def tobids_non_competitive_contracts():

    entity = "feis_non_competitive_published"
    filters = "Ready_For_Posting%20eq%20%27Yes%27%20and%20Status%20eq%20%27Awarded%27%20"
    raw = _tobids_get_records(entity, filters)

    field_mapping = {
        "Non_Competitive_Reference_Number":"Workspace Number",
        "Non_Competitive_Reason":"Reason",
        "Latest_Date_Awarded":"Contract Date",
        "Successful_Bidder":"Supplier Name",
        "Award_Amount":"Contract Amount",
        "Client_Division":"Division",
        "Supplier Address":"Supplier Address",
        "Wards": "Wards"
    }

    yield from _tobids_parse_records(raw, field_mapping, "Awarded_Suppliers")

    """
    has_more = True
    offset = 0
    total_records = []
    # filter only keep 18 months data
    cut_date = str(date.today() + relativedelta(months=-18))
    
    while has_more:
        url = (
            "https://secure.toronto.ca/c3api_data/v2/DataAccess.svc/pmmd_solicitations/feis_non_competitive_published?$format=application/json;odata.metadata=none&$count=true&$skiptoken="
            + str(offset)
            + "&$filter=Ready_For_Posting%20eq%20%27Yes%27%20
            #and%20Status%20eq%20%27Awarded%27%20
            + "and%20Awarded_Cancelled%20eq%20%27No%27%20"#and%20Latest_Date_Awarded%20gt%20"
            #+ cut_date
            + "&$orderby=Latest_Date_Awarded%20desc"
        )
        logging.info(f"Requesting data from {url}")
        records = json.loads(requests.get(url).content)
        total_records += records["value"]
        
        logging.info(f"Processing in batch, start from {offset}")
        has_more = has_more = records.get("@odata.nextLink", False) #len(total_records) < records["@odata.count"]
        offset += 100
    
    logging.info(f"A total of {len(total_records)} records.")

    fields = [
        "id",
        "Non_Competitive_Reference_Number",
        "Non_Competitive_Reason",
        "Latest_Date_Awarded",
    ]

    for record in total_records:
        clean_record = {}
        for field in fields:
            if field in record.keys():
                clean_record[field] = record[field]
            else:
                clean_record[field] = None

        # clean text before insert into ckan datastore
        clean_record["Client_Division"] = ",".join(record["Client_Division"])
        clean_record["Successful_Bidder"] = record["Awarded_Suppliers"][0][
            "Successful_Bidder"
        ]
        clean_record["Award_Amount"] = record["Awarded_Suppliers"][0]["Award_Amount"]

        awarded_supplier_address = ["street", "city", "province", "postalCode", "country"]
        full_address_list = []
        for item in awarded_supplier_address:
            addr = record["Awarded_Suppliers"][0][item] if item in record["Awarded_Suppliers"][0].keys() else ""
            if addr:
                full_address_list.append(addr)

        clean_record["Supplier Address"] = (
            ";".join(full_address_list) if full_address_list else ""
        )

        yield {
            "unique_id": clean_record["id"],
            "Workspace Number": clean_record["Non_Competitive_Reference_Number"],
            "Reason": clean_record["Non_Competitive_Reason"],
            "Contract Date": clean_record["Latest_Date_Awarded"],
            "Supplier Name": clean_record["Successful_Bidder"],
            "Contract Amount": clean_record["Award_Amount"],
            "Division": clean_record["Client_Division"],
            "Supplier Address": clean_record["Supplier Address"]
        }
"""

def washroom_facilities():
    
    # get source data
    locations_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/arcgis/rest/services/COT_PFR_washroom_drinking_water_source/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
    locations = json.loads(requests.get(locations_url).text)["features"]

    status_url = "https://www.toronto.ca/data/parks/live/washroom_allupdates.json"
    statuses = json.loads(requests.get(status_url).text)["locations"]

    for status in statuses:
        for location in locations:
            # if asset ids match, combine into dict and yield it
            if status["AssetID"] == location["properties"]["asset_id"]:
                location["properties"].update(status)
                
                yield misc_utils.parse_geometry_from_row(location["properties"])


def parks_drinking_fountains():
    # get source data
    locations_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/arcgis/rest/services/COT_PFR_washroom_drinking_water_source/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
    locations = json.loads(requests.get(locations_url).text)["features"]

    status_url = "https://www.toronto.ca/data/parks/live/dws_allupdates.json"
    statuses = json.loads(requests.get(status_url).text)["locations"]

    for status in statuses:
        for location in locations:
            # if asset ids match, combine into dict and yield it
            if status["AssetID"] == location["properties"]["asset_id"]:
                location["properties"].update(status)
                
                yield misc_utils.parse_geometry_from_row(location["properties"])


def dinesafe():
    from io import StringIO
    import hashlib
    url = "https://secure.toronto.ca/opendata/ds_od/inpections/v2?format=json"
    user_key = Variable.get("secure_toronto_opendata_USER_KEY")
    srv_key = Variable.get("dinesafe_secure_toronto_opendata_SRV_KEY")

    headers = {
        "SRV-KEY": srv_key,
        "USER-KEY": user_key,
    }

    raw_input = json.loads(requests.get(url, headers=headers).text)

    indices = []

    for item in raw_input:
        
        address = f'{item["address"]} {item["unit"]} {item["postal"]}'
        
        for inspection in item.get("inspections", None) or []:
            # if theres no infractions, append the data to the output
            if not inspection.get("infractions", None):

                unique_composite_key = (
                    address
                    + "_"
                    + inspection["inspectionDate"]
                ).encode("utf-8")

                # create hash value
                hash_value = hashlib.md5(unique_composite_key)
                
                # skip duplicates if they exist
                if hash_value.hexdigest() in indices:
                    continue
                indices.append(hash_value.hexdigest())
                    
                yield {
                    "unique_id": hash_value.hexdigest(),
                    "estId": item["estId"],
                    "oldEstId": item["oldEstId"],
                    "estName": item["estName"],
                    "address": address,
                    "phone": item["phone"],
                    "inspectionStatus": inspection["inspectionStatus"],
                    "inspectionDate": inspection["inspectionDate"],                    
                    "observation": inspection["observation"],                    
                    "typeDesc": None,                                        
                    "deficiencyDesc": None,
                    "severity": None,
                    "OutcomeDate": None,
                    "OutcomeDesc": None,
                    "amountFined": None,
                    "latitude": item["latitude"],
                    "longitude": item["longitude"],

                }

            for infraction in inspection.get("infractions", None) or []:
                # append infraction detail info, as available, to the output
                # add a unique primary key as required by datastore_upsert
                unique_composite_key = (
                    address
                    + "_"
                    + inspection["inspectionDate"]
                    + "_"
                    + infraction["typeDesc"]
                ).encode("utf-8")
                                
                # create hash value
                hash_value = hashlib.md5(unique_composite_key)
                
                # skip duplicates if they exist
                if hash_value.hexdigest() in indices:
                    continue
                indices.append(hash_value.hexdigest())
                
                # if theres no infractions details, append the data to the output
                if not infraction.get("prosecutions", None):
                                 
                    yield {
                        "unique_id": hash_value.hexdigest(),
                        "estId": item["estId"],
                        "oldEstId": item["oldEstId"],
                        "estName": item["estName"],
                        "address": address,
                        "phone": item["phone"],
                        "inspectionStatus": inspection["inspectionStatus"],
                        "inspectionDate": inspection["inspectionDate"],                        
                        "observation": inspection["observation"],                        
                        "typeDesc": infraction["typeDesc"],                                                
                        "deficiencyDesc": infraction["deficiencyDesc"],
                        "severity": infraction["severity"],
                        "OutcomeDate": None,
                        "OutcomeDesc": None,
                        "amountFined": None,                        
                        "latitude": item["latitude"],
                        "longitude": item["longitude"],
                    }

                for prosecution in infraction.get("prosecutions", None) or []:
                    # append infraction prosecution info, as available, to the output
                    # add a unique primary key as required by datastore_upsert
                    
                    yield {
                        "unique_id": hash_value.hexdigest(),
                        "estId": item["estId"],
                        "oldEstId": item["oldEstId"],
                        "estName": item["estName"],
                        "address": address,
                        "phone": item["phone"],
                        "inspectionStatus": inspection["inspectionStatus"],
                        "inspectionDate": inspection["inspectionDate"],                        
                        "observation": inspection["observation"],                                         
                        "typeDesc": infraction["typeDesc"],                                                
                        "deficiencyDesc": infraction["deficiencyDesc"],
                        "severity": infraction["severity"],                                              
                        "OutcomeDate": prosecution.get("outcomeDate", None),
                        "OutcomeDesc": prosecution.get("outcomeDesc", None),
                        "amountFined": prosecution.get("amountFined", None),                        
                        "latitude": item["latitude"],
                        "longitude": item["longitude"],
                    }


def swimsafe():
    from io import StringIO
    import hashlib
    url = "https://secure.toronto.ca/opendata/ss_od/full_list/v1?format=json"
    user_key = Variable.get("secure_toronto_opendata_USER_KEY")
    srv_key = Variable.get("swimsafe_secure_toronto_opendata_SRV_KEY")

    headers = {
        "SRV-KEY": srv_key,
        "USER-KEY": user_key,
    }

    raw_input = json.loads(requests.get(url, headers=headers).text)

    indices = []

    for json_item in raw_input:
        facility = json_item["json"]
        for establishment in facility.get("establishments", None) or []:
            for inspection in establishment.get("inspections", None) or []:
                # if theres no infractions, append the data to the output
                if not inspection.get("infractions", None):

                    unique_composite_key = (
                        establishment["estName"]
                        + "_"
                        + inspection["insDate"]
                    ).encode("utf-8")

                    # create hash value
                    hash_value = hashlib.md5(unique_composite_key)
                    
                    # skip duplicates if they exist
                    if hash_value.hexdigest() in indices:
                        continue
                    indices.append(hash_value.hexdigest())
                        
                    yield {
                        "unique_id": hash_value.hexdigest(),
                        #"estId": facility["estId"],
                        "facilityName": facility["facilityName"],
                        "address": facility["addrFull"],
                        "estName": establishment["estName"],
                        "accessType": establishment["accessType"],
                        "type": establishment["type"],
                        "insStatus": inspection["insStatus"],
                        "insDate": inspection["insDate"],                    
                        "observation": inspection["observation"],
                        "infCategory": None,
                        "defDesc": None,
                        "infType": None,
                        "actionDesc": None,
                        "geometry": json.dumps(
                            {
                                "type": "Point",
                                "coordinates": [
                                    float(facility["lon"]),
                                    float(facility["lat"]),
                                ],
                            }
                        ),
                    }

                for infraction in inspection.get("infractions", None) or []:
                    # append infraction detail info, as available, to the output
                    # add a unique primary key as required by datastore_upsert

                    for detail in infraction.get("infDtl", None) or []:
                        # append infraction detail info, as available, to the output
                        # add a unique primary key as required by datastore_upsert
                        unique_composite_key = (
                            establishment["estName"]
                            + "_"
                            + inspection["insDate"]
                            + "_"
                            + detail["defDesc"]
                        ).encode("utf-8")
                                        
                        # create hash value
                        hash_value = hashlib.md5(unique_composite_key)
                        
                        # skip duplicates if they exist
                        if hash_value.hexdigest() in indices:
                            continue
                        indices.append(hash_value.hexdigest())
                        
                        yield {
                            "unique_id": hash_value.hexdigest(),
                            #"estId": facility["estId"],
                            "facilityName": facility["facilityName"],
                            "address": facility["addrFull"],
                            "estName": establishment["estName"],
                            "accessType": establishment["accessType"],
                            "type": establishment["type"],
                            "insStatus": inspection["insStatus"],
                            "insDate": inspection["insDate"],                        
                            "observation": inspection["observation"],
                            "infCategory": infraction["infCategory"],
                            "defDesc": detail.get("defDesc", None),
                            "infType": detail.get("infType", None),
                            "actionDesc": detail.get("actionDesc", None),
                            "geometry": json.dumps(
                                {
                                    "type": "Point",
                                    "coordinates": [
                                        float(facility["lon"]),
                                        float(facility["lat"]),
                                    ],
                                }
                            ),
                        }

def childcaresafe():
        from io import StringIO
        import hashlib
        url = "https://secure.toronto.ca/opendata/cc_od/full_list/v1?format=json"
        user_key = Variable.get("secure_toronto_opendata_USER_KEY")
        srv_key = Variable.get("childcaresafe_secure_toronto_opendata_SRV_KEY")
    
        headers = {
            "SRV-KEY": srv_key,
            "USER-KEY": user_key,
        }
    
        raw_input = json.loads(requests.get(url, headers=headers).text)
    
        indices = []
    
        for json_item in raw_input:
            establishment = json_item["json"]
            for inspection in establishment.get("inspections", None) or []:
                # if theres no infractions, append the data to the output
                if not inspection.get("infractions", None):
    
                    unique_composite_key = (
                        establishment["estName"]
                        + "_"
                        + inspection["insDate"]
                    ).encode("utf-8")
    
                    # create hash value
                    hash_value = hashlib.md5(unique_composite_key)
                    
                    # skip duplicates if they exist
                    if hash_value.hexdigest() in indices:
                        continue
                    indices.append(hash_value.hexdigest())
                        
                    yield {
                        "unique_id": hash_value.hexdigest(),
                        "Establishment ID": establishment["estId"],
                        "Establishment Name": establishment["estName"],
                        "Establishment Address": establishment["addrFull"],                    
                        "Inspection Status": inspection["insStatus"],
                        "Inspection Date": inspection["insDate"],                    
                        "Observation": inspection["observation"],
                        "Infraction Category": None,
                        "Infraction Details": None,
                        "Severity": None,
                        "Action": None,
                        "geometry": json.dumps(
                            {
                                "type": "Point",
                                "coordinates": [
                                    float(establishment["lon"]),
                                    float(establishment["lat"]),
                                ],
                            }
                        ),
                    }
    
                for infraction in inspection.get("infractions", None) or []:
                    # append infraction detail info, as available, to the output
                    # add a unique primary key as required by datastore_upsert
    
                    for detail in infraction.get("infDtl", None) or []:
                        # append infraction detail info, as available, to the output
                        # add a unique primary key as required by datastore_upsert
                        unique_composite_key = (
                            establishment["estName"]
                            + "_"
                            + inspection["insDate"]
                            + "_"
                            + detail["defDesc"]
                        ).encode("utf-8")
                                        
                        # create hash value
                        hash_value = hashlib.md5(unique_composite_key)
                        
                        # skip duplicates if they exist
                        if hash_value.hexdigest() in indices:
                            continue
                        indices.append(hash_value.hexdigest())
                        
                        yield {
                            "unique_id": hash_value.hexdigest(),
                            "Establishment ID": establishment["estId"],
                            "Establishment Name": establishment["estName"],
                            "Establishment Address": establishment["addrFull"],                        
                            "Inspection Status": inspection["insStatus"],
                            "Inspection Date": inspection["insDate"],                        
                            "Observation": inspection["observation"],
                            "Infraction Category": infraction["infCategory"],
                            "Infraction Details": detail.get("defDesc", None),
                            "Severity": detail.get("infType", None),
                            "Action": detail.get("actionDesc", None),
                            "geometry": json.dumps(
                                {
                                    "type": "Point",
                                    "coordinates": [
                                        float(establishment["lon"]),
                                        float(establishment["lat"]),
                                    ],
                                }
                            ),
                        }


def residential_health_hazards():
    import hashlib

    url = "https://secure.toronto.ca/opendata/eh/properties/v1?format=json"
    user_key = Variable.get("secure_toronto_opendata_USER_KEY")
    srv_key = Variable.get("healthhazards_secure_toronto_opendata_SRV_KEY")

    headers = {
        "SRV-KEY": srv_key,
        "USER-KEY": user_key,
    }

    raw_input = json.loads(requests.get(url, headers=headers).text)

    for item in raw_input:
        # add a unique primary key as required by datastore_upsert
        unique_composite_key = (
            str(item["case_id"])
            + "_"
            + item["investigation_date"]
            + "_"
            + item["hazard_type"]
        ).encode("utf-8")
                        
        # create hash value
        hash_value = hashlib.md5(unique_composite_key)
                
        yield {
            "unique_id": hash_value.hexdigest(),
            "case_id": item["case_id"],
            "case_type": item["case_type"],
            "address": item["address"],
            "geo_id": item["geo_id"],
            "investigation_type": item["investigation_type"],
            "investigation_date": item["investigation_date"],
            "last_updated_date": item["last_updated_date"],
            "info_code": item["info_code"],
            "hazard_type": item["hazard_type"],
            "violation": item["violation"],
            "status_desc": item["status_desc"],
            "file_extract_date": item["file_extract_date"],
            "lon": item["lon"],
            "lat": item["lat"],
        }


def tennis_courts_facilities():
    url = "https://www.toronto.ca/data/parks/live/tennislist.json?_=1722446635835"
    records = json.loads(requests.get(url).content)["all"]

    for record in records:
        
        # clean coordinates
        lng = float(record["lng"]) if record["lng"] else None
        lat = float(record["lat"]) if record["lat"] else None

        yield {
            "ID": record["ID"],
            "Name": record["Name"],
            "Type": record["Type"],
            "Lights": record["Lights"],
            "Courts": record["Courts"],
            "Phone": record["Phone"],
            "ClubName": record["ClubName"],
            "ClubWebsite": record["ClubWebsite"],
            "ClubInfo": record["ClubInfo"],
            "LocationAddress": record["LocationAddress"],
            "WinterPlay": record["WinterPlay"],
            "geometry": json.dumps(
                {"type": "Point", "coordinates": [lng, lat]}
            )
        }


def members_of_toronto_city_council_voting_record():
    url = "https://opendata.toronto.ca/city.clerks.office/tmmis/VW_OPEN_VOTE_2022_2026.csv"

    content = requests.get(url).text
    csv_file = io.StringIO(content)
    csvreader = csv.DictReader(csv_file)
    for row in list(csvreader):
        row["Agenda Item Title"] = row["Agenda Item Title"].replace("\x92", "'")
        
        yield row


def building_permits_green_roofs():
    ibms_data_file = requests.get("https://opendata.toronto.ca/toronto.building/building-permits-green-roofs/greenroofs.csv").text
    headers = "PERMIT_NUM","REVISION_NUM","PERMIT_TYPE","STRUCTURE_TYPE","STREET_NUM","STREET_NAME","STREET_TYPE","STREET_DIRECTION","POSTAL","APPLICATION_DATE","ISSUED_DATE","COMPLETED_DATE","STATUS","DESCRIPTION","GREEN_ROOF_AREA","GREEN_ROOF_VARIATION_AREA",
    ibms_data = csv.DictReader(io.StringIO(ibms_data_file), fieldnames = headers) 
    next(ibms_data)

    eco_roofs_file = requests.get("https://opendata.toronto.ca/toronto.building/building-permits-green-roofs/Green Roof Permit Info_Open Data.xlsx").content
    eco_roofs_data = openpyxl.load_workbook(filename = io.BytesIO(eco_roofs_file))["Sheet1"]
        
    for ibms_row in ibms_data:
        ibms_row["ECO_ROOF"] = False
        for eco_roof in eco_roofs_data:
            if ibms_row["PERMIT_NUM"] == eco_roof[2].value[:9]:
                ibms_row["ECO_ROOF"] = True

        yield ibms_row
                    

def library_branch_programs_and_events_feed():
    raw = requests.get("https://opendatasstg.blob.core.windows.net/events-feed/tpl-events-feed.json?sp=r&st=2023-06-19T19:56:33Z&se=2031-01-01T04:59:59Z&spr=https&sv=2022-11-02&sr=b&sig=rpZYPwSIa4zXJIt45WztzvkJZL%2BnF3YIAIuZ%2Bq%2Fl2uI%3D").text
    expected_fields = [
        "EventID",
        "Title",
        "StartTime",
        "EndTime",
        "StartDateLocal",
        "LocationName",
        "Audiences",
        "Languages",
        "EventTypes",
        "IsRecurring",
        "IsFull",
        "RegistrationClosed",
        "Status",
        "RegistrationIsFull",
        "FeaturedImageUrl",
        "LastUpdatedOn",
    ]

    for line in raw.split("\n"):
        try:
            row = json.loads(line)
            for f in expected_fields:
                if f not in row.keys():
                    row[f] = None

            yield row
        except Exception as e:
            print(e)


def ckan_api_usage():
    import hashlib
    import boto3
    from botocore.exceptions import ClientError
    import sys
    sys.path.insert(0, "/home/apache-airflow")
    import cot_env_lambda

    FunctionName=cot_env_lambda.FunctionName
    Region=cot_env_lambda.Region

    """
    Invokes Cloud Services' Athena query Lambda function and returns the parsed JSON result.
    How this Lambda works:
    1. Someone hits the API 
    2. CloudFront put the log to S3 buckets (takes within one hour or longer)
    3. Step Functions / Lambda functions run regularly (per hour for QA, per 15 minutes for Prod) 
       to copy the data and perform partition to an Athena table
    """

    client = boto3.client("lambda", region_name= Region)
    
    # This gives us one day's data per call
    # Let's determine which days of data we need
    dates = []
    output = []
    # Get yesterday's date object
    yesterday = date.today() - timedelta(days=1)

    # prepare to check CKAN data
    import ckanapi
    active_env = Variable.get("active_env")
    ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
    ckan_address = ckan_creds[active_env]["address"]
    ckan_apikey = ckan_creds[active_env]["apikey"]

    ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])

    package = ckan.action.package_show(id="open-data-web-analytics")
    resource = [r for r in package.get("resources") if r["name"] == "API Usage"]

    # If the resource doesn't exist...
    if len(resource) == 0:
        # Grab all data from April 1 2026 to yesterday
        this_date = date(2026, 4, 1)

    # If resource exists, determine it's latest date of data
    elif len(resource) > 0:
        data = ckan.action.datastore_search(id=resource[0]["id"], sort="date desc")
        this_date = datetime.strptime(data["records"][0]["date"], "%Y-%m-%d").date()
    
    # grab all days from that day to yesterday
    while this_date != yesterday:
        dates.append(this_date.strftime("%Y-%m-%d"))
        this_date = this_date + timedelta(days=1)
    
    logging.info(f"Preparing to load data for {len(dates)} date(s)")
    for date_string in dates:
        payload = {
            "date": date_string
        }

        response = client.invoke(
            FunctionName=FunctionName,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8")
        )

        # Read and parse response payload
        raw_payload = response["Payload"].read()
        decoded = json.loads(raw_payload.decode("utf-8"))
        results = json.loads(decoded["body"])["results"]
        
        # add the date to the result
        if len(results) > 0:
            for result in results:                                
                # parse data for each different kind of id
                ids = ["pid", "rid", "id"]
                if any([this_id+"s" in result.keys() for this_id in ids]):
                    for this_id in ids:
                        if len(result.get(f"{this_id}s", [])) > 0:                    

                            for item in result[f"{this_id}s"]:
                                # create hash value
                                # make a compound key for the record id
                                unique_composite_key = (
                                    date_string
                                    + result["uri"]
                                    + result["cnt"]
                                    + item[this_id]
                                )
                                hash_value = hashlib.md5(unique_composite_key.encode("utf-8")).hexdigest()                                                                
                                yield {
                                    "date": date_string,
                                    "uri": result["uri"],
                                    "id": item[this_id],
                                    "count": result["cnt"],
                                    "record_id": hash_value,
                                }
                elif len(result.keys()) == 2:
                    unique_composite_key = (
                            date_string
                            + result["uri"]
                            + result["cnt"]
                        )
                    hash_value = hashlib.md5(unique_composite_key.encode("utf-8")).hexdigest()                                        
                    yield {
                            "date": date_string,
                            "uri": result["uri"],
                            "id": None,
                            "count": result["cnt"],
                            "record_id": hash_value,
                        }


def pcard_expenditures():
    '''Custom logic for parsing a folder in the NAS full of excel files into a single schema
    
    There are over a hundred files split into xls and xlsx formats. While they all are meant to contain the same attributes...
    - Many attributes have different names in each file
    - Some attributes are missing or duplicated
    '''
    import gc
    import xlrd
    import time

    correct_headers = ["Division", "Batch-Transaction ID", "Transaction Date", "Card Posting Dt", "Merchant Name", "Transaction Amt.", "Transaction Currency", "Original Amount", "Original Currency", "G/L Account", "G/L Account Description", "Cost Centre / WBS Element / Order Number", "Cost Centre / WBS Element / Order Number Description", "Merchant Type", "Merchant Type Description", "Purpose"]

    correct_headers_map = {
        'Divison': 'Division',
        'Batch Transaction Id': "Batch-Transaction ID",
        'Batch Transaction ID': "Batch-Transaction ID",
        "Batch-Transaction Id": "Batch-Transaction ID",
        'Card Posting Dt': 'Card Posting Dt',
        'Card Posting Date': 'Card Posting Dt',
        'Transaction Amount': 'Transaction Amt.',
        'Transaction Currency': "Transaction Currency",
        'Trx. Currency': "Transaction Currency",
        'Tr Currency': "Transaction Currency",
        'Trx Currency': "Transaction Currency",
        'Trx.Currency': "Transaction Currency",
        #'Original Currency': "Transaction Currency",
        'Cost Centre / Wbs Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / WBS Element / Order': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / WBS element / Order': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / WBS Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'G/L Account Description': "G/L Account Description",
        'Cost Centre/Wbs Element Description ': "G/L Account Description",
        'Long Text': "G/L Account Description",
        "G/L Account": "G/L Account",
        'G/L Account Discription': "G/L Account Description",        
        'G/L Expense Description': "G/L Account Description",
        #'Expense Type': "G/L Account Description",
        'Cost Centre / Wbs Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre /Wbs Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre/Wbs Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / WBS element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / WBS Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        #'G/L Account': "Cost Centre / WBS Element / Order Number",
        'Cost Centre/Wbs\nElement': "Cost Centre / WBS Element / Order Number",
        'Cost Centre/ \nWbs Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / Wbs\n Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre/\nWbs Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / Wbs Elelment': "Cost Centre / WBS Element / Order Number",
        'Cost Centre /  Wbs Element': "Cost Centre / WBS Element / Order Number",
        'Cost Centre/ Wbs Element / Order': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / Wbs Element / Work Order Number': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / Wbs Element / Order #': "Cost Centre / WBS Element / Order Number",
        'Cost Centre / Wbs Element / Order': "Cost Centre / WBS Element / Order Number",
        "Cost Centre / WBS Element": "Cost Centre / WBS Element / Order Number",
        "Cost Centre / WBS Element / Order": "Cost Centre / WBS Element / Order Number",
        'Cost Centre / WBS Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre /  WBS Element / Order No.': "Cost Centre / WBS Element / Order Number",
        'Cost Centre /  WBS Element / Order No. Decription': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / Wbs Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        "Cost Centre / WBS Element Description": "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / Wbs Element Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre /Wbs Element Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre/Wbs Element Discriprion': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre/Wbs Element Description': "Cost Centre / WBS Element / Order Number Description",
        #'G/L Account Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre /  WBS Element / Order No. Decription': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / Wbs Element Descrption': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre/ Wbs Element Descrption': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / Wbs Elelment Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre /  Wbs Element Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre/ Wbs Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre/Wbs Element/Work Order Number Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / Wbs Element / Order # Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / Wbs Element / Order # Decription': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre / Wbs Element /Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Funds Center': "Cost Centre / WBS Element / Order Number",
        'Merchant Type (MCC)': 'Merchant Type',
        'Cost Centre/WbsElement': "Cost Centre / WBS Element / Order Number",
        'Cost Centre/WbsElement Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre/ Wbs Element':"Cost Centre / WBS Element / Order Number", 
        'Cost Centre/Wbs Element Description': "Cost Centre / WBS Element / Order Number Description",
        #'Merchant Name': "Merchant Type Description",
        'Cost Centre / WBS Element / Order No': "Cost Centre / WBS Element / Order Number", 
        'Cost Centre / WBS Element / Order No.': "Cost Centre / WBS Element / Order Number", 
        'Cost Centre /  WBS Element / Order No.': "Cost Centre / WBS Element / Order Number", 
        'Cost Centre / WBS Element / Order ': "Cost Centre / WBS Element / Order Number", 
        'Cost Centre/ WBS Element / Order': "Cost Centre / WBS Element / Order Number", 
        'Cost Centre /WBS Element / Order': "Cost Centre / WBS Element / Order Number", 
        'Cost Center / WBS Element / Order': "Cost Centre / WBS Element / Order Number", 
        'Cost Center / WBS Element / Order #': "Cost Centre / WBS Element / Order Number", 
        'Cost Centre / WBS Element / Order No. Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre /  WBS Element / Order No. Decription': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre/ WBS Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre /WBS Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Center / WBS Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Center / WBLS Element / Order Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Center / WBS Element / Order # Description': "Cost Centre / WBS Element / Order Number Description",
        'Cost Centre /  WBS Element / Order No.': 'Cost Centre / WBS Element / Order Number',
        'Cost Centre /  WBS Element / Order No. Decription': 'Cost Centre / WBS Element Description',
        'Cost Centre / WBS Element / Order Description': 'Cost Centre / WBS Element Description',
        'Cost Centre / WBS element / Order': 'Cost Centre / WBS Element / Order Number',
        'Cost Centre / WBS Element / Order': 'Cost Centre / WBS Element / Order Number',
        'Cost Desc': 'Cost Centre / WBS Element / Order Number Description',        
        'Exp Type Desc': "G/L Account Description",
        "G/L  Description": "G/L Account Description",
    }

    base_url = "https://opendata.toronto.ca/accounting.services/pcard-expenditures/expenditures/PCardExpenses_yyyymmm.xls"
    filepaths = misc_utils.parse_possible_filepaths(base_url)
    # collect filepaths again for .xlsx files, too
    base_url += "x"
    filepaths += misc_utils.parse_possible_filepaths(base_url)

    for item in filepaths:
        date = item[0]
        filepath = item[1]
        unclear_cols = set()
        logging.info(f"Reading file for {date}")
        logging.info(f"Reading file {filepath}")
        file = requests.get(filepath).content
    
        if filepath.endswith(".xls"):
            wb = xlrd.open_workbook(file_contents = file)
            ws = wb.sheet_by_index(0)
            
            for rownum in range(ws.nrows):
                if rownum == 0:
                    source_headers = [ws.cell_value(0, colnum).title().strip().replace("\n", "") for colnum in range(ws.ncols)]
                    fixed_source_headers = []
                    # clean up any inconsistent header names
                    for i in range(len(source_headers)):
                        fixed_source_headers.append(correct_headers_map.get(source_headers[i], source_headers[i]))

                    
                else:
                    row = ws.row(rownum)
                    if row[0].value and row[1].value:
            
                        out_row = {
                            # convert everything to a string except empty cells
                            # openpyxl has more data types than we store in CKAN
                            # we convert from string to a CKAN-friendly datatype later
                            fixed_source_headers[i]: str(row[i].value).strip() if row[i].value is not None else None
                            for i in range(len(row))
                        }
                        for correct_header in correct_headers:
                            if correct_header not in out_row.keys():
                                out_row[correct_header] = None
                                unclear_cols.add(correct_header)

                        out_row["Transaction Date"] = xlrd.xldate_as_datetime(int(float(out_row["Transaction Date"])), wb.datemode)
                        out_row["Card Posting Dt"] = xlrd.xldate_as_datetime(int(float(out_row["Card Posting Dt"])), wb.datemode)

                        yield out_row

        elif filepath.endswith(".xlsx"):
            wb = openpyxl.load_workbook(filename = io.BytesIO(file))
            ws = wb.worksheets[0]
            source_headers = [col.value.strip().replace("\n", "") if col.value is not None else None for col in ws[1] ]
            fixed_source_headers = []
            # clean up any inconsistent header names
            for i in range(len(source_headers)):
                fixed_source_headers.append(correct_headers_map.get(source_headers[i], source_headers[i]))
           
            for row in ws.iter_rows(min_row=2):    
                if row[0].value:
                    out_row = {
                        # convert everything to a string except empty cells
                        # openpyxl has more data types than we store in CKAN
                        # we convert from string to a CKAN-friendly datatype later
                        fixed_source_headers[i]: str(row[i].value).strip() if row[i].value is not None else None
                        for i in range(len(row))
                    }
                    for correct_header in correct_headers:
                        if correct_header not in out_row.keys():
                            out_row[correct_header] = None
                            unclear_cols.add(correct_header)                                            
                    
                    yield out_row
        
        if len(unclear_cols):
            logging.warning(f"{unclear_cols} attributes are missing from this file. This file's attributes were: {source_headers}")
        del file
        gc.collect()

