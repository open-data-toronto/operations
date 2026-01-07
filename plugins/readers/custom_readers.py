"""Functions yielding rows (as dicts) for custom read jobs"""

import json
import requests
import logging
import csv
import io
from datetime import datetime
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
            "comments": item["comments"],
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
    url = "https://secure.toronto.ca/opendata/ds_od/inspections/v1?format=csv"
    user_key = Variable.get("secure_toronto_opendata_USER_KEY")
    srv_key = Variable.get("dinesafe_secure_toronto_opendata_SRV_KEY")

    headers = {
        "SRV-KEY": srv_key,
        "USER-KEY": user_key,
    }
    
    with requests.post(url, headers=headers, stream=True) as r:   
            
        iterator = (line.decode("latin1") for line in r.iter_lines())

        header_reader = csv.reader(StringIO(next(iterator)))
        for line in header_reader:
            headers = line
    
        reader = csv.DictReader(iterator, fieldnames = headers)
        
        for row in reader:
            # add a unique primary key as required by datastore_upsert
            unique_composite_key = (
                row["Establishment ID"]
                + "_"
                + row["Inspection ID"]
                + "_"
                + row["Infraction Details"]
            ).encode("utf-8")
            
            # create hash value
            hash_value = hashlib.md5(unique_composite_key)
            row["unique_id"] = hash_value.hexdigest()

            # remove old key
            row.pop("Rec #")

            yield row


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
                    
                    