"""Functions yielding rows (as dicts) for custom read jobs"""

import json
import requests
import logging
from datetime import datetime
from airflow.models import Variable
from utils import misc_utils



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

def tobids_all_solicitations():
    has_more = True
    offset = 0
    total_records = []
    curr_date = datetime.today().strftime('%Y-%m-%d')

    while has_more:
        url = (
                "https://secure.toronto.ca/c3api_data/v2/DataAccess.svc/pmmd_solicitations/feis_solicitation?$format=application/json;odata.metadata=none&$count=true&$skip="
                + str(offset)
                + "0&$filter=Ready_For_Posting%20eq%20%27Yes%27%20and%20Status%20eq%20%27Open%27%20and%20Closing_Date%20ge%20" + curr_date + "&$orderby=Closing_Date%20desc,Issue_Date%20desc"
        )
        logging.info(f"Requesting data from {url}")
        records = json.loads(requests.get(url).content)
        total_records += records["value"]
        
        logging.info(f"Processing in batch, start from {offset}")
        has_more = len(total_records) < records["@odata.count"]
        offset += 100

    logging.info(f"A total of {len(total_records)} records.")

    fields = ['id',
              'Solicitation_Document_Number',
              'Solicitation_Document_Type',  
              'High_Level_Category',
              'Solicitation_Form_Type',
              'Issue_Date', 
              'Closing_Date',
              'Publish_Date',
              'Buyer_Name',
              'Buyer_Email',
              'Buyer_Phone_Number',
              'Client_Division',
              'Solicitation_Document_Description']

    for record in total_records:
        clean_record = {}
        for field in fields:
            if field in record.keys():
                clean_record[field] = record[field]
            else:
                clean_record[field] = None
        
        # clean text before insert into ckan datastore
        clean_record["Client_Division"] = ",".join(clean_record["Client_Division"])
        clean_record["Solicitation_Document_Description"] = clean_record["Solicitation_Document_Description"].replace("\n", "")
        
        yield {
                'unique_id': clean_record['id'],
                'Document Number': 'Doc' + clean_record['Solicitation_Document_Number'],
                'RFx (Solicitation) Type': clean_record['Solicitation_Document_Type'],
                'NOIP (Notice of Intended Procurement) Type': clean_record['Solicitation_Form_Type'],
                'Issue Date': clean_record['Issue_Date'], 
                'Submission Deadline': clean_record['Closing_Date'],
                'High Level Category': clean_record['High_Level_Category'],
                'Solicitation Document Description': clean_record['Solicitation_Document_Description'],
                'Division': clean_record['Client_Division'],
                'Buyer Name': clean_record['Buyer_Name'],
                'Buyer Email': clean_record['Buyer_Email'],
                'Buyer Phone Number': clean_record['Buyer_Phone_Number'],
        }

def tobids_awarded_contracts():
    from datetime import date
    from dateutil.relativedelta import relativedelta

    has_more = True
    offset = 0
    total_records = []
    # filter only keep 18 months data
    cut_date = str(date.today() + relativedelta(months=-18))

    while has_more:
        url = (
            "https://secure.toronto.ca/c3api_data/v2/DataAccess.svc/pmmd_solicitations/feis_solicitation?$format=application/json;odata.metadata=none&$count=true&$skip="
            + str(offset)
            + "&$filter=Ready_For_Posting%20eq%20%27Yes%27%20and%20((Solicitation_Form_Type%20eq%20%27Awarded%20Contracts%27%20and%20Awarded_Cancelled%20eq%20%27No%27%20and%20Latest_Date_Awarded%20gt%20"
            + cut_date
            + ")%20or%20(Solicitation_Form_Type%20eq%20%27Awarded%20Contracts%27%20and%20Awarded_Cancelled%20eq%20%27Yes%27%20and%20Cancellation_Date%20gt%20%27"
            + cut_date
            + "%27))&$orderby=Purchasing_Group,High_Level_Category,Solicitation_Document_Number,Posting_Title%20desc"
        )
        logging.info(f"Requesting data from {url}")
        records = json.loads(requests.get(url).content)
        total_records += records["value"]
        
        logging.info(f"Processing in batch, start from {offset}")
        has_more = len(total_records) < records["@odata.count"]
        offset += 100

    logging.info(f"A total of {len(total_records)} records.")

    fields = [
        "id",
        "Solicitation_Document_Number",
        "Solicitation_Document_Type",
        "High_Level_Category",
        "Client_Division",
        "Buyer_Name",
        "Buyer_Email",
        "Buyer_Phone_Number",
        "Solicitation_Document_Description",
    ]

    awarded_supplier_fields = ["Successful_Bidder", "Award_Amount", "Date_Awarded"]

    for record in total_records:
        clean_record = {}
        for field in fields:
            if field in record.keys():
                clean_record[field] = record[field]
            else:
                clean_record[field] = None

        # clean text before insert into ckan datastore
        clean_record["Client_Division"] = ",".join(clean_record["Client_Division"])
        clean_record["Solicitation_Document_Description"] = clean_record[
            "Solicitation_Document_Description"
        ].replace("\n", "")

        awarded_supplier_info = record["Awarded_Suppliers"]

        counter = 0
        for entry in awarded_supplier_info:
            # Use counter to create unique key for each record
            counter += 1
            for field in awarded_supplier_fields:
                clean_record[field] = entry[field] if field in entry.keys() else None

            yield {
                "unique_id": clean_record["id"] + "_" + str(counter),
                "Document Number": "Doc" + clean_record["Solicitation_Document_Number"],
                "RFx (Solicitation) Type": clean_record["Solicitation_Document_Type"],
                "High Level Category": clean_record["High_Level_Category"],
                "Successful Supplier": clean_record["Successful_Bidder"],
                "Awarded Amount": clean_record["Award_Amount"],
                "Award Date": clean_record["Date_Awarded"],
                "Division": clean_record["Client_Division"],
                "Buyer Name": clean_record["Buyer_Name"],
                "Buyer Email": clean_record["Buyer_Email"],
                "Buyer Phone Number": clean_record["Buyer_Phone_Number"],
                "Solicitation Document Description": clean_record[
                    "Solicitation_Document_Description"
                ],
            }

def tobids_non_competitive_contracts():
    from datetime import date
    from dateutil.relativedelta import relativedelta

    has_more = True
    offset = 0
    total_records = []
    # filter only keep 18 months data
    cut_date = str(date.today() + relativedelta(months=-18))
    
    while has_more:
        url = (
            "https://secure.toronto.ca/c3api_data/v2/DataAccess.svc/pmmd_solicitations/feis_non_competitive?$format=application/json;odata.metadata=none&$count=true&$skip="
            + str(offset)
            + "&$filter=Ready_For_Posting%20eq%20%27Yes%27%20and%20Status%20eq%20%27Awarded%27%20and%20Awarded_Cancelled%20eq%20%27No%27%20and%20Latest_Date_Awarded%20gt%20"
            + cut_date
            + "&$orderby=Latest_Date_Awarded%20desc"
        )
        logging.info(f"Requesting data from {url}")
        records = json.loads(requests.get(url).content)
        total_records += records["value"]
        
        logging.info(f"Processing in batch, start from {offset}")
        has_more = len(total_records) < records["@odata.count"]
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

        yield {
            "unique_id": clean_record["id"],
            "Workspace Number": clean_record["Non_Competitive_Reference_Number"],
            "Reason": clean_record["Non_Competitive_Reason"],
            "Contract Date": clean_record["Latest_Date_Awarded"],
            "Supplier Name": clean_record["Successful_Bidder"],
            "Contract Amount": clean_record["Award_Amount"],
            "Division": clean_record["Client_Division"],
        }

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

