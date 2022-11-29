# nested_file_readers.py - list of custom functions for parsing tailor made nested inputs using yaml

import json

def bodysafe(data_path):
# custom logics to produce a flat list of dicts from the nested bodysafe input
    input = json.load( open(data_path, "r", encoding="latin-1"))
    output = []

    for item in input:
        for service in item["json"].get("services", None) or []:

            # if theres no inspections, append the data to the output
            if not service.get("inspections", None):
                output.append( 
                    {
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
                        "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
                    }
                 )
                

            for inspection in service.get("inspections", None) or []:
                # if theres no infractions, append the data to the output
                if not inspection.get("infractions", None):
                    output.append( 
                        {
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
                            "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
                        }
                    )
                    

                for infraction in inspection.get("infractions", None) or []:
                    # if theres no infractions details, append the data to the output
                    if not infraction.get("infDtl", None):
                        output.append( 
                            {
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
                                "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
                            }
                        )
                        

                    for detail in infraction["infDtl"]:
                        # append infraction detail info, as available, to the output
                        output.append( 
                                {
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
                                    "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
                                }
                            )
    return output


def toronto_beaches_water_quality(data_path):
    input = json.load( open(data_path, "r", encoding="latin-1"))
    output = []

    for item in input:
        output.append(
            {
                "beachId": item["beachId"],
                "beachName": item["beachName"],
                "siteName": item["siteName"],
                "collectionDate": item["collectionDate"],
                "eColi": item["eColi"],
                "comments": item["comments"],
                "geometry": json.dumps({ "type": "Point", "coordinates": [item["lon"], item["lat"] ] }),
            }
        )

    return output



nested_readers = {
    "https://secure.toronto.ca/opendata/bs_od/full_list/v1?format=json": bodysafe,
    "https://secure.toronto.ca/opendata/adv_od/beach_results/v1?format=json&startDate=2000-01-01&endDate=9999-01-01": toronto_beaches_water_quality,
}