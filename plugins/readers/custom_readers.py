'''Functions yielding rows (as dicts) for custom read jobs'''

def test_reader():
    data = [
        {
            "row1": 1,
            "row2": "text",
            "row3": 2.11
        },
        {
            "row1": 1,
            "row2": "text",
            "row3": 3.1
        },
        {
            "row1": 2,
            "row2": "text",
            "row3": 3.1
        },
        {
            "row1": 1,
            "row2": "text",
            "row3": 3.1
        },
    ]

    for i in data:
        yield i

def bodysafe():
    # custom logics to produce a flat list of dicts from the nested bodysafe input

    print("------------ BODYSAFE")
    import json
    import requests
    from airflow.models import Variable

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
                        "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
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
                            "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
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
                                "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
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
                                    "geometry": json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })
                                }
