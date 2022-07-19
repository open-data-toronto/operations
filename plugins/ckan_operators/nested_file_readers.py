# nested_file_readers.py - list of custom functions for parsing tailor made nested inputs using yaml

import json

def bodysafe(data_path):
# custom logics to produce a flat list of dicts from the nested bodysafe input
    input = json.load( open(data_path, "r", encoding="latin-1"))
    output = []

    for item in input:

        template = {
                "estId": item["json"]["estId"],
                "estName": item["json"]["estName"],
                "addrFull": item["json"]["addrFull"],
                "srvType": None,
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
            }
        print("----------- WORKING ON " + item["json"]["estName"])
        # deal with geometry
        template["geometry"] = json.dumps({ "type": "Point", "coordinates": [item["json"]["lon"], item["json"]["lat"] ] })

        for service in item["json"].get("services", None) or []:
            template["srvType"] = service["srvType"]
            print("------ working on service " + service["srvType"] )

            # if theres no inspections, append the template to the output
            if not service.get("inspections", None):
                print("===== APPENDING TEMPLATE BECAUSE INSPECTIONS IS " + str(service.get("inspections", None)))
                template["insStatus"] = None
                template["insDate"] = None
                template["observation"] = None
                template["infCategory"] = None
                template["defDesc"] = None
                template["infType"] = None
                template["actionDesc"] = None
                template["OutcomeDate"] = None
                template["OutcomeDesc"] = None
                template["fineAmount"] = None
                print("xxx TEMPLATE: " + str(template))    
                output.append( template )
                

            for inspection in service.get("inspections", None) or []:
                print("------- working on inspection " + inspection["insDate"])
                template["insStatus"] = inspection["insStatus"]
                template["insDate"] = inspection["insDate"]
                template["observation"] = inspection["observation"]

                # if theres no infractions, append the template to the output
                if not inspection.get("infractions", None):
                    print("===== APPENDING TEMPLATE BECAUSE infractions IS " + str(service.get("infractions", None)))
                    print(inspection)
                    print(service)
                    print(item)
                    template["infCategory"] = None
                    template["defDesc"] = None
                    template["infType"] = None
                    template["actionDesc"] = None
                    template["OutcomeDate"] = None
                    template["OutcomeDesc"] = None
                    template["fineAmount"] = None
                    print("xxx TEMPLATE: " + str(template))
                    output.append( template )
                    

                for infraction in inspection.get("infractions", None) or []:
                    template["infCategory"] = infraction["infCategory"]
                    print(" ----- working on infraction " + infraction["infCategory"])

                    # if theres no infractions details, append the template to the output
                    if not infraction.get("infDtl", None):
                        print("===== APPENDING TEMPLATE BECAUSE infDtl IS " + str(service.get("infDtl", None)))
                        template["defDesc"] = None
                        template["infType"] = None
                        template["actionDesc"] = None
                        template["OutcomeDate"] = None
                        template["OutcomeDesc"] = None
                        template["fineAmount"] = None
                        print("xxx TEMPLATE: " + str(template))
                        output.append( template )
                        

                    for detail in infraction["infDtl"]:
                        print(" ----- working on infdetail " + detail.get("defDesc", None) )
                        template["defDesc"] = detail.get("defDesc", None)
                        template["infType"] = detail.get("infType", None)
                        template["actionDesc"] = detail.get("actionDesc", None)
                        template["outcomeDate"] = detail.get("outcomeDate", None)
                        template["outcomeDesc"] = detail.get("outcomeDesc", None)
                        template["fineAmount"] = detail.get("fineAmount", None)

                        print(" ========= APPENDING INFRACTION DETAILS")
                        print("xxx TEMPLATE: " + str(template))
                        output.append( template )
    return output
        



nested_readers = {
    "https://secure.toronto.ca/opendata/bs_od/full_list/v1?format=json": bodysafe
}