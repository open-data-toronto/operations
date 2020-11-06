import jobs.utils.common as utils
import logging


def run(**kwargs):
    def pprint_2d_list(matrix):
        s = [[str(e) for e in row] for row in matrix]
        lens = [max(map(len, col)) for col in zip(*s)]
        fmt = "\t".join("{{:{}}}".format(x) for x in lens)
        table = [fmt.format(*row) for row in s]
        return "\n".join(table)

    ckan = kwargs.pop("ckan")

    packages = utils.get_all_packages(ckan)
    logging.info(f"Retrieved {len(packages)} packages")
    datastore_resources = []

    logging.info("Identifying datastore resources")
    for package in packages:
        for resource in package["resources"]:
            if resource["url_type"] != "datastore":
                continue
            response = ckan.action.datastore_search(id=resource["id"], limit=0)

            datastore_resources.append(
                {
                    "package_id": package["title"],
                    "resource_id": resource["id"],
                    "resource_name": resource["name"],
                    "extract_job": resource["extract_job"],
                    "row_count": response["total"],
                    "fields": response["fields"],
                }
            )

            logging.info(
                f'{package["name"]}: {resource["name"]} - {response["total"]} records'
            )

    logging.info(f"Identified {len(datastore_resources)} datastore resources")

    empties = [r for r in datastore_resources if r["row_count"] == 0]

    if not len(empties):
        logging.info("No empty resources found")
        return {"message_type": "success", "msg": "No empties"}

    empties = sorted(empties, key=lambda i: i["package_id"])

    matrix = [["#", "PACKAGE", "EXTRACT_JOB", "ROW_COUNT"]]
    for i, r in enumerate(empties):
        string = [f"{i+1}."]
        string.extend(
            [r[f] for f in ["package_id", "extract_job", "row_count"] if r[f]]
        )
        matrix.append(string)

    empties = pprint_2d_list(matrix)

    logging.warning(f"Empty resources found:\n```\n{empties}\n```")

    return {
        "message_type": "warning",
        "msg": f"""```{empties}```""",
    }