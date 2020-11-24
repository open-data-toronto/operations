import logging


def update_resource_last_modified(ckan, resource_id, new_last_modified):
    return ckan.action.resource_patch(
        id=resource_id,
        last_modified=new_last_modified.strftime("%Y-%m-%dT%H:%M:%S"),
    )


def get_all_packages(ckan):
    catalogue_size = len(ckan.action.package_list())
    packages = ckan.action.package_search(rows=catalogue_size)["results"]
    logging.info(f"Retrieved {len(packages)} packages")

    return packages


def get_package(**kwargs):
    ckan = kwargs.pop("ckan")
    package_id = kwargs.pop("package_id")

    package = ckan.action.package_show(id=package_id)

    return package


def create_resource_if_new(**kwargs):
    ckan = kwargs.pop("ckan")
    package = kwargs.pop("package")
    resource = kwargs.pop("resource")

    resources = package["resources"]

    is_new = resource["name"] not in [r["name"] for r in resources]

    if is_new:
        res = ckan.action.resource_create(**resource)
        return res

    return [r for r in resources if r["name"] == resource["name"]][0]


def insert_datastore_records(ckan, resource_id, records, chunk_size=20000):

    chunks = [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]

    for chunk in chunks:
        logging.info(f"Removing NaNs and inserting {len(chunk)} records")

        clean_records = []
        for r in chunk:
            record = {}
            for key, value in r:
                if value == value:
                    record[key] = value
            clean_records.append(record)

        ckan.action.datastore_create(id=resource_id, records=clean_records)
