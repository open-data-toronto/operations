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
