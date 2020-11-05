from datetime import datetime, timedelta

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}


def run(logger, utils, ckan, configs=None):
    packages = utils.get_all_packages(ckan)

    run_time = datetime.utcnow()

    packages_behind = []

    logger.info("Looping through packages")
    for p in packages:
        refresh_rate = p.pop("refresh_rate").lower()

        if refresh_rate not in TIME_MAP.keys() or p["is_retired"]:
            continue

        last_refreshed = p.pop("last_refreshed")
        last_refreshed = datetime.strptime(last_refreshed, "%Y-%m-%dT%H:%M:%S.%f")

        days_behind = (run_time - last_refreshed).days
        next_refreshed = last_refreshed + timedelta(days=TIME_MAP[refresh_rate])

        if days_behind > TIME_MAP[refresh_rate]:
            details = {
                "name": p["name"],
                "email": p["owner_email"],
                "publisher": p["owner_division"],
                "rate": refresh_rate,
                "last": last_refreshed.strftime("%Y-%m-%d"),
                "next": next_refreshed.strftime("%Y-%m-%d"),
                "days": days_behind,
            }
            packages_behind.append(details)
            logger.info(
                f"{details['name']}: Behind {details['days']} days. {details['email']}"
            )

    lines = [
        "_{name}_ ({rate}): refreshed `{last}`. Behind *{days} days*. {email}".format(
            **p
        )
        for p in sorted(packages_behind, key=lambda x: -x["days"])
    ]

    lines = [f"{i+1}. {l}" for i, l in enumerate(lines)]

    return {
        "message_type": "success",
        "msg": "\n".join(lines),
    }
