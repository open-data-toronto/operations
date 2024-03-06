"""Receives input YAML dag_ids, runs and assesses dag run results"""

import os
import time
import logging
import yaml
import pandas as pd

from datetime import datetime
from pytz import timezone
from utils import misc_utils
from airflow.models import DagRun
from utils_operators.slack_operators import task_failure_slack_alert, SlackWriter
from airflow.decorators import dag, task

CONFIG_FOLDER = "/data/operations/dags/datasets/files_to_datastore"
TMP_DIR = "data/tmp/"
CKAN = misc_utils.connect_to_ckan()


@dag(
    schedule="@once",
    on_failure_callback=task_failure_slack_alert,
    start_date=datetime(2024, 2, 22, 0, 0, 0),
    catchup=False,
    tags=["tests", "yaml"],
)
def thorough_tests():
    """
    ### What
    This DAG tests input YAML DAGs by running them many times against this
    Airflow isntance's associated CKAN instance.

    ### How
    Run this DAG with a config. That config must have this structure:

    `{dag_ids: ["dag_id1", "dag_id2", "dag_id3"]}`

    ### Note
    Make sure all relevant dags are turned on before trigger the thorough tests.
    """

    @task
    def get_dag_ids(**context):
        yamls = list(os.listdir(CONFIG_FOLDER))

        # if given a list of dag ids, check and return those dag_ids:
        if "dag_ids" in context["dag_run"].conf.keys():
            dag_ids = context["dag_run"].conf.get("dag_ids", None)
            assert dag_ids, "Missing input 'dag_ids'"
            assert isinstance(
                dag_ids, list
            ), "Input 'dag_ids' object needs to be a list of strings of the names of YAML dags you want to test"
            for dag_id in dag_ids:
                assert (
                    f"{dag_id}.yaml" in yamls
                ), f"Input 'dag_id' {dag_id} must be a YAML DAG"

        return {"dag_ids": dag_ids}

    def run_dag(dag_id):
        # trigger each dag based on dag_ids
        logging.info("\tRunning command: airflow dags trigger " + dag_id)
        assert os.system("airflow dags trigger " + dag_id) == 0, (
            dag_id + " was unable to start!"
        )

        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        # wait for dag to finish and then store its result in output
        while dag_runs[0].state in ["running", "queued"]:
            time.sleep(5)
            dag_runs = DagRun.find(dag_id=dag_id)
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        return dag_runs[0]

    @task
    def run_dags(dag_ids):
        """This runs a few tests on an input dag_id:

        - First, it purges its package then runs the DAG to make dataset from scratch
        - Then it runs the DAG again to make sure no update is attempted
        - Then it edits data and runs the DAG to make sure the delta(difference) is found
        - Then it spoofs a failure and runs the DAG to check failure protocol
        """
        output = {}
        for dag_id in dag_ids["dag_ids"]:
            logging.info(f"Testing {dag_id}...")
            output[dag_id] = {}
            # get metadata from YAML config
            with open(CONFIG_FOLDER + "/" + dag_id + ".yaml", "r") as f:
                config = yaml.load(f, yaml.SafeLoader)

            package_name = list(config.keys())[0]

            ### PURGE PACKAGE, RUN DAG, EXPECT DATASET CREATED
            logging.info(f"\tTesting {dag_id} purge and run...")
            # purge package
            try:
                CKAN.action.dataset_purge(id=package_name)
            except:
                logging.warning(f"{package_name} was not purged, package not exists.")

            # run DAG and get results
            run = run_dag(dag_id)

            # parse task instance metadata
            results = get_results(run)

            output[dag_id]["test_create_success"] = (
                results["updated"] and results["state"] == "success"
            )

            ### RUN DAG, RUN DAG, EXPECT NO UPDATE TO DATASET
            logging.info(f"\tTesting {dag_id} run with no updates...")
            # run DAG and get results
            run = run_dag(dag_id)

            # parse task instance metadata
            results = get_results(run)

            output[dag_id]["test_no_update_success"] = (
                results["updated"] == False and results["state"] == "success"
            )

            ### EDIT DATA, RUN DAG, EXPECT UPDATE TO DATASET
            logging.info(f"\tTesting {dag_id} run with updates...")
            # edit data stored on airflow server
            tmp_filenames = os.listdir(f"{TMP_DIR}{package_name}")
            for filename in tmp_filenames:
                with open(f"{TMP_DIR}{package_name}/{filename}", "a") as f:
                    f.write("random line of text")
                    f.close()

            # run DAG and get results
            run = run_dag(dag_id)

            # parse task instance metadata
            results = get_results(run)

            output[dag_id]["test_update_success"] = (
                results["updated"] == True and results["state"] == "success"
            )

        logging.info(output)
        return output

    def get_results(run):
        updated = False
        failure_protocol = False
        for ti in run.get_task_instances():
            if ti.task_id.startswith("insert_records_") and ti.state == "success":
                updated = True

            if ti.task_id.startswith("restore_backup_") and ti.state == "success":
                failure_protocol = True

        return {
            "updated": updated,
            "failure_protocol": failure_protocol,
            "state": run.state,
        }

    @task
    def write_to_slack(results):
        # record for slack notifications
        message_lines = []
        # record for report output
        outputs = []
        # counter for how many dags has been tested
        yamls_tested = 0

        for dag_id, test_item in results.items():
            yamls_tested += 1
            for t_name, t_status in test_item.items():
                try:
                    test_name = t_name
                    test_status = ":done_green:" if t_status else ":bangbang:"
                    message_line = f":yaml: {dag_id}  `{test_name}` : {test_status}"
                    message_lines.append(message_line)
                    outputs.append(
                        {
                            "dag_id": dag_id,
                            "test_name": t_name,
                            "test_status": t_status,
                        }
                    )
                except Exception as e:
                    logging.error(e)
                    continue

        df = pd.DataFrame.from_records(outputs)
        pivoted = df.pivot(
            index="dag_id", columns="test_name", values="test_status"
        ).reset_index()
        pivoted["timestamp"] = datetime.now(timezone('UTC'))

        output_path = (
            CONFIG_FOLDER
            + "/thorough_tests_outputs_"
            + str(datetime.today()).split(" ")[0]
            + ".csv"
        )
        pivoted.to_csv(output_path, index=False)

        body = f"\n\t\t\t {yamls_tested} *yamls* tested"

        message = "\n\t\t\t".join(message_lines)

        # write to slack
        SlackWriter(
            dag_id="thorough_tests",
            message_header="YAML DAGs Thorough Tests Complete",
            message_content=message,
            message_body=body,
        ).announce()

    write_to_slack(run_dags(get_dag_ids()))


thorough_tests()


##################### Yamls in Chunk of 10 (DAG Input) ####################

# {'dag_ids': ['active-affordable-and-social-housing-units.yaml', 'address-points-municipal-toronto-one-address-repository.yaml', 'ambulance-station-locations.yaml', 'apartment-building-evaluation.yaml', 'automated-speed-enforcement-locations.yaml', 'bicycle-parking-bike-stations-indoor.yaml', 'bicycle-parking-high-capacity-outdoor.yaml', 'bicycle-parking-racks.yaml', 'bicycle-shops.yaml', 'bicycle-thefts.yaml']}
# {'dag_ids': ['bodysafe.yaml', 'bridge-structure.yaml', 'building-construction-demolition-violations.yaml', 'building-permits-active-permits.yaml', 'building-permits-cleared-permits.yaml', 'business-improvement-areas.yaml', 'cafeto-curb-lane-parklet-cafe-locations.yaml', 'central-intake-calls.yaml', 'centralized-waiting-list-activity-for-social-housing.yaml', 'chemical-tracking-chemtrac.yaml']}
# {'dag_ids': ['city-council-and-committees-meeting-schedule-reports.yaml', 'city-of-toronto-free-public-wifi.yaml', 'city-wards.yaml', 'clothing-drop-box-locations.yaml', 'committee-of-adjustment-applications.yaml', 'community-council-boundaries.yaml', 'council-business-travel-expenses.yaml', 'councillors-constituency-services-office-expense-reporting.yaml', 'covid-19-cases-in-toronto.yaml', 'covid-19-immunization-clinics.yaml']}
# {'dag_ids': ['covid-19-testing-sites.yaml', 'cultural-hotspot-points-of-interest.yaml', 'cycling-network.yaml', 'daily-shelter-overnight-service-occupancy-capacity.yaml', 'deaths-of-people-experiencing-homelessness.yaml', 'demolition-and-replacement-of-rental-housing-units.yaml', 'development-applications.yaml', 'dinesafe.yaml', 'earlyon-child-and-family-centres.yaml', 'elected-officials-contact-information.yaml']}
# {'dag_ids': ['elections-subdivisions.yaml', 'elections-voting-locations.yaml', 'fatal-and-non-fatal-suspected-opioid-overdoses-in-the-shelter-system.yaml', 'fire-hydrants.yaml', 'fire-services-emergency-incident-basic-detail.yaml', 'fire-station-locations.yaml', 'former-municipality-boundaries.yaml', 'green-spaces.yaml', 'imagination-manufacturing-innovation-and-technology-imit-program-recipients.yaml', 'indoor-ice-rinks.yaml']}
# {'dag_ids': ['intersection-file-city-of-toronto.yaml', 'library-branch-general-information.yaml', 'library-branch-programs-and-events-feed.yaml', 'library-card-registrations.yaml', 'library-circulation.yaml', 'library-visits.yaml', 'library-workstation-usage.yaml', 'licensed-child-care-centres.yaml', 'major-crime-indicators.yaml', 'members-of-toronto-city-council-meeting-attendance.yaml']}
# {'dag_ids': ['members-of-toronto-city-council-voting-record.yaml', 'mental-health-apprehensions.yaml', 'motor-vehicle-collisions-involving-killed-or-seriously-injured-persons.yaml', 'municipal-licensing-and-standards-business-licences-and-permits.yaml', 'neighbourhood-crime-rates.yaml', 'neighbourhood-improvement-areas.yaml', 'neighbourhood-profiles.yaml', 'neighbourhoods.yaml', 'non-regulated-lead-sample.yaml', 'on-street-permit-parking-area-maps.yaml']}
# {'dag_ids': ['outbreaks-in-toronto-healthcare-institutions.yaml', 'outdoor-artificial-ice-rinks.yaml', 'park-and-recreation-facility-projects.yaml', 'park-and-recreation-facility-study-areas.yaml', 'parks-drinking-fountains.yaml', 'pedestrian-network.yaml', 'persons-in-crisis-calls-for-service-attended.yaml', 'places-of-interest-and-toronto-attractions.yaml', 'police-annual-statistical-report-administrative.yaml', 'police-annual-statistical-report-complaint-dispositions.yaml']}
# {'dag_ids': ['police-annual-statistical-report-dispatched-calls-by-division.yaml', 'police-annual-statistical-report-firearms-top-5-calibres.yaml', 'police-annual-statistical-report-gross-expenditures-by-division.yaml', 'police-annual-statistical-report-gross-operating-budget.yaml', 'police-annual-statistical-report-homicide.yaml', 'police-annual-statistical-report-investigated-alleged-misconduct.yaml', 'police-annual-statistical-report-miscellaneous-calls-for-service.yaml', 'police-annual-statistical-report-miscellaneous-data.yaml', 'police-annual-statistical-report-miscellaneous-firearms.yaml', 'police-annual-statistical-report-personnel-by-command.yaml']}
# {'dag_ids': ['police-annual-statistical-report-personnel-by-rank-by-division.yaml', 'police-annual-statistical-report-personnel-by-rank.yaml', 'police-annual-statistical-report-reported-crimes.yaml', 'police-annual-statistical-report-search-of-persons.yaml', 'police-annual-statistical-report-top-20-offences-of-firearms-seizures.yaml', 'police-annual-statistical-report-total-public-complaints.yaml', 'police-annual-statistical-report-traffic-collisions.yaml', 'police-annual-statistical-report-victims-of-crime.yaml', 'police-boundaries.yaml', 'police-facility-locations.yaml']}
# {'dag_ids': ['police-race-and-identity-based-data-collection-arrests-strip-searches.yaml', 'police-race-and-identity-based-data-use-of-force.yaml', 'proclamations.yaml', 'public-art.yaml', 'real-estate-asset-inventory.yaml', 'red-light-cameras.yaml', 'registered-programs-and-drop-in-courses-offering.yaml', 'registry-of-declared-interest.yaml', 'school-locations-all-types.yaml', 'secondary-plans.yaml']}
# {'dag_ids': ['shootings-firearm-discharges.yaml', 'site-and-area-specific-policies.yaml', 'solarto.yaml', 'solid-waste-pickup-schedule.yaml', 'street-furniture-bench.yaml', 'street-furniture-bicycle-parking.yaml', 'street-furniture-information-pillar-wayfinding-structure.yaml', 'street-furniture-litter-receptacle.yaml', 'street-furniture-poster-board.yaml', 'street-furniture-poster-structure.yaml']}
# {'dag_ids': ['street-furniture-public-washroom.yaml', 'street-furniture-publication-structure.yaml', 'street-furniture-transit-shelter.yaml', 'street-tree-data.yaml', 'tenant-notification-for-rent-reduction.yaml', 'theft-from-motor-vehicle.yaml', 'topographic-mapping-building-outlines.yaml', 'topographic-mapping-water-line.yaml', 'topographic-mapping-waterbodies-and-rivers.yaml', 'toronto-beaches-water-quality.yaml']}
# {'dag_ids': ['toronto-centreline-tcl.yaml', 'toronto-district-school-board-locations.yaml', 'toronto-fire-services-run-areas.yaml', 'toronto-shelter-system-flow.yaml', 'toronto-signature-sites.yaml', 'traffic-calming-database.yaml', 'traffic-cameras.yaml', 'traffic-control-devices.yaml', 'traffic-signals-tabular.yaml', 'traffic-volumes-at-intersections-for-all-modes.yaml']}
# {'dag_ids': ['upcoming-and-recently-completed-affordable-housing-units.yaml', 'wards-and-elected-councillors.yaml', 'washroom-facilities.yaml', 'wellbeing-youth-aboriginal-youth-services.yaml', 'zoning-by-law.yaml']}