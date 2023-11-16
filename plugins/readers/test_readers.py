'''test cases for reader classes'''

import pytest
import csv
import os
from readers.base import Reader, CSVReader

#def test_reader():
#    '''test cases for Reader parent class'''
#
#    test_source_url = "https://httpstat.us/200"
#    test_schema = []
#    test_out_dir = ""
#    test_filename = ""
#
#    reader = Reader(
#        source_url = test_source_url,
#        schema = test_schema,
#        out_dir = test_out_dir,
#        filename = test_filename
#    )
#
#    assert reader.source_url == test_url

def test_csvreader():
    '''test cases for CSVReader class'''

    test_source_url = "https://opendata.toronto.ca/DummyDatasets/COT_affordable_rental_housing_mod.csv"
    test_schema = [
        {
            "id": "Address",
            "type": "text",
            "info": {
                "notes": "Includes the municipal address(es) of the building/complex where the affordable units will be built. There can be multiple addresses listed in this field. For large scale projects, it includes the name of the neighbourhood or the project name, as well as the phase(s) if applicable / known at the date of the last update of the dataset."
            }
        },
        {
            "id": "Anchor_Address_",
            "type": "text",
            "info": {
                "notes": "Address used to map projects."
            }
        },
        {
            "id": "Ward_Number",
            "type": "text",
            "info": {
                "notes": "Current number of the municipal ward where the project is located."
            }
        },
        {
            "id": "Pgm_Incntve_frst_appr_date",
            "type": "date",
            "format": "%m/%d/%Y",
            "info": {
                "notes": "The initial date when Council approved financial incentives to a project, and where the number of units is stated. If this date is not available, then the date when Council approved the number of units through another type of report (Housing Now, By-Law Zoning) is used"
            }
        },
        {
            "id": "First_Planning_Approval_date",
            "type": "date",
            "format": "%m/%d/%Y",
            "info": {
                "notes": "The date on which the Official Plan and/or Zoning By-law Amendment application was approved in principle by Toronto City Council or the Ontario Land Tribunal (OLT). For Minor Variance applications (MV), the date the application is approved in principle by the Committee of Adjustment or, if appealed, by the Toronto Local Appeal Body (TLAB) or OLT."
            }
        },
        {
            "id": "IBMS_Folder_Num",
            "type": "text",
            "info": {
                "notes": "The Integrated Business Management System (IBMS) number for this project"
            }
        },
        {
            "id": "Most_recent_number_of_affordab",
            "type": "text",
            "info": {
                "notes": "Most up to date number of units based on most recent application received for a project; this only tracks the most recent # for the purpose of the dataset.  In cases where the approval secured a portion of the total residential Gross Floor Area (GFA) as affordable housing and not a specific number of affordable units, the number of units is estimated."
            }
        },
        {
            "id": "Construction_Start_Date",
            "type": "text",
            "info": {
                "notes": "The date a project is assumed to be under construction. For most projects, the date when the first building permit for shoring or a new building is issued by Toronto Building."
            }
        },
        {
            "id": "Actual_Construction_Completion",
            "type": "text",
            "info": {
                "notes": "The date that the units are considered built and are ready for occupancy. The date the first occupancy permit for a development is issued is used to determine that a project is “completed”."
            }
        },
        {
            "id": "Occupancy_Permit_Issued",
            "type": "date",
            "format": "%m/%d/%Y",
            "info": {
                "notes": "The date an occupancy permit is issued for a project; used as proxy to determine 'Actual Construction Completion' for multi-res"
            }
        },
        {
            "id": "Occupancy_Permit_Number",
            "type": "text",
            "info": {
                "notes": "The Occupancy Permit # for a project; used to determine construction completion"
            }
        },
        {
            "id": "Units_Built_Num",
            "type": "text",
            "info": {
                "notes": "The number of units completed"
            }
        },
        {
            "id": "geometry",
            "type": "text",
            "info": {
                "notes": ""
            }
        }
    ]
    test_out_dir = os.path.dirname(os.path.realpath(__file__))
    test_filename = "test_output.csv"

    test_reader = CSVReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = test_out_dir,
        filename = test_filename
    )

    test_reader.write_to_csv(test_reader.stream_from_csv())

    with open(test_filename, "r") as f:
        assert f


