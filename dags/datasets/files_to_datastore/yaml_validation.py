import os
import yaml
from pydantic import BaseModel, ValidationError
from typing import List, Literal, Optional


class CIVIC_ISSUE(BaseModel):
    civic_issues: Optional[
        List[
            Literal[
                "Climate change",
                "Fiscal responsibility",
                "Mobility",
                "Affordable housing",
                "Poverty reduction",
            ]
        ]
    ] = None


class TOPICS(BaseModel):
    topics: Optional[
        List[
            Literal[
                "City government",
                "Locations and mapping",
                "Community services",
                "Transportation",
                "Public safety",
                "Health",
                "Finance",
                "Culture and tourism",
                "Environment",
                "Business",
                "Parks and recreation",
                "Permits and licenses",
                "Water",
                "Garbage and recycling",
                "Development and infrastructure",
            ]
        ]
    ]


CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
counter = 0

for file in sorted(os.listdir(CONFIG_FOLDER)):
    if file.endswith(".yaml"):
        counter += 1

        # load yaml file
        with open(CONFIG_FOLDER + "/" + file, "r") as f:
            yaml_obj = yaml.load(f, yaml.SafeLoader)
            package_name = list(yaml_obj.keys())[0]

            print(f"Processing yaml file {counter}: {package_name}")

            # civic issue validator
            civic_issues = yaml_obj[package_name]["civic_issues"]
            validation_civic_issues = {"civic_issues": civic_issues}

            # topics validator
            topics = yaml_obj[package_name]["topics"]
            validation_topics = {"topics": topics}

            try:
                CIVIC_ISSUE(**validation_civic_issues)
                TOPICS(**validation_topics)

            except ValidationError as e:
                print(f"{validation_civic_issues}\n" f"{validation_topics}")
                print(e)
