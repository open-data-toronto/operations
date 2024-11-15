import os
import yaml
from pydantic import BaseModel, ValidationError
from typing import List, Literal, Optional, Dict, Union


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


class FORMAT(BaseModel):
    format: Literal[
        "csv",
        "xlsx",
        "json",
        "geojson",
    ]


def validator(
    model: Union[CIVIC_ISSUE, TOPICS, FORMAT], validation_data: Dict, file_name: str
) -> None:
    try:
        model(**validation_data)
    except ValidationError as e:
        print(f"Yaml file: {file_name} \n {validation_data}")
        for error in e.errors():
            print(error)


def main():

    CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
    counter = 0

    print("Validating yamls...")

    for file in sorted(os.listdir(CONFIG_FOLDER)):
        if file.endswith(".yaml"):
            counter += 1

            # load yaml file
            with open(CONFIG_FOLDER + "/" + file, "r") as f:
                yaml_obj = yaml.load(f, yaml.SafeLoader)
                package_name = list(yaml_obj.keys())[0]

                # civic issue validator
                civic_issues = yaml_obj[package_name]["civic_issues"]
                validation_civic_issues = {"civic_issues": civic_issues}
                validator(CIVIC_ISSUE, validation_civic_issues, package_name)

                # topics validator
                topics = yaml_obj[package_name]["topics"]
                validation_topics = {"topics": topics}
                validator(TOPICS, validation_topics, package_name)

                resources_config = yaml_obj[package_name]["resources"]
                for resource in resources_config:

                    # format validator
                    format = resources_config[resource]["format"]
                    validation_format = {"format": format}
                    validator(FORMAT, validation_format, package_name)


if __name__ == "__main__":
    main()
