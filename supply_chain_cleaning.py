"""Module for cleaning supply chain data."""

import hashlib
from pathlib import Path

import pandas as pd
from recordlinkage.preprocessing import clean  # type: ignore

OUTPUT_FILE_DIRECTORY = "/output/csvs/"


# TODO: Right now categories of subsidiaries are left blank but should they be the same category as the parent company?
def clean_suppliers(
    supplier_source: str,
) -> pd.DataFrame:  # TODO change when finished
    """
    Process and normalize supplier data based on the source

    Parameters:
        supplier_source (str): The source of the data. Must be either 'rspo' or 'uml'.

    Raises:
        ValueError: If data_source is not 'rspo' or 'uml'
    """  # noqa: D202, D212

    # Processing steps depend on source
    supplier_source = supplier_source.lower()
    if supplier_source not in ["rspo", "uml"]:
        raise ValueError("data_source must be either 'rspo' or 'uml'")

    elif supplier_source == "rspo":
        raw_rspo_df = pd.read_csv("/project/data/csv/rspo_members_raw.csv")

        # Clean name, parent company and country fields using clean from record linkage
        raw_rspo_df["name"] = clean(raw_rspo_df["name"])
        raw_rspo_df["parent_company"] = clean(raw_rspo_df["parent_company"].fillna(""))
        raw_rspo_df["country"] = clean(raw_rspo_df["country"]).astype(str).str.title()

        # Create unique ID
        raw_rspo_df["id"] = (
            raw_rspo_df["name"]
            .astype(str)
            .str.cat(raw_rspo_df["country"].astype(str))
            .map(lambda x: hashlib.sha256(x.encode()).hexdigest()[:12])
        )
        raw_rspo_df["id_type"] = "Created"
        raw_rspo_df["class"] = ""

        return raw_rspo_df

    else:  # Load raw UML
        uml_file = Path.glob("/project/data/csv/UML*.csv")
        if uml_file:
            raw_uml_df = pd.read_csv(uml_file[0], encoding="latin1")

        # Rename fields
        raw_uml_df.rename(
            columns={
                "Group Name": "group_name",
                "Parent Company": "parent_company",
                "Country": "country",
                "Mill Name": "mill_name",
            },
            inplace=True,  # noqa: PD002
        )

        # Clean group name, parent company and country fields using clean from record linkage
        raw_uml_df["group_name"] = clean(raw_uml_df["group_name"])
        raw_uml_df["parent_company"] = clean(raw_uml_df["parent_company"])
        raw_uml_df["country"] = clean(raw_uml_df["country"]).astype(str).str.title()
        raw_uml_df["mill_name"] = clean(raw_uml_df["mill_name"])

        # Create unique ID
        raw_uml_df["id"] = (
            raw_uml_df["mill_name"]
            .astype(str)
            .str.cat(raw_uml_df["country"].astype(str))
            .map(lambda x: hashlib.sha256(x.encode()).hexdigest()[:12])
        )
        raw_uml_df["id_type"] = "Created"
        raw_uml_df["class"] = ""

        return raw_uml_df
