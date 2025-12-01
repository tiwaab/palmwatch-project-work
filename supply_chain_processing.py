"""Module for processing supply chain data."""

import sys

import pandas as pd
import supply_chain_cleaning as supply

sys.path.append("..")

OUTPUT_PATH = "/project/output/supply_chain_entities.csv"


def classify_uml(row: pd.Series) -> str:
    """Function to classify entities from the UML table"""
    if row["original_field"] == "group_name":
        return "Group"
    elif row["original_field"] == "parent_company":
        return "Parent Company"
    else:
        return "Mill"


def classify_rspo(row: pd.Series) -> str:
    """Function to classify entities from the RSPO table. No logic yet so they will be classified as unclassified"""
    return "Unclassified"


def process_supply_chain() -> None:
    """Function to hold all the processing steps"""
    # Load clean data from sources
    rspo = supply.clean_suppliers("rspo")
    uml = supply.clean_suppliers("uml")

    # Melt UML so that group name, parent company and mill name each get their own row
    to_melt = ["group_name", "parent_company", "mill_name"]
    id_vars = [col for col in uml.columns if col not in to_melt]

    melted_df = pd.melt(
        uml,
        id_vars=id_vars,
        value_vars=to_melt,
        var_name="original_field",
        value_name="name",
    )

    # Apply classification
    melted_df["class"] = melted_df.apply(classify_uml, axis=1)
    rspo["class"] = rspo.apply(classify_rspo, axis=1)

    # Stack both RSPO and UML
    supply_chain_entities = pd.concat([rspo, melted_df], ignore_index=True)

    # Sort so that classified rows come first
    supply_chain_entities = supply_chain_entities.sort_values(
        by="class", ascending=False, key=lambda x: x != "Unclassified"
    )

    # Drop duplicates, keeping the first (classified) one
    clean_supply_chain_entities = supply_chain_entities.drop_duplicates(
        subset="name", keep="first"
    )

    # Limit it to relevant fields
    clean_supply_chain_entities = clean_supply_chain_entities[
        ["id", "id_type", "name", "country", "class"]
    ]

    # Export cleaned entity list
    clean_supply_chain_entities.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    process_supply_chain()
