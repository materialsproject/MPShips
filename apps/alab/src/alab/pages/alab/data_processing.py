from mpships_infra import get_rester
from .utils import parse_formula

import time

def load_experiment_list():
    """Load list of all experiments from Parquet data"""
    try:
        kwargs = {"fields": ["identifier", "formula"]}
        exp_list = (
            get_rester(project="A_Lab").contribs.query_contributions(**kwargs).data
        )
        return exp_list
    except Exception:
        return []


def load_experiment_data(experiment_name):
    """Load data for a specific experiment from Parquet files

    Note: SEM-EDS analysis data is not included in the Parquet pipeline.
    SEM data must be accessed separately from individual experiment folders.
    The dashboard will show a notice whenload_experiment_data SEM data is not available.
    """
    kwargs = {"query": {"identifier": experiment_name}, "fields": ["_all"]}
    raw_data = get_rester(
        use_document_model=False, project="A_Lab"
    ).contribs.query_contributions(**kwargs)["data"][0]

    # get "Sample preparation", "Heating", and "Characterization" tables
    table_field_map = {
        "Sample preparation": "_sample_preparation", 
        "Heating": "_heating",
        "Characterization": "_characterization"
    }

    for table in raw_data["tables"]:
        table_name = table["name"]
        if table_name in table_field_map:
            table_md5 = table["md5"]
            temp = get_rester(project="A_Lab").contribs.get_table(table_md5)
            raw_data[table_field_map[table_name]] = temp.to_dict()

    # Parse formula
    target = raw_data.get("formula", {})
    composition = parse_formula(target)
    total = sum(composition.values())
    percentages = {el: (amt / total) * 100 for el, amt in composition.items()}
    raw_data["composition"] = {
        "elements": composition,
        "total_atoms": total,
        "percentages": percentages,
    }


    return raw_data


def analyze_data(raw_data):
    """Analyze sample data and extract key metrics"""
    data = raw_data.get("data", {})
    target = data.get("globalFormula", "")
    powders = raw_data.get("powerRecovery", {})

    # Parse formula
    composition = parse_formula(target)
    total = sum(composition.values())
    percentages = {el: (amt / total) * 100 for el, amt in composition.items()}

    return {
        "sample_info": {
            "name": raw_data["identifier"],
            "id": raw_data["identifier"],
            # 'last_updated': raw_data['last_updated'],
            "target_formula": target,
        },
        "target_composition": {
            "elements": composition,
            "total_atoms": total,
            "percentages": percentages,
        }
    }