import os
from pathlib import Path
import subprocess
from typing import Optional

from controller.data_source.data_source import get_data_source_and_data_catalog_item_id
from database.database import get_db
from fastapi import Depends, Query
import oyaml as yaml
from sqlalchemy.orm import Session


def create_source_yml(dbt_dir: str, schema_name: str):
    dbt_source_model_dir = dbt_dir + f"/models/staging/{schema_name}"

    if not os.path.exists(dbt_source_model_dir):
        args = dict(
            schema_name=schema_name, generate_columns=True, include_descriptions=True
        )
        output = subprocess.run(
            f"dbt run-operation generate_source --args '{args}' --profiles-dir .",
            cwd=dbt_dir,
            shell=True,
            capture_output=True,
        )
        source_yml = yaml.safe_load(
            f"version{output.stdout.decode('utf8').split('version')[1][:-5]}"
        )

        Path(dbt_source_model_dir).mkdir(parents=True, exist_ok=True)

        with open(f"{dbt_source_model_dir}/src_{schema_name}.yml", "w+") as file:
            yaml.safe_dump(source_yml, file, indent=4)
            file.close()


def create_data_block(
    name: str,
    data_source_id: str,
    table_name: str,
    schema_name: str = None,
    dataset_name: str = None,
    columns: Optional[list[str]] = Query(None),
    db: Session = Depends(get_db),
):
    _, data_catalog_item_id = get_data_source_and_data_catalog_item_id(
        data_source_id=data_source_id, db=db
    )

    if data_catalog_item_id == "bigquery":
        schema_name = dataset_name

    script_dir = os.path.dirname(__file__)
    dbt_dir = os.path.join(
        script_dir, f"../../../../tmp/kuwala/backend/dbt/{data_source_id}"
    )

    create_source_yml(dbt_dir=dbt_dir, schema_name=schema_name)

    return "yes"
