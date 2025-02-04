import os
from pathlib import Path
import subprocess

from controller.data_source.data_source import (
    get_data_source_and_data_catalog_item_id,
    get_table_preview,
)
import database.crud.common as crud
from database.database import get_db
import database.models.data_block as models
from database.schemas.data_block import DataBlockCreate, DataBlockUpdate
from fastapi import Depends
import oyaml as yaml
from sqlalchemy.orm import Session


def generate_model_name(name: str):
    return "_".join(map(lambda n: n.lower(), name.split()))


def create_source_yaml(dbt_dir: str, schema_name: str):
    dbt_source_model_dir = dbt_dir + f"/models/staging/{schema_name}"

    if os.path.exists(dbt_source_model_dir):
        return

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


def create_base_model(dbt_dir: str, schema_name: str, table_name: str):
    base_model_name = f"stg_{schema_name}_{table_name}"
    dbt_base_model_path = (
        f"{dbt_dir}/models/staging/{schema_name}/{base_model_name}.sql"
    )

    if os.path.exists(dbt_base_model_path):
        return base_model_name

    args = dict(source_name=schema_name, table_name=table_name)
    output = subprocess.run(
        f"dbt run-operation generate_base_model --args '{args}' --profiles-dir .",
        cwd=dbt_dir,
        shell=True,
        capture_output=True,
    )
    base_model = (
        f"with source{output.stdout.decode('utf8').split('with source')[1][:-5]}"
    )

    with open(dbt_base_model_path, "w+") as file:
        file.write(base_model)
        file.close()

    return base_model_name


def create_model(
    dbt_dir: str, name: str, schema_name: str, table_name: str, columns: list[str]
):
    if not columns:
        columns = "*"
    else:
        columns = ", ".join(columns)

    model = """
        SELECT columns
        FROM {{ ref('stg_schema_name_table_name') }}
    """
    model = model.replace("columns", columns)
    model = model.replace("schema_name", schema_name)
    model = model.replace("table_name", table_name)
    model_name = generate_model_name(name=name)
    model_dir = f"{dbt_dir}/models/marts/{schema_name}"

    Path(model_dir).mkdir(parents=True, exist_ok=True)

    with open(f"{model_dir}/{model_name}.sql", "w+") as file:
        file.write(model)
        file.close()

    return model_name


def create_model_yaml(dbt_dir: str, schema_name: str, model_name: str):
    args = dict(model_name=model_name)
    output = subprocess.run(
        f"dbt run-operation generate_model_yaml --args '{args}' --profiles-dir .",
        cwd=dbt_dir,
        shell=True,
        capture_output=True,
    )
    source_yml = yaml.safe_load(
        f"version{output.stdout.decode('utf8').split('version')[1][:-5]}"
    )

    with open(f"{dbt_dir}/models/marts/{schema_name}/{model_name}.yml", "w+") as file:
        yaml.safe_dump(source_yml, file, indent=4)
        file.close()


def create_data_block(
    data_block: DataBlockCreate,
    db: Session = Depends(get_db),
):
    _, data_catalog_item_id = get_data_source_and_data_catalog_item_id(
        data_source_id=data_block.data_source_id, db=db
    )
    schema_name = data_block.schema_name
    table_name = data_block.table_name

    if data_catalog_item_id == "bigquery":
        schema_name = data_block.dataset_name

    if data_catalog_item_id == "snowflake":
        schema_name = data_block.schema_name.lower()
        table_name = data_block.table_name.lower()

    script_dir = os.path.dirname(__file__)
    dbt_dir = os.path.join(
        script_dir, f"../../../../tmp/kuwala/backend/dbt/{data_block.data_source_id}"
    )

    create_source_yaml(dbt_dir=dbt_dir, schema_name=schema_name)

    base_model_name = create_base_model(
        dbt_dir=dbt_dir, schema_name=schema_name, table_name=table_name
    )
    model_name = create_model(
        dbt_dir=dbt_dir,
        name=data_block.name,
        schema_name=schema_name,
        table_name=table_name,
        columns=data_block.columns,
    )

    subprocess.call(
        f"dbt run --select {base_model_name} {model_name} --profiles-dir .",
        cwd=dbt_dir,
        shell=True,
    )
    create_model_yaml(dbt_dir=dbt_dir, schema_name=schema_name, model_name=model_name)

    return model_name


def update_data_block_name(
    db: Session, dbt_dir: str, data_block: models.DataBlock, updated_name: str
) -> models.DataBlock:
    updated_model_name = generate_model_name(updated_name)
    dbt_model_dir = f"{dbt_dir}/models/marts/{data_block.schema_name}"
    updated_yaml_path = f"{dbt_model_dir}/{updated_model_name}.yml"

    # Rename SQL and YAML files
    os.rename(
        f"{dbt_model_dir}/{data_block.dbt_model}.sql",
        f"{dbt_model_dir}/{updated_model_name}.sql",
    )
    os.rename(
        f"{dbt_model_dir}/{data_block.dbt_model}.yml",
        updated_yaml_path,
    )

    # Update model name in YAML file
    with open(updated_yaml_path, "r") as read_file:
        model_yml = yaml.safe_load(read_file)
        model_yml["models"][0]["name"] = updated_model_name

        read_file.close()

        with open(updated_yaml_path, "w") as write_file:
            yaml.safe_dump(model_yml, write_file, indent=4)
            write_file.close()

    return crud.update_attributes(
        db=db,
        db_object=data_block,
        attributes=[
            dict(name="dbt_model", value=updated_model_name),
            dict(name="name", value=updated_name),
        ],
    )


def update_data_block_columns(
    db: Session, dbt_dir: str, data_block: models.DataBlock, updated_columns: [str]
):
    create_model(
        dbt_dir=dbt_dir,
        name=data_block.dbt_model,
        schema_name=data_block.schema_name,
        table_name=data_block.table_name,
        columns=updated_columns,
    )
    subprocess.call(
        f"dbt run --select {data_block.dbt_model} --profiles-dir .",
        cwd=dbt_dir,
        shell=True,
    )
    create_model_yaml(
        dbt_dir=dbt_dir,
        schema_name=data_block.schema_name,
        model_name=data_block.dbt_model,
    )

    return crud.update_attributes(
        db=db,
        db_object=data_block,
        attributes=[dict(name="columns", value=updated_columns)],
    )


def update_data_block(
    data_block: DataBlockUpdate,
    db: Session = Depends(get_db),
):
    script_dir = os.path.dirname(__file__)
    db_data_block = crud.get_object_by_id(
        db=db, model=models.DataBlock, object_id=data_block.id
    )
    dbt_dir = os.path.join(
        script_dir,
        f"../../../../tmp/kuwala/backend/dbt/{db_data_block.data_source_id}",
    )

    if data_block.name:
        db_data_block = update_data_block_name(
            db=db,
            dbt_dir=dbt_dir,
            data_block=db_data_block,
            updated_name=data_block.name,
        )

    if data_block.columns:
        db_data_block = update_data_block_columns(
            db=db,
            dbt_dir=dbt_dir,
            data_block=db_data_block,
            updated_columns=data_block.columns,
        )

    return db_data_block


def get_data_block_preview(
    data_block_id: str,
    limit_columns: int = None,
    limit_rows: int = None,
    db: Session = Depends(get_db),
):
    data_block = crud.get_object_by_id(
        db=db, model=models.DataBlock, object_id=data_block_id
    )
    data_source, _ = get_data_source_and_data_catalog_item_id(
        db=db, data_source_id=data_block.data_source_id
    )

    return get_table_preview(
        data_source_id=data_source.id,
        schema_name="dbt_kuwala",
        dataset_name="dbt_kuwala",
        table_name=data_block.dbt_model,
        columns=None,
        limit_columns=limit_columns,
        limit_rows=limit_rows,
        db=db,
    )
