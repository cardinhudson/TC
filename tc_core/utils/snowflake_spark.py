from __future__ import annotations

import os
from typing import Any, Iterable, Sequence

DEFAULT_SNOWFLAKE_URL = "rr67688.north-europe.azure.snowflakecomputing.com"
DEFAULT_SNOWFLAKE_USER = "U235107@INETPSA.COM"
DEFAULT_SNOWFLAKE_ROLE = "ZSNF.FINdtsana"
DEFAULT_SNOWFLAKE_WAREHOUSE = "WH_LAB_FIN"
DEFAULT_SNOWFLAKE_DATABASE = "DB_BL_FIN"
DEFAULT_SNOWFLAKE_SOURCE = "snowflake"


def get_snowflake_spark_options(
    *,
    schema: str = "PUBLIC",
    oauth_token: str | None = None,
    extra_options: dict[str, str] | None = None,
) -> dict[str, str]:
    """Retorna opções padrão para o conector Spark-Snowflake com OAuth.

    O token é opcional porque alguns ambientes corporativos resolvem o fluxo
    OAuth diretamente no conector/compute. Se houver token em ambiente, ele é
    propagado automaticamente.
    """
    token = oauth_token or os.environ.get("SNOWFLAKE_OAUTH_TOKEN") or os.environ.get("SF_OAUTH_TOKEN")

    options: dict[str, str] = {
        "sfUrl": DEFAULT_SNOWFLAKE_URL,
        "sfUser": DEFAULT_SNOWFLAKE_USER,
        "sfRole": DEFAULT_SNOWFLAKE_ROLE,
        "sfWarehouse": DEFAULT_SNOWFLAKE_WAREHOUSE,
        "sfDatabase": DEFAULT_SNOWFLAKE_DATABASE,
        "sfSchema": schema,
        "authentication": "oauth",
        "sfAuthenticator": "oauth",
    }

    if token:
        options["sfToken"] = token

    if extra_options:
        options.update({key: str(value) for key, value in extra_options.items()})

    return options


def read_snowflake_query(
    spark: Any,
    query: str,
    *,
    schema: str = "PUBLIC",
    oauth_token: str | None = None,
    extra_options: dict[str, str] | None = None,
):
    """Executa uma query no Snowflake via Spark connector e retorna DataFrame Spark."""
    options = get_snowflake_spark_options(
        schema=schema,
        oauth_token=oauth_token,
        extra_options=extra_options,
    )
    return (
        spark.read.format(DEFAULT_SNOWFLAKE_SOURCE)
        .options(**options)
        .option("query", query)
        .load()
    )


def smoke_test_snowflake_oauth(
    spark: Any,
    *,
    schema: str = "PUBLIC",
    oauth_token: str | None = None,
    extra_options: dict[str, str] | None = None,
):
    """Smoke test de conectividade OAuth via Spark-Snowflake."""
    query = (
        "SELECT CURRENT_USER() AS USER_NAME, CURRENT_ROLE() AS ROLE_NAME, "
        "CURRENT_WAREHOUSE() AS WAREHOUSE_NAME, CURRENT_DATABASE() AS DB_NAME, "
        "CURRENT_SCHEMA() AS SCHEMA_NAME, 1 AS TESTE"
    )
    return read_snowflake_query(
        spark,
        query,
        schema=schema,
        oauth_token=oauth_token,
        extra_options=extra_options,
    )


def _qualified_table_name(table_name: str, *, schema: str, database: str = DEFAULT_SNOWFLAKE_DATABASE) -> str:
    return f'{database}.{schema}."{table_name}"'


def build_delete_preactions_for_years(
    table_name: str,
    years: Iterable[int],
    *,
    schema: str = "SCI_CURATED",
    database: str = DEFAULT_SNOWFLAKE_DATABASE,
) -> str:
    statements: list[str] = []
    for year in sorted({int(year) for year in years}):
        statements.append(f'DELETE FROM {_qualified_table_name(table_name, schema=schema, database=database)} WHERE "Ano" = {year}')
    return "; ".join(statements)


def write_snowflake_dataframe(
    dataframe: Any,
    table_name: str,
    *,
    schema: str = "SCI_CURATED",
    mode: str = "append",
    oauth_token: str | None = None,
    preactions: str | None = None,
    postactions: str | None = None,
    extra_options: dict[str, str] | None = None,
) -> None:
    """Escreve um DataFrame Spark no Snowflake via conector Spark-Snowflake."""
    options = get_snowflake_spark_options(
        schema=schema,
        oauth_token=oauth_token,
        extra_options=extra_options,
    )
    writer = (
        dataframe.write.format(DEFAULT_SNOWFLAKE_SOURCE)
        .options(**options)
        .option("dbtable", table_name)
        .mode(mode)
    )

    if preactions:
        writer = writer.option("preactions", preactions)
    if postactions:
        writer = writer.option("postactions", postactions)

    writer.save()


def bootstrap_schema_objects(
    spark: Any,
    tables: Sequence[str],
    *,
    target_schema: str = "SCI_CURATED",
    oauth_token: str | None = None,
    extra_options: dict[str, str] | None = None,
) -> None:
    """Cria schema e tabelas placeholder via preactions do conector Spark-Snowflake.

    Usa a schema PUBLIC como staging operacional, evitando depender de DDL via
    conector Python ou secrets.
    """
    ddl_statements = [f"CREATE SCHEMA IF NOT EXISTS {DEFAULT_SNOWFLAKE_DATABASE}.{target_schema}"]
    ddl_statements.extend(
        f'CREATE TABLE IF NOT EXISTS {_qualified_table_name(table_name, schema=target_schema)} ("_PLACEHOLDER" STRING)'
        for table_name in tables
    )

    dummy_df = spark.createDataFrame([("setup",)], ["_PLACEHOLDER"])
    write_snowflake_dataframe(
        dummy_df,
        "SCI_SETUP_NOP",
        schema="PUBLIC",
        mode="append",
        oauth_token=oauth_token,
        preactions="; ".join(ddl_statements),
        postactions=f'DROP TABLE IF EXISTS {_qualified_table_name("SCI_SETUP_NOP", schema="PUBLIC")}',
        extra_options=extra_options,
    )