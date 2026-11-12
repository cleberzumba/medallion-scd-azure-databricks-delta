"""
Shared pytest fixtures.

The tests exercise the pipeline scripts exactly as they are in the repository,
with no changes to them:

  * The scripts are named after their execution order (4-silver.py, ...),
    which is not a valid Python module name, so they are loaded by file path.
  * Each script ends with a bare `main()` call. The loader drops that final
    call before executing the file, so loading never runs the pipeline.
  * The functions read and write Delta tables, so the tests run a local
    SparkSession with Delta Lake. The first run downloads the Delta jars from
    Maven Central (internet access required).
  * Locally there is no Unity Catalog: tables live in the default
    `spark_catalog`, in a database named like the project's schema.
"""

import importlib.util
import shutil
import tempfile
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]

LOCAL_CATALOG = "spark_catalog"
SCHEMA = "medallion_scd"


def load_script(relative_path):
    """
    Loads a pipeline script as a module without running its pipeline.

    Only the last non-empty line, the bare `main()` call, is removed.
    Scripts that guard main() with `if __name__ == "__main__"` load as-is.
    """
    path = REPO_ROOT / relative_path
    lines = path.read_text(encoding="utf-8").rstrip().splitlines()
    if lines and lines[-1] == "main()":          # unindented call only
        lines = lines[:-1]

    module_name = path.stem.replace("-", "_")
    spec = importlib.util.spec_from_loader(module_name, loader=None, origin=str(path))
    module = importlib.util.module_from_spec(spec)
    module.__file__ = str(path)
    exec(compile("\n".join(lines) + "\n", str(path), "exec"), module.__dict__)
    return module


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with Delta Lake, shared by the whole test run."""
    warehouse = tempfile.mkdtemp(prefix="medallion-warehouse-")
    builder = (SparkSession.builder
               .master("local[1]")
               .appName("medallion-scd-tests")
               .config("spark.sql.extensions",
                       "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", warehouse)
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC"))
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def schema(spark):
    """A fresh, empty project schema for each test."""
    spark.sql(f"DROP DATABASE IF EXISTS {SCHEMA} CASCADE")
    spark.sql(f"CREATE DATABASE {SCHEMA}")
    spark.sql(f"USE {SCHEMA}")
    yield SCHEMA
    spark.sql("USE default")
    spark.sql(f"DROP DATABASE IF EXISTS {SCHEMA} CASCADE")


def _point_to_local_catalog(module):
    # apply_scd_type1 builds a three-part name from CATALOG; locally the
    # tables live in spark_catalog instead of the Unity Catalog `medallion`.
    module.CATALOG = LOCAL_CATALOG
    return module


@pytest.fixture(scope="session")
def silver():
    return _point_to_local_catalog(load_script("4-silver.py"))


@pytest.fixture(scope="session")
def gold():
    return _point_to_local_catalog(load_script("5-gold.py"))


@pytest.fixture(scope="session")
def dq():
    return load_script("tests/data_quality/checks.py")
