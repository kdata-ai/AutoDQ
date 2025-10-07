"""
# Copied from lines 24-61 of AutoDQ/01_setup_environment.py
"""
# Databricks notebook source
import logging, warnings
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults

logger = logging.getLogger("GXValidationLogger")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)

warnings.filterwarnings("ignore")
logger.info("🔧 Logger initialized successfully")

#  GE Context
config = DataContextConfig(
    config_version=3,
    expectations_store_name="expectations_store",
    stores={
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "module_name": "great_expectations.data_context.store",
            "store_backend": {"class_name": "InMemoryStoreBackend"}
        },
        "default_validation_results_store": {
            "class_name": "ValidationsStore",
            "module_name": "great_expectations.data_context.store",
            "store_backend": {"class_name": "InMemoryStoreBackend"}
        }
    },
    data_docs_sites={},
    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/tmp/gx")
)

context = BaseDataContext(project_config=config)


