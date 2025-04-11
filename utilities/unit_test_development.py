# Databricks notebook source
# MAGIC %sh
# MAGIC cd ..
# MAGIC cp -R .azuredevops/templates/utilities/pytest.ini pytest.ini
# MAGIC cp -R .azuredevops/templates/utilities/conftest.py tests/conftest.py

# COMMAND ----------

pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pytest
import os
import sys

sys.dont_write_bytecode = True
try:
  pytest.main(["../tests/", "-p","no:cacheprovider","-s","-vv"])
except Exception as e:
  print(str(e))

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ..
# MAGIC rm pytest.ini
# MAGIC rm tests/conftest.py
