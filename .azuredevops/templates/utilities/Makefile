.PHONY: lint dist
.DEFAULT_GOAL := help
 
define PRINT_HELP_PYSCRIPT
import re, sys
 
for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT
 
BROWSER := python -c "$$BROWSER_PYSCRIPT"
 
 
help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)
 
lint/pylint: ## check style with flake8
	export workingDirectory=$(workingDirectory);
	touch $(workingDirectory)/notebooks/__init__.py;
	pylint $(workingDirectory)/notebooks/* --recursive=y --rcfile=$(workingDirectory)/.azuredevops/templates/utilities/pylintrc;
	rm $(workingDirectory)/notebooks/__init__.py;
lint/black: ## check style with black
	export workingDirectory=$(workingDirectory);
	black --check $(workingDirectory)/notebooks
lint: lint/pylint lint/black ## check style
 
clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts
 
clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '**.egg-info' -exec rm -fr {} +
	find . -name 'src/*.egg' -exec rm -f {} +
 
clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
 
clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr coverage.xml
	rm -fr junit/
	rm -fr htmlcov/
	rm -fr .pytest_cache
 
dist: clean ## builds source and wheel package #python setup.py sdist
	python setup.py bdist_wheel 
	cp dist/*.whl dist/deep-latest-py3-none-any.whl
	ls -l dist
 
 
databricks_configure_dev: ## configure databricks cli for dev workspace
	databricks auth login --configure-cluster --host https://adb-1386853257882337.17.azuredatabricks.net/