install-dependencies:
	python -m pip install --upgrade pip
	pip install pytest
	if [ -f requirements.txt ]; then pip install -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.7.txt"; fi


setup:
	export AIRFLOW_HOME=~/airflow
	export PYTHONPATH=/home/runner/work/operations/operations
	airflow db init
	airflow webserver --port 8080 -D
	airflow scheduler -D

testing:
	python --version
	pytest

docs:
	cd plugins
	pdoc plugins --html
	mv html/plugins /home/runner/work/operations/operations/docs

docs-commit:
	git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"
	git config --local user.name "github-actions[bot]"
	git commit -m "Add changes to docs" -a