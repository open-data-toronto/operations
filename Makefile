include .env


setup:
	export AIRFLOW_HOME=~/airflow

	export PYTHONPATH=/home/runner/work/operations/operations

	airflow db init

	airflow webserver --port 8080 -D

	airflow scheduler -D

	pwd

	ls -la



down:
	echo "down"

testing:
	pwd
	python --version
	python -m pytest