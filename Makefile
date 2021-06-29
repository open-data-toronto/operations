include .env


setup:
	export AIRFLOW_HOME=~/airflow

	PYTHONPATH=/home/runner/work/operations/operations

	airflow db init

	airflow webserver --port 8080 -D

	airflow scheduler -D

	pwd



down:
	echo "down"

testing:
	python -m pytest