include .env


setup:
	export AIRFLOW_HOME=~/airflow

	export PYTHONPATH=/home/runner/work/operations/operations

	airflow db init

	airflow webserver --port 8080 -D

	airflow scheduler -D

	pwd

	ls -la

	airflow jobs check --job-type SchedulerJob --allow-multiple --limit 100

	airflwo db check



down:
	echo "down"

testing:
	pwd
	python --version
	pytest --import-mode=importlib