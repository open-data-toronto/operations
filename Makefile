include .env


setup:
	export AIRFLOW_HOME=~/airflow

	PYTHONPATH=.

	airflow db init

	airflow webserver --port 8080 -D

	airflow scheduler -D

	pwd



down:
	echo "down"

testing:
	cd tests
	python -m pytest