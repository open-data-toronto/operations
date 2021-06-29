include .env


setup:
	export AIRFLOW_HOME=~/airflow

	airflow db init

	airflow webserver --port 8080 -D

	airflow scheduler -D

	pwd



down:
	echo "down"

testing:
	pytest