include .env


setup:
	export AIRFLOW_HOME=~/airflow

	export PYTHONPATH=/home/runner/work/operations/operations

	airflow db init

	airflow webserver --port 8080 -D

	airflow scheduler -D

	pwd

	ls -la

	airflow scheduler status



down:
	echo "down"

testing:
	pwd
	python --version
	pytest --import-mode=importlib