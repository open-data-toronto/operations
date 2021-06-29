include .env

setup:
	export AIRFLOW_HOME=~/airflow

	AIRFLOW_VERSION=2.1.0
	PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
	CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
	pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
	pip install -r requirements.txt

	airflow db init

	airflow users create \
		--username admin \
		--firstname Peter \
		--lastname Parker \
		--role Admin \
		--email spiderman@superhero.org

	airflow webserver --port 8080

	airflow scheduler

	pwd
	ll
	cd \
	ll


down:
	echo "down"

testing:
	pytest