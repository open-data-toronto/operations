include .env


setup:
	export AIRFLOW_HOME=~/airflow

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