include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 240
	docker exec airflow airflow users create --username admin --password admin --role Admin --firstname open --lastname data --email admin@email.com

down:
	docker-compose down

testing:
	docker exec airflow pytest -v
