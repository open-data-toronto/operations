include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	
down:
	docker-compose down

testing:
	docker exec airflow pytest -v
