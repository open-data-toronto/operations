include .env

setup:
	docker-compose up airflow-init
	
down:
	docker-compose down

testing:
	pytest
