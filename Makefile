include .env

setup:
	docker-compose up airflow-init
	docker-compose up
	
down:
	docker-compose down

testing:
	pytest
