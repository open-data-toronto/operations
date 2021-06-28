include .env

setup:
	pip install -r requirements.txt
	docker-compose up airflow-init
	
down:
	docker-compose down

testing:
	pytest
