include .env

setup:
	docker-compose up airflow-init --detach
	pip install -r requirements.txt
	
	
down:
	docker-compose down

testing:
	pytest
