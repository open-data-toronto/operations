include .env

setup:
	mkdir ./dags ./logs ./plugins
	docker-compose up airflow-init
	docker-compose up -d
	docker exec airflow-scheduler pip install -r requirements.txt
	
	
down:
	docker-compose down

testing:
	docker exec airflow-scheduler pytest
