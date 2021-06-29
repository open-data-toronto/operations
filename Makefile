include .env

setup:
	docker-compose up airflow-init
	docker-compose up -d
	docker exec operations_airflow-scheduler_1 pip install -r requirements.txt
	
	
down:
	docker-compose down

testing:
	docker exec operations_airflow-scheduler_1 pytest
