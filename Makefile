include .env

setup:
	docker-compose up --detach
	pip install -r requirements.txt
	
	
down:
	docker-compose down

testing:
	pytest
