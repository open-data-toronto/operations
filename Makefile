
setup:
	export AIRFLOW_HOME=~/airflow
	export PYTHONPATH=/home/runner/work/operations/operations
	airflow db init
	airflow webserver --port 8080 -D
	airflow scheduler -D
	pwd
	ls -la

down:
	echo "down"

testing:
	pwd
	python --version
	pytest

docs:
	pwd
	ls -la
	cd plugins
	pdoc plugins --html
	pwd

	cd operations
	git status