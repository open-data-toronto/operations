
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
	mv html/plugins /home/runner/work/operations/operations/docs

	git status
	git branch