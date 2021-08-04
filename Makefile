
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
	pdoc /home/runner/work/operations/operations/plugins --html
	mv html/plugins operations/docs
	ls operations/docs

	cd operations
	git status