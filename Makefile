.PHONY: python-setup

.ONESHELL:
python-setup:
	echo "creating and activating virtual environment" && \
	python3 -m venv venv && . venv/bin/activate && \
	ACTIVE_VENV=$$(which python3) && echo "Now in venv $$ACTIVE_VENV" && \
	echo "Installing dependencies" && \
	brew list redis || brew install redis && \
	pip3 install -r requirements.txt

.ONESHELL:
run-tests:
	. venv/bin/activate && \
	pytest

create-sample-data:
	echo "Creating sample data" && \
	venv/bin/python3 data_creation/producer.py

run-toy-example:
	echo "starting up Redis server on standard port" && \
	redis-server --port 6379 --daemonize yes && \
	echo "executing toy example loop. Terminate with CTRL+C!" && \
	venv/bin/python3 toy_example/run_toy_example.py

cleanup:
	rm -rf .pytest_cache
	rm -rf toy_example/__pycache__

teardown:
	echo "removing sample data"
	rm -f output/sample_events.json
	echo "tearing down datalake"
	rm -rf output/datalake
	echo "deleting cloudwatch_reports"
	rm -rf output/cloudwatch_reports
	rm -rf event_streaming-venv



