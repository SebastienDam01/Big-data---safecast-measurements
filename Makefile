SHELL = /bin/bash

.DEFAULT_GOAL := all

## help: Display list of commands
.PHONY: help
help: Makefile
	@sed -n 's|^##||p' $< | column -t -s ':' | sed -e 's|^| |'

## all: Run all targets
.PHONY: all
all: init style run

## init: Bootstrap your application.
.PHONY: init
init:
	pre-commit install -t pre-commit
	pipenv install --dev

## format: Format code.
## style: Check lint, code styling rules.
.PHONY: format
.PHONY: style
style format:
	pre-commit run -a

## init-keyspace: Initialise the Cassandra keyspace.
.PHONY: init-keyspace
init-keyspace:
	cqlsh --file=project/init_keyspace.cql

## insert: Insert Measurements data in the keyspace.
.PHONY: insert
insert:
	PYTHONPATH=. pipenv run python project/insert.py

## run: Run the project
.PHONY: run
run:
	PYTHONPATH=. pipenv run python project

## run: Run the "spark file"
.PHONY: spark
spark:
	PYTHONPATH=. pipenv run python project/time_radiations.py

## clean: Remove temporary files
.PHONY: clean
clean:
	-pipenv --rm


# You can add more targets here
