#!/bin/bash

postgres:
	docker run --rm --name testdb -e POSTGRES_USER=testuser -e POSTGRES_PASSWORD=password -e POSTGRES_DB=testdb -p 5432:5432 postgres -c 'max_connections=200'

stress:
	go run ../cmd/sqlstress --config ./config.yaml