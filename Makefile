TOKEN=$(SINGLESTORE_TOKEN)
PASSWORD=$(SINGLESTORE_PASSWORD)
CONTAINER_NAME=memsql-ciab
PORT=3306

.PHONY: cli_connect

docker_run:
	docker run -i --init \
		--name ${CONTAINER_NAME} \
		-e LICENSE_KEY=${TOKEN} \
		-e ROOT_PASSWORD=${PASSWORD} \
		-p ${PORT}:${PORT} -p 8080:8080 \
		memsql/cluster-in-a-box

docker_start:
	docker start ${CONTAINER_NAME}

docker_remove:
	docker rm ${CONTAINER_NAME}

docker_stop:
	docker stop ${CONTAINER_NAME}

test: install_deps generate format lint staticcheck  
	go test -v -race -cover ./connectionpool

format:
	gofmt -l ./connectionpool

lint:
	bin/golint ./connectionpool

staticcheck:
	bin/staticcheck ./connectionpool

install_deps:
	go mod vendor
	go build -o bin/mockgen github.com/golang/mock/mockgen
	go build -o bin/golint golang.org/x/lint/golint
	go build -o bin/staticcheck honnef.co/go/tools/cmd/staticcheck
	go build -o bin/staticcheck honnef.co/go/tools/cmd/staticcheck

generate:
	go generate ./...
