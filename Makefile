SERVICE_DIR=cmd
BIN_DIR=bin
SERVICE_BIN=service
.PHONY: compile clean test

clean:
	rm -fr ./${BIN_DIR}/${SERVICE_BIN}

compile:
	go build -o ${BIN_DIR}/${SERVICE_BIN} ./${SERVICE_DIR}

test:
	go test ./...

build: clean compile

run:
	./${BIN_DIR}/${SERVICE_BIN}
