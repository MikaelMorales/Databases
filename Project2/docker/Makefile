build: build-base build-slave build-master

build-base:
	docker build -t diasbase base

build-slave:
	docker build -t diasslave cs422-pr2

build-master:
	docker build -t diasmaster cs422-pr2/master

cluster-up:
	docker-compose -f cs422-pr2/docker-compose.yml up

cluster-down:
	docker-compose -f cs422-pr2/docker-compose.yml down
