PROJECT_NAME := government_project

up:
	docker-compose -p $(PROJECT_NAME) up -d

down:
	docker-compose -p $(PROJECT_NAME) down

restart:
	docker-compose -p $(PROJECT_NAME) down
	docker-compose -p $(PROJECT_NAME) up -d

logs:
	docker-compose -p $(PROJECT_NAME) logs -f

ps:
	docker-compose -p $(PROJECT_NAME) ps

build:
	docker-compose -p $(PROJECT_NAME) build