build:
	@poetry build

bump-and-commit: docs
	@cd tools && bash increment-app-version.sh `git branch | grep "*" | awk -F'[*-]' '{ print $$2 }' | tr -d ' '`
	@git add VERSION
	@git commit

docs:
	@cd docs && make clean && make html

install:
	@poetry install

major-branch:
	@test -n "$(NAME)"
	@echo "making major branch \"$(NAME)\""
	@git branch major-$(NAME)
	@git checkout major-$(NAME)

minor-branch:
	@test -n "$(NAME)"
	@echo "making minor branch \"$(NAME)\""
	git branch minor-$(NAME)
	git checkout minor-$(NAME)

patch-branch:
	@test -n "$(NAME)"
	@echo "making patch branch \"$(NAME)\""
	@git branch patch-$(NAME)
	@git checkout patch-$(NAME)

push:
	@git push origin `git branch | grep "*" | awk -F'[*]' '{ print $$2 }' | tr -d ' '`
