ROOT_DIR := $(shell dirname "$(realpath $(firstword $(MAKEFILE_LIST)))")

# for uv .venv creation; also in Dockerfile, .pre-commit-config.yaml
PYTHON=3.13

# NOTE!! mypy now runs from uv .venv

VENVDIR=.venv
VENVBIN=$(VENVDIR)/bin
VENVDONE=$(VENVDIR)/.done
PRE_COMMIT=$(VENVBIN)/pre-commit

## display this message
help:
	@echo  ''
	@echo  'Usage:'
	@echo  'make <target>'
	@echo  ''
	@echo  'Targets:'
	@awk '/^##/{c=substr($$0,3);next}c&&/^[[:alpha:]][[:alnum:]_-]+:/{print substr($$1,1,index($$1,":")),c}1{c=0}' $(MAKEFILE_LIST) | column -s: -t
	@echo  ''

## tidy up local dev environment
clean:
	$(PRE_COMMIT) clean
	rm -rf .venv venv __pycache__ .mypy_cache indexer/__pycache__

# XXX also run (only?) in pre-commit!!!!
## lock requirements
requirements uv.lock: pyproject.toml
	uv lock

## update .pre-commit-config.yaml
update:
	$(PRE_COMMIT) autoupdate

## install all required dependencies for development
install $(VENVDONE): Makefile uv.lock
	uv sync --python $(PYTHON) --extra dev --extra deploy --extra typing
	$(PRE_COMMIT) install
	touch $(VENVDONE)

## check and format code
lint:	$(VENVDONE)
	$(PRE_COMMIT) run --all-files

## deploy using mc-deploy script
deploy:	$(VENVDONE)
	$(VENVBIN)/python docker/deploy.py deploy

## run pytests (not tested!)
test:	$(VENVDONE)
	$(VENVBIN)/pytest

## list outdated packages
list-outdated:
	uv pip list --outdated

#.PHONY: all clean help install install-app-dev install-deps-dev install-deps-prod lint test upgrade upgrade-dev upgrade-prod
