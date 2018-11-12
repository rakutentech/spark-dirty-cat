SHELL = /bin/bash
VERSION = $(shell cat version.txt)

.PHONY: clean clean-pyc clean-dist

clean: clean-dist clean-pyc

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-dist:
	rm -rf target
	rm -rf python/build/
	rm -rf python/*.egg-info


publish: clean
	# use spark packages to create the distribution
	sbt clean
	sbt compile
	sbt package

	cd python; python setup.py sdist

