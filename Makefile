export DEBUG_MODE=True

cov:
	pytest  -vv -s --cov=mongoie --cov-config=.coveragerc
test-all:
	pytest -vv -s tests
test-integration:
	pytest -vv -s tests/test_integration.py

black:
	black mongoie/ && black tests/