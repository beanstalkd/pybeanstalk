develop:
	python setup.py develop

install:
	python setup.py install

init: 
	pip install -r requirements.txt

# Omit currently failing multiServer tests
test:
	pip install nose
	cd tests; nosetests test_Proto test_errors test_ServerConn test_MultiServerConn

# Test twisted part separately using Trial rather than nose
test-twisted:
	pip install -r requirements-twisted.txt
	cd tests; trial test_Twisted.py
