
buildNewRelease:
	flake8 ./pyNSRDB
	# python -m pytest ./tests
	python setup.py sdist bdist_wheel
	twine upload -r pyNSRDB dist/*
