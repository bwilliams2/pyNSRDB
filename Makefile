
buildNewRelease:
	python setup.py sdist bdist_wheel
	twine upload -r pyNSRDB dist/*
