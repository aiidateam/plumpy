# Following guide from:
# https://packaging.python.org/tutorials/distributing-packages/

# To clean everything:
# rm -rf dist/ plumpy.egg-info/ build/

python setup.py sdist
python setup.py bdist_wheel

# To upload now run:
# twine upload dist/*

