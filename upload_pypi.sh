#!/bin/bash
rm -rf dist
rm -rf coralinedb.egg-info
python setup.py sdist
twine upload dist/*