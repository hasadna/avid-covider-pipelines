import datetime
from setuptools import setup, find_packages


setup(
    name="datapackage_pipelines_covid19israel",
    version=datetime.datetime.now().timestamp(),
    packages=find_packages(exclude=['examples', 'tests', '.tox']),
)
