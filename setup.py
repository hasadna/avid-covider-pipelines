import datetime
from setuptools import setup, find_packages


setup(
    name="avid_covider_pipelines",
    version=datetime.datetime.now().timestamp(),
    packages=find_packages(exclude=['examples', 'tests', '.tox']),
)
