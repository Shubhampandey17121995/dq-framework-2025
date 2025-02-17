from setuptools import setup, find_packages

setup(
	name='dq_framework',
	version='1.0',
	packages=find_packages(),
	install_requires=['pg8000'],
)