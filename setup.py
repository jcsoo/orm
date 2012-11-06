from setuptools import setup, find_packages

setup(
   name = 'orm',
   version = '0.30',
   packages = find_packages(),
   test_suite = 'orm.tests',
   install_requires = ['psycopg2']
)
