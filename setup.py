"""
Setup.py is used to be able to install the package into an environment if needed.
"""

from setuptools import find_packages, setup
import io

setup(
    name='amyloidosis_prediction',
    version='0.1.0',
    description='Amyloidois prediction project data manipulation and modelling',
    long_description=io.open('README.md', 'r', encoding='utf-8').read(),
    classifiers=[''],
    keywords='',
    author='Soren Solari',
    author_email='sorensolari@gmail.com',
    url='',
    license='',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'gensim',
        'numpy',
        'tables',
        'sklearn',
        'ujson',
    ],
    entry_points={
        'console_scripts': [
        ],
    },
    package_data={
        'amyloidosis_prediction': ['cms/data/*.txt',],
    }
)