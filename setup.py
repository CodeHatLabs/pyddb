import os
from setuptools import setup, find_packages


setup(
    name='pyddb',
    version=0.0,
    author='Code Hat Labs, LLC',
    author_email='dev@codehatlabs.com',
    url='https://github.com/CodeHatLabs/pyddb',
    description='Python tools for DynamoDB',
    packages=find_packages(),
    long_description="",
    keywords='python dynamodb',
    zip_safe=False,
    install_requires=[
        'boto3',
        'pypool @ git+https://github.com/CodeHatLabs/pypool',
    ],
    test_suite='',
    include_package_data=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
