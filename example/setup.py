# Always prefer setuptools over distutils
from os import path

from setuptools import find_packages
from setuptools import setup

here = path.abspath(path.dirname(__file__))


setup(
    name="frobulator",
    version="0.1",
    author="Alex Rudy",
    author_email="alex.rudy@gmail.com",
    description="An example package to demonstrate tox-server",
    long_description="Frobulator does the frobulation",
    long_description_content_type="text/markdown",
    url="https://github.com/alexrudy/tox-server/example/",
    packages=find_packages(),
    python_requires=">=3.6, <4",
)
