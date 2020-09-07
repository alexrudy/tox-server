# Always prefer setuptools over distutils
from os import path

from setuptools import find_packages
from setuptools import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="tox_server",
    version="0.5.0",
    author="Alex Rudy",
    author_email="alex.rudy@gmail.com",
    description="A mini tox server for calling tox in a loop",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alexrudy/tox-server",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Environment :: Console",
        "License :: OSI Approved :: BSD License",
        "Operating System :: Unix",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Utilities",
    ],
    keywords="utilties tox testing",
    packages=find_packages(),
    python_requires=">=3.7, <4",
    entry_points={"console_scripts": ["tox-server = tox_server:main"]},
    install_requires=["click", "pyzmq", "tox"],
)
