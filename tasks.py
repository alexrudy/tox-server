# type: ignore
from invoke import task


@task
def clean_build(c):
    """remove Python build artifacts"""
    c.run("rm -fr build/")
    c.run("rm -fr dist/")
    c.run("rm -fr .eggs/")
    c.run("find . -name '*.egg-info' -exec rm -fr {} +")
    c.run("find . -name '*.egg' -exec rm -f {} +")


@task
def clean_pyc(c):
    """remove Python file artifacts"""
    c.run("find . -name '*.pyc' -exec rm -f {} +")
    c.run("find . -name '*.pyo' -exec rm -f {} +")
    c.run("find . -name '*~' -exec rm -f {} +")
    c.run("find . -name '__pycache__' -exec rm -fr {} +")


@task
def clean_tests(c):
    """remove test and coverage artifacts"""
    c.run("rm -fr .tox/")
    c.run("rm -f .coverage")
    c.run("rm -fr htmlcov/")
    c.run("rm -fr .pytest_cache")
    c.run("rm -fr .mypy_cache")


@task(clean_build, clean_pyc, clean_tests)
def clean(c):
    """clean everything"""
    pass


@task(clean)
def dist(c):
    """build distributions"""
    c.run("python setup.py sdist bdist_wheel")


@task(dist)
def release(c, test=True):
    """release to pypi"""
    if test:
        c.run("twine upload --repository testpypi dist/*")
    else:
        c.run("twine upload dist/*")


@task
def test(c):
    """run tests"""
    c.run("pytest")


@task(name="test-all")
def test_all(c):
    """run all tests via tox"""
    c.run("tox")
