"""`tau` lives on `GitHub <http://github.com/halst/tau/>`_."""
from setuptools import setup


setup(
    name="tau",
    version="0.1.0",
    author="Vladimir Keleshev",
    author_email="vladimir@keleshev.com",
    description="Time series database",
    license="GPL",
    keywords="time series database tau",
    url="http://github.com/halst/tau",
    py_modules=['tau'],
    long_description=__doc__,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7"])
