from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="nempy",
    version="1.0.0",
    description="A flexible tool kit for modelling Australia's National Electricity Market dispatch procedure.",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    url="https://github.com/UNSW-CEEM/nempy",
    author="Nicholas Gorman",
    author_email="n.gorman@unsw.edu.au",

    install_requires=[
        "pandas>=1.0.0, <2.0.0",
        "mip>=1.11.0, <2.0.0",
        "xmltodict==0.12.0",
        "requests>=2.0.0, <3.0.0"
    ],

    extras_require={
        "dev": [
            "pytest==6.0.1",
            "twine",
        ],
    },
)