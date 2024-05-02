from setuptools import find_packages, setup

setup(
    name="assets",
    packages=find_packages(exclude=["assets_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "trino"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
