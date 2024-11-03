from setuptools import find_packages, setup

setup(
    name="assets",
    packages=find_packages(exclude=["assets_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "pandas",
        "psycopg2-binary",
        "openpyxl",
        "minio",
	"unidecode"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
