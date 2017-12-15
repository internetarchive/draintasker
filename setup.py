from setuptools import setup, find_packages

setup(
    name="draintasker",
    version="3.0a1",
    description="Continuous Uploader for Crawling Projects",
    author="Internet Archive",
    author_email="kenji@archive.org",
    packages=['draintasker'], #find_packages(),
    package_data={
        'drain': ["templates/*.html", "static/*.css", "static/*.js"]
        },
    # until we setup resource loader
    zip_safe=False,
    install_requires=[
        "six",
        "tornado",
        "PyYAML",
        "python-dateutil"
        ],
    tests_require=[
        "pytest",
        "mock"
        ],
    scripts=[
        "dtmon.py"
        ]
)
