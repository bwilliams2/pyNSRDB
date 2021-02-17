import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyNSRDB",
    version="0.0.4",
    author="Bryce Williams",
    author_email="bwilliams2@gmail.com",
    description="Python frontend to NREL NSRDB API.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bwilliams2/pyNSRDB",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)