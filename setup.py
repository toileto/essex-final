import os
import setuptools

setuptools.setup(
    name="laminar",
    version=os.environ["build_version"],
    url="https://github.com/toileto/essex-final",
    packages=setuptools.find_packages(
        exclude=["tests*"]
    ),
    entry_points={
        "console_scripts": [
            "laminar = laminar.entrypoint:cli",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8.13",
)