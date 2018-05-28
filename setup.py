from setuptools import setup, find_packages


REQUIRES = []

for line in open("requirements.txt").read().split("\n"):
    REQUIRES.append(line)


setup(
    name="mongosync",
    version="0.0.1",
    packages=find_packages(),
    install_requires=REQUIRES,
    license="Apache License v2",
    author="Cam",
    entry_points={"console_scripts": ["mongosync = mongosync.entry_point:group"]},
)