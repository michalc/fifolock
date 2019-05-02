import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='fifolock',
    version='0.0.20',
    author='Michal Charemza',
    author_email='michal@charemza.name',
    description='A flexible low-level tool to make synchronisation primitives in asyncio Python',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/michalc/fifolock',
    py_modules=[
        'fifolock',
    ],
    python_requires='>=3.5',
    test_suite='test',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Framework :: AsyncIO',
    ],
)
