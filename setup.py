"""
Installation file for soops.
"""
import os
from setuptools import setup

import soops.version as version

srcdir = os.path.dirname(__file__)

readme_filename = os.path.join(srcdir, 'README.rst')

setup(
    name='soops',
    version=version.__version__,
    description='Run parametric studies and scoop output files.',
    long_description=open(readme_filename, encoding="utf-8").read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
    ],
    keywords='run parametric studies, scoop output',
    url='http://github.com/rc/soops',
    author='Robert Cimrman',
    author_email='cimrman3@ntc.zcu.cz',
    license='BSD',
    packages=['soops'],
    install_requires=[
        'dask',
        'pandas',
        'pyparsing',
    ],
    entry_points={
        'console_scripts': [
            'soops-run=soops.run_parametric:main',
            'soops-scoop=soops.scoop_outputs:main',
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
