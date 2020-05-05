"""
Installation file for soops.
"""
import os
from setuptools import setup

srcdir = os.path.dirname(__file__)
readme_filename = os.path.join(srcdir, 'README.rst')

def read_version_py(filename='soops/version.py'):
    ns = {}
    with open(filename, 'rb') as fd:
        exec(fd.read(), ns)
    return ns['__version__']

version = read_version_py()

setup(
    name='soops',
    version=version,
    description='Run parametric studies and scoop output files.',
    long_description=open(readme_filename, encoding="utf-8").read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
    ],
    keywords='run parametric studies, scoop output',
    url='https://github.com/rc/soops',
    author='Robert Cimrman',
    author_email='cimrman3@ntc.zcu.cz',
    license='BSD',
    packages=['soops'],
    python_requires='>=3.5',
    install_requires=[
        'pyparsing',
        'dask',
        'distributed',
        'pandas',
        'tables',
        'matplotlib',
    ],
    entry_points={
        'console_scripts': [
            'soops-run=soops.run_parametric:main',
            'soops-scoop=soops.scoop_outputs:main',
            'soops-info=soops.print_info:main',
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
