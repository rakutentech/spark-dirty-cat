# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function
import sys
import os
import glob
from setuptools import setup, find_packages
from shutil import copyfile, copytree, rmtree


basedir = os.path.dirname(os.path.abspath(__file__))
os.chdir(basedir)

# A temporary path so we can access above the Python project root and fetch scripts and jars we need
JARS_TARGET = os.path.join(basedir, "dirty_cat_spark/lib")
DIRTY_CAT_HOME = os.path.abspath("../")

# Figure out where the jars are we need to package with PySpark.
JARS_PATH = glob.glob(os.path.join(DIRTY_CAT_HOME, "target/scala-*"))

if len(JARS_PATH) == 1:
    JARS_PATH = JARS_PATH[0]
else:
    raise IOError("cannot find jar files."
                  " Please make user to run sbt package first")


def _supports_symlinks():
    """Check if the system supports symlinks (e.g. *nix) or not."""
    return getattr(os, "symlink", None) is not None


if _supports_symlinks():
    os.symlink(JARS_PATH, JARS_TARGET)
else:
    # For windows fall back to the slower copytree
    copytree(JARS_PATH, JARS_TARGET)



def setup_package():

    try:
        def f(*path):
            return open(os.path.join(basedir, *path))

        setup(
            name='dirty_cat_spark',
            maintainer='Andres Hoyos Idrobo',
            maintainer_email='andres.hoyosidrobo@rakuten.com',
            version=f('../version.txt').read().strip(),
            description='Similarity-based embedding to encode dirty categorical strings in PySpark.',
            long_description=f('../README.md').read(),
            # url='https://github.com/XXX/dirty_cat',
            license='Apache License 2.0',

            keywords='spark pyspark categorical ml',

            classifiers=[
                'Development Status :: 2 - Pre-Alpha',
                'Environment :: Other Environment',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: Apache Software License',
                'Operating System :: OS Independent',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2',
                'Programming Language :: Python :: 2.7',
                'Topic :: Software Development :: Libraries',
                'Topic :: Scientific/Engineering :: Information Analysis',
                'Topic :: Utilities',
            ],
            install_requires=open('./requirements.txt').read().split(),

            packages=find_packages(exclude=['test']),
            include_package_data=True, # Needed to install jar file
        )

    finally:
        # print("here")
        if _supports_symlinks():
            os.remove(JARS_TARGET)
        else:
            rmtree(JARS_TARGET)


if __name__ == '__main__':
    setup_package()
