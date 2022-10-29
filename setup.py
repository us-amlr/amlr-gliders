from setuptools import setup

setup(name='amlr-gliders',
      version='0.1.0a1',
      description='Process AMLR glider data, primarily in GCP',
      url='http://github.com/us-amlr/amlr-gliders',
      author='Sam Woodman',
      author_email='sam.woodman@noaa.gov',
      license='CC0',
      packages=['amlrgliders'],
      python_requires='>=3.9, !=3.10.*',
      install_requires=[
            'pandas', 
            'numpy', 
            'gdm'
      ],
      zip_safe=False)
