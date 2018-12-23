from setuptools import setup, find_packages

setup(name="smpq",
      version=0.1,
      description="Simple Multi-Processing with Queues",
      author='Dane Austin',
      author_email='dane_austin@fastmail.com.au',
      url='https://github.com/draustin/smpq',
      license='BSD',
      packages=find_packages(),
      install_requires=['dill'],
      test_requires=['pytest'])
