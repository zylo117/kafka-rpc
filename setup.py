from glob import glob

from setuptools import setup

requirements = [
    'confluent-kafka==1.3.0',
    'aplex==1.0.1',
    'msgpack==0.6.2',
    'msgpack-numpy==0.4.4.3',
    'pycryptodome==3.9.0',
    'redis>=3.3.8',
    'zstd>=1.4.4.1'
]

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='kafka-rpc',
    version='1.0.11',
    description='RPC protocol based on kafka',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Carl Cheung',
    url='https://github.com/zylo117/kafka-rpc',
    packages=['kafka_rpc'],
    install_requires=requirements,
    zip_safe=False,
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
)
