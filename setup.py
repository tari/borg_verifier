from setuptools import setup

setup(
        name='borg_verifier',
        version='0.1.1',
        description='Verifies Borg backups and exports Prometheus metrics',
        author='Peter marheine',
        author_email='peter@taricorp.net',
        url='https://bitbucket.org/tari/borg_verifier',
        classifiers=[
            'Programming Language :: Python :: 3',
        ],
        packages=['borg_verifier'],
        install_requires=[
            'prometheus_client',
        ],
        entry_points={
            'console_scripts': ['borg_verifier=borg_verifier.cli:main'],
        },
)
