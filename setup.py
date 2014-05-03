from setuptools import setup, find_packages

setup(

    name='uifutures',
    version='0.1-dev',
    description='Qt UI for background jobs.',
    url='http://github.com/westernx/uifutures',
    
    packages=find_packages(exclude=['build*', 'tests*']),
    include_package_data=True,
    
    author='Mike Boers',
    author_email='uifutures@mikeboers.com',
    license='BSD-3',
    
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    
)
