from distutils.core import setup

setup(
    name         = 'vireo',
    version      = '0.7.9',
    description  = 'A library and framework for event-driven application development',
    license      = 'MIT',
    author       = 'Juti Noppornpitak',
    author_email = 'juti_n@yahoo.co.jp',
    # url          = 'http://shiroyuki.com/projects/vireo.html',
    packages     = [
        'vireo',
        'vireo.drivers',
        'vireo.drivers.rabbitmq',
    ],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries'
    ],
    install_requires = ['pika']
)
