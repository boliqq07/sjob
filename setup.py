from setuptools import setup, find_packages

setup(
    name='sjob',
    version='0.1.0',
    description='A Python package for sjob',
    author='wcx',
    author_email='986798607@qq.com',
    packages=find_packages(),
    install_requires=[
        'psutil',  # Ensure psutil is installed
        'requests',  # Example dependency, adjust as needed
        'python-daemon',
    ],
    python_requires='>=3.9',
    entry_points={
        'console_scripts': [
            'sjob=sjob.base:main',  # Adjust this to your main function
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    keywords='sjob job queue task management',
    license='MIT',
)