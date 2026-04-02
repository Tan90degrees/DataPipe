from setuptools import setup, find_packages

setup(
    name="datapipe",
    version="0.1.0",
    description="DataPipe Python SDK",
    long_description="Python SDK for DataPipe pipeline execution system",
    author="DataPipe Team",
    author_email="datapipe@example.com",
    url="https://github.com/datapipe/datapipe",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    license="MIT",
    keywords="datapipe pipeline sdk",
)
