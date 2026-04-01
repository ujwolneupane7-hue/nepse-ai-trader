"""
Setup script for NEPSE Trading System
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="nepse-trading-system",
    version="3.0.0",
    author="Trading System",
    description="NEPSE Final Optimized Trading System",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "flask==2.3.2",
        "pandas==2.0.3",
        "requests==2.31.0",
        "websocket-client==1.6.1",
        "pytz==2023.3",
        "xlrd==2.0.1",
        "lxml==4.9.2",
        "numpy==1.24.3",
        "scikit-learn==1.3.0",
    ],
)