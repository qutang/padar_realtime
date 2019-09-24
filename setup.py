from setuptools import setup, find_packages

setup(
    name='padar_realtime',
    version='1.4.6',
    packages=find_packages(),
    include_package_data=True,
    description='Real-time engine used to process and visualize accelerometer data for padar package',
    long_description=open('README.md').read(),
    install_requires=[
        "websockets", "flask", "arrow", "pandas", "pymetawear@git+https://github.com/qutang/pymetawear.git@master"
    ]
)
