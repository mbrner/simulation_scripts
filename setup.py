from setuptools import setup

setup(
    name='simulation_scripts',
    version='0.0.1',
    py_modules=['simulations_scripts'],
    install_requires=[
        'Click',
        'pyyaml'
    ],
    entry_points='''
        [console_scripts]
        simulations_scripts_write=simulations_scripts:main
    ''',
)
