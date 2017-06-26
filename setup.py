from setuptools import setup

setup(
    name='simulation_scripts',
    version='0.0.1',
    py_modules=['simulations_script'],
    install_requires=[
        'Click',
        'pyyaml'
    ],
    entry_points='''
        [console_scripts]
        simulation_scripts_write=simulation_scripts:main
        simulation_scripts_process=process_local:main
    ''',
)
