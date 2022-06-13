import setuptools

setuptools.setup(
    name="#{Build.DefinitionName}#",
    version="#{version}#", 
    description="Python #{Build.DefinitionName}# Package",
    packages=setuptools.find_packages('src'),
    package_dir={'':'src'},
    install_requires=[
        'pandas <= 1.3.5',
        'numpy <= 1.21.6',
        'pm4py <= 2.2.22',
        'matplotlib <= 3.2.2',
        'seaborn <= 0.11.2'
    ]
)
