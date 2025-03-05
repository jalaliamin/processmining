import setuptools

setuptools.setup(
    name="#{Build.DefinitionName}#",
    version="#{version}#", 
    description="Python #{Build.DefinitionName}# Package",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/jalaliamin/processmining',
    author='Your Name',
    author_email='your_email@example.com',
    license='MIT',
    packages=setuptools.find_packages('src'),
    package_dir={'':'src'},
    install_requires=[
        'pandas',
        'numpy',
        'pm4py',
        'matplotlib',
        'seaborn'
    ]
)
