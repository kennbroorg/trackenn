from cx_Freeze import setup, Executable

output_directory = "trackenn"

build_options = {
    'packages': [
        'core',
        'flask',
        'flask_cors',
        'sqlalchemy',
        'sqlalchemy.dialects.sqlite',
        'pandasql',
        'pandas',
        'pyarrow',
        'sqlite3',
        'encodings',
        'requests',
        'yaml',
        'termcolor',
        'coloredlogs',
    ],
    'include_files': [
        ('front', 'front'),
        ('data', 'data'),
        'config.yaml',
    ],
    'excludes': ['tkinter'],
    'include_msvcr': True,
    'build_exe': output_directory,
}

base = 'console'

executables = [
    Executable('app.py', base=base, target_name = 'trackenn')
]

setup(name='trackenn',
      version = '0.0.5',
      description = 'Blockchain traceability',
      options = {'build_exe': build_options},
      executables = executables)
