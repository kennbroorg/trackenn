from cx_Freeze import setup, Executable

output_directory = "trackenn"

build_options = {
    'packages': [
        'core',
        'fastapi',
        'starlette',
        'pydantic',
        'uvicorn',
        'click',
        'h11',
        'anyio',
        'sqlalchemy',
        'sqlalchemy.dialects.sqlite',
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
        # INFO: The binary ships the placeholder config, never the local one with real keys
        ('config.example.yaml', 'config.yaml'),
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
