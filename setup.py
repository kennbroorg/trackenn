import os
import sys

# INFO: front/ is downloaded, not committed (see `just front`); fail early with a clear message
#       instead of cx_Freeze silently shipping a binary with no web UI. Checked before importing
#       cx_Freeze so the error is clear even when cx_Freeze itself is not installed yet.
if not os.path.isfile(os.path.join('front', 'index.html')):
    sys.exit("front/index.html not found: run 'just front' (or 'just front <version>') before building")

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
