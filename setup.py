from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need
# fine tuning.
output_directory = "trackenn"

build_options = {'packages': ['core'], 'excludes': [], 'build_exe': output_directory}

base = 'console'

executables = [
    Executable('app.py', base=base, target_name = 'trackenn')
]

setup(name='trackenn',
      version = '0.1',
      description = 'Blockchain traceability',
      options = {'build_exe': build_options},
      executables = executables)
