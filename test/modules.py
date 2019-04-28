# -*- coding: utf-8 -*-

from modulefinder import ModuleFinder
from os import path

basepath = path.dirname(__file__)
filepath = path.abspath(path.join(basepath, '..', 'bondapp.py'))

f = ModuleFinder()
# Run the main script
f.run_script(filepath)

# Get names of all the imported modules
names = list(f.modules.keys())
# Get a sorted list of the root modules imported
basemods = sorted(set([name.split('.')[0] for name in names]))
# Print it nicely
print ("\n".join(basemods))