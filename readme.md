

Environment Setup and Installation: setup a python virtual environment and install quads on top of it using the following steps:

1. git clone repo-url
2. cd quads_dev
3. ./bootstrap.sh
   
Step 3 loads the python/GEOSpyD module, disables ~/.local packages (PYTHONNOUSERSITE=1),
creates a virtual environment in ./.venv/, activates it, upgrades pip, and installs all dependencies from requirements.txt, and finally installs quads in editable mode. Installing it in editable mode allows the user to simply clone and run when a new version of quads comes (with out re-installation).


Optinal: If you want to submit jobs using slurm, your .sh should include the following lines:
"load python/GEOSpyD and activate"
"source .venv/bin/activate" # activates the virtual environment

