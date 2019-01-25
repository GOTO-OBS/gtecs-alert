# GOTO-alert

**GOTO-alert** is the alert processing module for the GOTO Observatory.

Note this module is Python3 only and has been developed for Linux, otherwise use at your own risk.

## Requirements

GOTO-alert requires several Python modules, which should be included during installation.

To work fully GOTO-alert also requires other GOTO modules to be installed:

- [ObsDB](https://github.com/GOTO-OBS/goto-obsdb)
- [GOTO-tile](https://github.com/GOTO-OBS/goto-tile)

## Installation

Once you've downloaded or cloned the repository, in the base directory run:

    pip3 install . --user

You should then be able to import the module using `import gotoalert` within Python.

Several scripts from the `scripts` folder should also be added to your path.

### Configuration

The module will look for a file named `.gotoalert.conf` either in the user's home directory or any path specified by the `GOTOALERT_CONF` environment variable. An example file is included in the base directory of this repository.

When installing GOTO-alert, copy the included `.gotoalert.conf` file to one of the above locations, and change the `HTML_PATH` parameter to specify where you want GOTO-alert to save webpages. Once that has been done run the `setup_gotoalert.py` script to create the expected directory structure at that location.

### Testing

After installing the module, you can test it works correctly using the included `test_gotoalert.py` script in the `gotoalert/tests/` directory.

## Usage instructions

To start a listener on the command line you will need [Comet](comet.readthedocs.io/en/stable) installed, then run:

    twistd -n comet \
      --cmd=gotoalert.sh \
      --local-ivo=ivo://org.goto-observatory/test \
      --remote=voevent.4pisky.org:8099 \
      --verbose >> comet.log 2>&1 &
