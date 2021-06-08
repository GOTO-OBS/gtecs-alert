# G-TeCS alert package

**G-TeCS** (*gee-teks*) is the GOTO Telescope Control System.

This package (`gtecs-alert`) contains functions for receiving and processing transient alerts.

Note this module is Python3 only and has been developed for Linux, otherwise use at your own risk.

## Requirements

This package requires several Python modules, which should be included during installation.

This package requires the following G-TeCS packages to function fully:

- [gtecs-common](https://github.com/GOTO-OBS/gtecs-common)

It also requires the following packages created for GOTO:

- [GOTO-tile](https://github.com/GOTO-OBS/goto-tile)

## Installation

Once you've downloaded or cloned the repository, in the base directory run:

    pip3 install . --user

You should then be able to import the module from within Python.

Several scripts from the `scripts` folder should also be added to your path.

### Configuration

The module will look for a file named `.alert.conf` either in the user's home directory, the `gtecs` subdirectory, or a path specified by the `GTECS_CONF` environment variable. An example file is included in the base directory of this repository.

After installing this package copy this sample config file to one of the above locations, and change the file path parameters to specify where you want the package to save files.

Once that has been done run the `initialise.py` script to create the expected directory structure at that location.

### Testing

After installing the module, you can test it works correctly using the included `test_gotoalert.py` script in the `gotoalert/tests/` directory.

## Usage

TODO
