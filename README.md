# GOTO-alert

## Installation

Once you've downloaded or cloned the repository, in the base directory:

> `$ python setup.py install`

Or alternatively:

> `$ pip install .`

You should then be able to import the module using `import gotoalert` within Python.

Several scripts from the `scripts` folder should also be added to your path.

You will also need to have <https://comet.readthedocs.io/en/stable/> installed.

## Configuration

A script `setup_gotoalert` is provided to create the webpage directories which should be run after installation.

## Instructions

Run the system live with this terminal command (it may need some adjusting for your local system).

```
twistd -n comet \
  --cmd=runscript.bash \
  --local-ivo=ivo://org.goto-observatory/test \
  --remote=voevent.4pisky.org:8099 \
  --verbose >> comet.log 2>&1 &
```

Further options:

```
  --pidfile=twistd.pid
  --umask=0002
  --logfile=comit.log
  --eventdb=.cometdb
  ```
