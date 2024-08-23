"""Functions to define the observing strategy for different events."""

import importlib.resources as pkg_resources
import json

import astropy.units as u
from astropy.time import Time


def get_strategy_details(name='DEFAULT', time=None):
    """Get details of the requested strategy."""
    name = name.upper()
    if time is None:
        time = Time.now()

    if name in ['IGNORE', 'RETRACTION']:
        # Special cases
        return None

    # Load the strategy definitions
    with pkg_resources.path('gtecs.alert.data', 'strategies.json') as path, open(path) as f:
        strategies = json.load(f)

    # Get the correct strategy for the given key
    try:
        strategy_dict = strategies[name]
    except KeyError as err:
        raise ValueError(f'Unknown strategy: {name}') from err
    strategy_dict['strategy'] = name

    # Check all the required keys are present
    if 'cadence' not in strategy_dict:
        raise ValueError(f'Undefined cadence for strategy {name}')
    if 'constraints' not in strategy_dict:
        raise ValueError(f'Undefined constraints for strategy {name}')
    if 'exposure_sets' not in strategy_dict:
        raise ValueError(f'Undefined exposure sets for strategy {name}')

    # Fill out the cadence strategy based on the given time
    # NB A list of multiple cadence strategies can be given, which makes this more awkward!
    if isinstance(strategy_dict['cadence'], dict):
        cadences = [strategy_dict['cadence']]
    else:
        cadences = strategy_dict['cadence']
    for cadence in cadences:
        if 'delay_days' in cadence:
            cadence['start_time'] = time + cadence['delay_days'] * u.day
        else:
            cadence['start_time'] = time
        cadence['stop_time'] = cadence['start_time'] + cadence['valid_days'] * u.day
    if len(cadences) == 1:
        strategy_dict['cadence'] = cadences[0]
    else:
        strategy_dict['cadence'] = cadences

    return strategy_dict
