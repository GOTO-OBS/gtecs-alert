"""Functions to define the observing strategy for different events."""

import importlib.resources as pkg_resources
import json

import astropy.units as u
from astropy.time import Time


def _load_strategy_files():
    with pkg_resources.path('gtecs.alert.data', 'strategies.json') as path, open(path) as f:
        strategies = json.load(f)
    with pkg_resources.path('gtecs.alert.data', 'cadences.json') as path, open(path) as f:
        cadences = json.load(f)
    with pkg_resources.path('gtecs.alert.data', 'constraints.json') as path, open(path) as f:
        constraints = json.load(f)
    with pkg_resources.path('gtecs.alert.data', 'exposures.json') as path, open(path) as f:
        exposures = json.load(f)
    return strategies, cadences, constraints, exposures


def get_strategy_details(name='DEFAULT', time=None):
    """Get details of the requested strategy."""
    if time is None:
        time = Time.now()

    # Load the strategy files
    strategies, cadences, constraints, exposures = _load_strategy_files()

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
    # NB A list of multiple cadence strategies can be given
    if isinstance(strategy_dict['cadence'], str):
        strategy_dict['cadence'] = [strategy_dict['cadence']]
    strategy_dict['cadence_dict'] = []
    for cadence in strategy_dict['cadence']:
        cadence_dict = cadences[cadence]
        if 'delay_days' in cadence_dict:
            cadence_dict['start_time'] = time + cadence_dict['delay_days'] * u.day
        else:
            cadence_dict['start_time'] = time
        cadence_dict['stop_time'] = cadence_dict['start_time'] + cadence_dict['valid_days'] * u.day
        strategy_dict['cadence_dict'].append(cadence_dict)

    # Add other dicts
    strategy_dict['constraints_dict'] = constraints[strategy_dict['constraints']]
    strategy_dict['exposure_sets_dict'] = exposures[strategy_dict['exposure_sets']]

    return strategy_dict
