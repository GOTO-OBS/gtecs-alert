"""Functions to define the observing strategy for different events."""

import astropy.units as u


# Define event strategy options
# This should be in params too
STRATEGY_DICTONARY = {'DEFAULT': {'rank': 309,
                                  'cadence': 'TWO_NIGHTS',
                                  'constraints': 'NORMAL',
                                  'exposure_sets': '4x90L',
                                  'on_grid': True,
                                  'tile_limit': 200,
                                  'prob_limit': 0,
                                  },
                      'GW_CLOSE_NS': {'rank': 2,
                                      'cadence': 'NO_DELAY',
                                      'constraints': 'LENIENT',
                                      'exposure_sets': '4x90L',
                                      'on_grid': True,
                                      'tile_limit': 200,
                                      'prob_limit': 0,
                                      },
                      'GW_FAR_NS': {'rank': 13,
                                    'cadence': 'NO_DELAY',
                                    'constraints': 'LENIENT',
                                    'exposure_sets': '4x90L',
                                    'on_grid': True,
                                    'tile_limit': 200,
                                    'prob_limit': 0,
                                    },
                      'GW_CLOSE_BH': {'rank': 24,
                                      'cadence': 'TWO_NIGHTS',
                                      'constraints': 'LENIENT',
                                      'exposure_sets': '4x90L',
                                      'on_grid': True,
                                      'tile_limit': 200,
                                      'prob_limit': 0,
                                      },
                      'GW_FAR_BH': {'rank': 105,
                                    'cadence': 'TWO_NIGHTS',
                                    'constraints': 'LENIENT',
                                    'exposure_sets': '4x90L',
                                    'on_grid': True,
                                    'tile_limit': 200,
                                    'prob_limit': 0,
                                    },
                      'GW_BURST': {'rank': 52,
                                   'cadence': 'NO_DELAY',
                                   'constraints': 'LENIENT',
                                   'exposure_sets': '4x90L',
                                   'on_grid': True,
                                   'tile_limit': 200,
                                   'prob_limit': 0,
                                   },
                      'GRB_SWIFT': {'rank': 207,
                                    'cadence': 'MANY_FIRST_ONE_SECOND',
                                    'constraints': 'NORMAL',
                                    'exposure_sets': '6x90L',
                                    'on_grid': True,
                                    'tile_limit': 4,
                                    'prob_limit': 0.05,
                                    },
                      'GRB_FERMI': {'rank': 218,
                                    'cadence': 'MANY_FIRST_ONE_SECOND',
                                    'constraints': 'NORMAL',
                                    'exposure_sets': '4x90L',
                                    'on_grid': True,
                                    'tile_limit': 5,
                                    'prob_limit': 0.05,
                                    },
                      'GRB_FERMI_SHORT': {'rank': 210,
                                          'cadence': 'MANY_FIRST_ONE_SECOND',
                                          'constraints': 'NORMAL',
                                          'exposure_sets': '6x90L',
                                          'on_grid': True,
                                          'tile_limit': 8,
                                          'prob_limit': 0.005,
                                          },
                      'NU_ICECUBE_GOLD': {'rank': 259,
                                    'cadence': 'TWO_FIRST_ONE_SECOND',
                                    'constraints': 'NORMAL',
                                    'exposure_sets': '4x90L',
                                    'on_grid': True,
                                    'tile_limit': 3,
                                    'prob_limit': 0.05,
                                    },
                      'NU_ICECUBE_BRONZE': {'rank': 269,
                                    'cadence': 'TWO_FIRST_ONE_SECOND',
                                    'constraints': 'NORMAL',
                                    'exposure_sets': '4x90L',
                                    'on_grid': True,
                                    'tile_limit': 3,
                                    'prob_limit': 0.05,
                                    },
                      'NU_ICECUBE_CASCADE': {'rank': 279,
                                    'cadence': 'TWO_FIRST_ONE_SECOND',
                                    'constraints': 'NORMAL',
                                    'exposure_sets': '4x90L',
                                    'on_grid': True,
                                    'tile_limit': 8, 
                                    'prob_limit': 0.005,
                                    },
                      }

# Define possible cadence strategies
CADENCE_DICTONARY = {'NO_DELAY': {'num_todo': 99,
                                  'wait_time': 0,
                                  'valid_days': 3,
                                  },
                     'TWO_NIGHTS': {'num_todo': 2,
                                    'wait_time': 12 * 60,
                                    'valid_days': 3,
                                    },
                     'TWO_FIRST_ONE_SECOND': {'num_todo': 3,
                                              'wait_time': [4 * 60, 12 * 60],
                                              'valid_days': 3,
                                              },
                     'MANY_FIRST_ONE_SECOND': {'num_todo': 8,
                                               'wait_time': [60, 60, 120, 120, 120, 120, 12 * 60],
                                               'valid_days': 3,
                                               },
                     }

# Define possible constraint sets
CONSTRAINTS_DICTONARY = {'NORMAL': {'max_sunalt': -15,
                                    'min_alt': 30,
                                    'min_moonsep': 30,
                                    'max_moon': 'B',
                                    },
                         'LENIENT': {'max_sunalt': -12,
                                     'min_alt': 30,
                                     'min_moonsep': 10,
                                     'max_moon': 'B',
                                     },
                         }

# Define possible exposure sets
EXPOSURE_SETS_DICTIONARY = {'3x60L': [{'num_exp': 3, 'exptime': 60, 'filt': 'L'},
                                      ],
                            '9x60L': [{'num_exp': 9, 'exptime': 60, 'filt': 'L'},
                                      ],
                            '4x90L': [{'num_exp': 4, 'exptime': 90, 'filt': 'L'},
                                      ],
                            '6x90L': [{'num_exp': 4, 'exptime': 90, 'filt': 'L'},
                                      ],
                            '3x60RBG': [{'num_exp': 1, 'exptime': 60, 'filt': 'R'},
                                        {'num_exp': 1, 'exptime': 60, 'filt': 'G'},
                                        {'num_exp': 1, 'exptime': 60, 'filt': 'B'},
                                        ],
                            }


def get_event_strategy(event):
    """Get the strategy for the given Event."""
    if not event.interesting:
        # Uninteresting events shouldn't be added to the database
        return None

    # Get the specific event strategy
    strategy = 'DEFAULT'
    if event.type == 'GW':
        if event.group == 'CBC':
            if event.properties['HasNS'] > 0.25:
                if event.distance < 400:
                    strategy = 'GW_CLOSE_NS'
                else:
                    strategy = 'GW_FAR_NS'
            else:
                if event.distance < 100:
                    strategy = 'GW_CLOSE_BH'
                else:
                    strategy = 'GW_FAR_BH'
        else:
            strategy = 'GW_BURST'
    elif event.type == 'GRB':
        if event.source == 'Swift':
            strategy = 'GRB_SWIFT'
        else:
            if event.duration.lower() == 'short':
                strategy = 'GRB_FERMI_SHORT'
            else:
                strategy = 'GRB_FERMI'
    elif event.type == 'NU':
        if event.notice == 'ICECUBE_ASTROTRACK_GOLD':
            strategy = 'NU_ICECUBE_GOLD'
        elif event.notice == 'ICECUBE_ASTROTRACK_BRONZE': 
            strategy = 'NU_ICECUBE_BRONZE'
        elif event.notice == 'ICECUBE_CASCADE': 
            strategy = 'NU_ICECUBE_CASCADE'

    # Get the strategy dictionary
    strategy_dict = STRATEGY_DICTONARY[strategy]
    strategy_dict['strategy'] = strategy

    # Fill out the other dicts
    strategy_dict['cadence_dict'] = CADENCE_DICTONARY[strategy_dict['cadence']]
    strategy_dict['constraints_dict'] = CONSTRAINTS_DICTONARY[strategy_dict['constraints']]
    strategy_dict['exposure_sets_dict'] = EXPOSURE_SETS_DICTIONARY[strategy_dict['exposure_sets']]

    # Store the event start and stop time too
    strategy_dict['start_time'] = event.time
    valid_days = strategy_dict['cadence_dict']['valid_days'] * u.day
    strategy_dict['stop_time'] = event.time + valid_days

    return strategy_dict
