#!/usr/bin/env python3

"""A python script to monitor ASKAP observations.

JSON link: https://prod-api.vlbi.atnf.tools/get_status/askap
Frontend: http://elenchically.net/ASKAPview/askapview.py

RUN: python3.9 -u askap_sentinel.py |& tee -a askap_log.txt

"""

import json
import time
import traceback

import astropy.units as u
import numpy as np
import requests
from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time
from sqlalchemy.exc import NoResultFound

from gtecs.alert.slack import send_slack_msg
from gtecs.alert import params
from gtecs.obs import database as obs_db

WAIT_TIME = 30  # seconds
SLACK_CHANNEL = params.SLACK_EVENT_CHANNELS["ASKAP"]
GOTO_FIELDS = {
    "VAST_0127-73": "T0029",
    "VAST_0913-50": "T0130",
    "VAST_0931-56": "T0101",
    "VAST_1014-56": "T0102",
    "VAST_1404-62": "T0080",
    "VAST_1428-56": "T0107",
    "VAST_1453-62": "T0081",
    "VAST_1651-37": "T0209",
    "VAST_1753-18": "T0376",
    "VAST_1806-12": "T0421",
}
EXPOSURE_SETS = {
    "num_exp": 4,
    "exptime": 45,
    "filt": "L",
}
OBSERVING_TIME = 2 * 60 * 60  # 2 hours
READOUT_TIME = 10
SET_TIME = EXPOSURE_SETS["num_exp"] * (EXPOSURE_SETS["exptime"] + READOUT_TIME)
N_SETS = int(OBSERVING_TIME / SET_TIME)


class ASKAPPointing:
    """A class to represent the ASKAP pointing."""

    # TODO: Make the Notice class more general, and make a ASKAPNotice class?

    def __init__(self, data):
        self.data = data
        self.info_time = Time(data["infoTime"])
        self.field = data["schedblock"]["field_name"]
        self.start_time = Time(data["schedblock"]["startTime"])
        self.duration = data["schedblock"]["duration"]
        self.end_time = self.start_time + self.duration * u.s
        self.progress = data["schedblock"]["progress"]
        self.footprint = data["footprint"]["name"]
        self.pitch = data["footprint"]["pitch"]
        self.rotation = (
            data["footprint"]["rotation"] + data["schedblock"]["pol_axis_angle"]
        )

    def __repr__(self):
        return f"ASKAPPointing({self.info_time.iso[:-4]}: {self.field} ({self.progress:.1f}%)"

    @classmethod
    def fetch(cls, url="https://prod-api.vlbi.atnf.tools/get_status/askap"):
        """Fetch the status of ASKAP observations."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except Exception as err:
            print(f"Error fetching data from {url}: {err}")
            print(traceback.format_exc())
            raise
        try:
            pointing = cls(data)
            return pointing
        except Exception as err:
            print(f"Error parsing data: {err}")
            print(traceback.format_exc())
            try:
                print(data)
            except Exception:
                pass
            raise

    @classmethod
    def from_file(cls, filename):
        """Load the status of ASKAP observations from a file."""
        with open(filename, "r") as file:
            data = json.load(file)
        return cls(data)

    @property
    def status(self):
        """Get the overall array status.

        Taken from https://github.com/epaell/askap/blob/master/monitor/askap.py#L54.
        """
        state = self.data["state"]
        ant_states = {}
        ant_states["Idle"] = state.count("Idle")
        ant_states["Stowed"] = state.count("Stowed")
        ant_states["Slewing"] = state.count("Slewing")
        ant_states["Tracking"] = state.count("Tracking")
        obs_status = "undefined"
        if ant_states["Tracking"] > 18:
            obs_status = "Tracking"
        elif ant_states["Stowed"] > 18:
            obs_status = "Stowed"
        elif ant_states["Idle"] > 18:
            obs_status = "Idle"
        elif ant_states["Slewing"] > 18:
            obs_status = "Slewing"
        return obs_status

    @property
    def coords(self):
        """Get the median array position.

        Taken from https://github.com/epaell/askap/blob/master/monitor/askap.py#L28.
        """
        ra_str = np.array(self.data['rightAscensionICRF'])
        dec_str = np.array(self.data['declinationICRF'])
        ra_str = ra_str[ra_str.nonzero()]   # Remove dodgy values
        dec_str = dec_str[ra_str.nonzero()]
        ra_str = ra_str[dec_str.nonzero()]
        dec_str = dec_str[dec_str.nonzero()]
        coords = SkyCoord(Angle(ra_str, unit=u.deg), Angle(dec_str, unit=u.deg), frame='fk5')
        return coords

    @property
    def centre(self):
        """Get the median array position.

        Taken from https://github.com/epaell/askap/blob/master/monitor/askap.py#L28.
        """
        ra_deg = np.median(self.coords.ra.deg)
        dec_deg = np.median(self.coords.dec.deg)
        coord = SkyCoord(Angle(ra_deg, unit=u.deg), Angle(dec_deg, unit=u.deg), frame='fk5')
        return coord


def add_to_database(askap_pointing, time=None):
    """Add entries for this notice into the database(s)."""
    # Based on gtecs.alert.handler.add_to_database
    # TODO: If we have an ASKAPNotice class we could make a mini skymap,
    # otherwise I think everything would work?
    if time is None:
        time = Time.now()

    # Note: we don't add to the alert database since this isn't a transient...
    with obs_db.session_manager() as session:
        # Fetch the ASKAP survey (or make it if this is the first time)
        try:
            query = session.query(obs_db.Survey)
            query = query.filter_by(name="ASKAP_survey")
            db_survey = query.one()
            survey_id = db_survey.db_id
        except NoResultFound:
            db_survey = obs_db.Survey(name="ASKAP_survey")
            session.add(db_survey)
            session.commit()
            survey_id = db_survey.db_id

        # Get the database User (make it if it doesn't exist) and the current Grid,
        # so we can link them to the new Targets
        try:
            db_user = obs_db.get_user(session, username="sentinel")
        except ValueError:
            db_user = obs_db.User("sentinel", "", "Sentinel alert Listener")
        db_grid = obs_db.get_current_grid(session)

        # Find the matching GridTile
        if askap_pointing.field not in GOTO_FIELDS:
            return
        tile_name = GOTO_FIELDS[askap_pointing.field]
        query = session.query(obs_db.GridTile)
        query = query.filter(obs_db.GridTile.grid == db_grid)
        query = query.filter(obs_db.GridTile.name == str(tile_name))
        db_grid_tile = query.one()

        # Create ExposureSets
        db_exposure_sets = []
        for _ in range(N_SETS):
            db_exposure_sets.append(
                obs_db.ExposureSet(
                    num_exp=EXPOSURE_SETS["num_exp"],
                    exptime=EXPOSURE_SETS["exptime"],
                    filt=EXPOSURE_SETS["filt"],
                )
            )

        # Create Strategy
        # Very simple, mostly keep all the defaults
        db_strategy = obs_db.Strategy(
            num_todo=1,
            too=True,
            tel_mask=12,  # 1100 i.e. GOTO 3 and 4 only
        )

        # Create Targets (this will automatically create Pointings)
        db_target = obs_db.Target(
            name=f"ASKAP_{askap_pointing.field}_{tile_name}",
            ra=None,  # RA/Dec are inherited from the grid tile
            dec=None,
            rank=100,
            weight=1,
            start_time=askap_pointing.start_time,
            stop_time=askap_pointing.end_time,
            creation_time=time,
            user=db_user,
            grid_tile=db_grid_tile,
            exposure_sets=db_exposure_sets,
            strategies=[db_strategy],
            survey_id=survey_id,
        )

        # Add the target to the database (and all related entries)
        session.add(db_target)

        # Commit changes
        try:
            session.commit()
        except Exception:
            # Undo database changes before raising
            session.rollback()
            raise

        return db_target.name, db_target.db_id

def run():
    """Run the script"""
    print("ASKAP Sentinel started...")
    send_slack_msg("ASKAP Sentinel started", channel=SLACK_CHANNEL)
    observing_blocks = []
    last_info_time = None

    while True:
        # Wait for a few seconds before the next request
        if len(observing_blocks) > 0:
            time.sleep(WAIT_TIME)

        try:
            # Download the status of ASKAP observations
            pointing = ASKAPPointing.fetch()

            # Check if the info time has changed
            if last_info_time is not None and pointing.info_time == last_info_time:
                # No new information available yet
                print(pointing.info_time)
                continue
            last_info_time = pointing.info_time
            print(pointing.info_time, end=" ")

            # Print pointing data
            print(pointing.field, end=" ")
            print(
                f"({pointing.start_time.iso[:-4]} - {pointing.end_time.iso[:-4]},", end=" "
            )
            print(f"{pointing.progress:.1f}%)", end=" ")
            print(f" {pointing.status} ", end=" ")

            # Check if the target has changed
            block_data = (pointing.field, pointing.start_time)
            if len(observing_blocks) == 0:
                # First target after startup
                observing_blocks.append(block_data)
            elif observing_blocks[-1] == block_data:
                # We're still observing the same target
                print("")
                continue
            elif pointing.status != "Tracking" or pointing.progress < 0.1:
                # We're not observing anything, or changing targets
                print("")
                continue

            # We have a new target
            observing_blocks.append(block_data)
            print("<< new", end=" ")

            if 'VAST' not in pointing.field:
                # We don't care
                print("")
                continue

            # debug, since I don't have any VAST data
            print(pointing.data)

            # Start Slack message
            msg = f"ASKAP is now observing *{pointing.field}*\n"
            msg += f"  Start: {pointing.start_time.iso}\n"
            msg += f"  End: {pointing.end_time.iso}\n"

            # Is it one of the fields we want GOTO to observe?
            if pointing.field not in GOTO_FIELDS:
                # Not one we care about
                msg += "Target is not in GOTO tile list"
                send_slack_msg(msg, channel=SLACK_CHANNEL)
                continue

            # It's one of our fields!
            print("--", GOTO_FIELDS[pointing.field])

            # Add the target to the database
            target_name, target_id = add_to_database(pointing)

            # Send a message to Slack
            msg += "Target is in GOTO tile list!"
            msg += f"  GOTO tile: {GOTO_FIELDS[pointing.field]}\n"
            msg += "Added target to the database:\n"
            msg += f"  `{target_name}` (ID={target_id})\n"
            send_slack_msg(msg, channel=SLACK_CHANNEL)

        except Exception as err:
            print(f"Error fetching status: {err}")
            print(traceback.format_exc())
            try:
                print(pointing.data)
            except Exception:
                pass
            continue
        except KeyboardInterrupt:
            print("Exiting...")
            break

if __name__ == "__main__":
    run()
