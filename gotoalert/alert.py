#! /opt/local/bin/python3.6
import sys
import os
import voeventparse as vp
from voeventparse import get_toplevel_params, get_event_time_as_utc
import numpy as np
import pkg_resources

from .goto_observatories_definitions import telescope, params, top_params, event_definitions, observing_definitions
from .coms import alert_dictionary, writecsv, create_graphs, htmlwrite, sendemail, topten
from .slack_message import slackmessage
from .csv2htmltable import main


#used for local testing
data_dir = pkg_resources.resource_filename('gotoalert', 'data')
testdata_path = os.path.join(data_dir, 'ivo__nasa.gsfc.gcn_SWIFTBAT_GRB_Pos_813449030')

with open(testdata_path, "rb") as f:
    TEST_DATA = vp.load(f)

path = "./www"


def main(v):
    role = v.attrib['role']

    if role == "test" or role == "utility":
        sys.exit()


    params = params(v)
    top_params = top_params(v)
    event_dictionary = event_definitions(v)
    alert_dictionary = alert_dictionary()

    goto_north = telescope('goto north', +37, 145, 10, 'UTC')
    goto_south = telescope('goto south', -37, 145, 10, 'UTC')

    goto_north_dictionary = observing_definitions('23:59:59', 4, 30, goto_north, event_dictionary["ra_dec_formatted"])
    goto_south_dictionary = observing_definitions('11:59:59', 4, 30, goto_south, event_dictionary["ra_dec_formatted"])
    #defines ra_dec, ra_dec_formatted and ra_dec_error and how to read xml file


    if event_dictionary["ivorn"].startswith(alert_dictionary["Swift_XRT_POS"]):
        name = "Swift_XRT_POS_"
        name1 = str("swift")

    if event_dictionary["ivorn"].startswith(alert_dictionary["Swift_BAT_GRB_POS"]):
        name = "Swift_BAT_GRB_POS_"
        name1 = str("swift")

    if event_dictionary["ivorn"].startswith(alert_dictionary["Fermi_GMB_GND_POS"]):
        name =  "Fermi_GMB_GND_POS_"
        name1 = str("fermi")


    writecsv(
        os.path.join(path, "master.csv"),
        event_dictionary["ivorn"],
        name+top_params['TrigID']['value'],
        event_dictionary["event_time"], event_dictionary["ra_dec"].ra.deg,
        event_dictionary["ra_dec"].dec.deg, event_dictionary["dist_galactic_center"],
        event_dictionary["object_galactic_lat"],
        goto_north_dictionary["alt_observable_adjusted"],
        goto_south_dictionary["alt_observable_adjusted"],
        )


    def parse(site, scope, scope_string):

        if -8 < event_dictionary["object_galactic_lat"].value < 8:
            sys.exit("too close to the Galactic plane")

        if event_dictionary["dist_galactic_center"].value < 15:
            sys.exit("too close to the Galactic centre")

        if site["alt_observable"] == False:
            print("Target does not rise above alt 40 at " +scope_string)

        if site["alt_observable"] == True:

            print("Target does rise above alt 40 at " +scope_string)

            if np.sum(scope.target_is_up(site["night_time"], event_dictionary["ra_dec_formatted"])) <6 :
                print("Target is not up longer then 1:30 at " +scope_string+ " during the night")

            if np.sum(scope.target_is_up(site["night_time"], event_dictionary["ra_dec_formatted"])) >6 :
                print("Target is up longer then 1:30 at " +scope_string+ " during the night")

                if site["final_constraint"] == False:
                    print("Target does not rise above alt 40 at " +scope_string+ " during observation peroid")

                if site["final_constraint"] == True:
                    print("Target does rise above alt 40 at " +scope_string+ " during observation peroid")


                    file_name = name+top_params['TrigID']['value']
                    file_path1 = "./www/"+scope_string+"_transients/airmass_plots/"
                    file_path2 = "./www/"+scope_string+"_transients/finder_charts/"
                    file_path3 = "./www/"+scope_string+"_transients/"



                    create_graphs(
                    event_dictionary["ra_dec"],
                    scope,
                    site["airmass_time"],
                    file_path1,
                    file_path2,
                    file_name,
                    30,
                    event_dictionary["ra_dec_formatted"]
                    )

                    htmlwrite(
                    file_path3,
                    file_name,
                    "New transient for "+scope_string+" from "+name,
                    top_params['TrigID']['value'],
                    name1,
                    event_dictionary["event_time"],
                    event_dictionary["ra_dec"],
                    event_dictionary["ra_dec_error"],
                    v.Who.Author.contactEmail,
                    site["target_rise"],
                    site["target_set"],
                    site["dark_sunset_tonight"],
                    site["dark_sunrise_tonight"],
                    site["observation_start"],
                    site["observation_end"],
                    event_dictionary["dist_galactic_center"],
                    event_dictionary["object_galactic_lat"],
                    site["is_safe_from_moon"]
                    )


    #                sendemail(
    #                "lapalmaobservatory@gmail.com",
    #                "aobr10@student.monash.edu",
    #                "Detection from "+scope_string,
    #                name+" Detection: See more at  http://118.138.235.166/~obrads",
    #                "lapalmaobservatory1",
    #                file_path3,
    #                file_name
    #                )


                    writecsv(
                        os.path.join(file_path3, scope_string+".csv"),
                        event_dictionary["ivorn"],
                        name+top_params['TrigID']['value'],
                        event_dictionary["event_time"], event_dictionary["ra_dec"].ra.deg,
                        event_dictionary["ra_dec"].dec.deg, event_dictionary["dist_galactic_center"],
                        event_dictionary["object_galactic_lat"],
                        goto_north_dictionary["alt_observable_adjusted"],
                        goto_south_dictionary["alt_observable_adjusted"],
                        )

                    topten(
                    file_path3+scope_string+".csv",
                    file_path3+"recent_ten.html"
                    )

                    if scope_string == "goto_north":
                        print("sent message to slack")

                        slackmessage(
                        name,
                        str(event_dictionary["event_time"])[:22],
                        str(event_dictionary["ra_dec"].ra.deg),
                        str(event_dictionary["ra_dec"].dec.deg),
                        file_name
                        )

                    main()

    parse(goto_north_dictionary, goto_north, "goto_north")
    parse(goto_south_dictionary, goto_south, "goto_south")


    print("done")


if __name__ == '__main__':
    v = TEST_DATA
    #use this when live parsing is working
    #v = vp.loads(sys.stdin.buffer.read())

    main(v)
