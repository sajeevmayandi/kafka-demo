import json
import logging
import math
import os
import random
import sys
import sys


# function to generate the test data with given telematic id
def generate_gps(telematic_id, file_name, noofgps):
    try:
        radius = 10000  # Choose your own radius
        radiusInDegrees = radius / 111300
        r = radiusInDegrees
        x0 = 40.84
        y0 = -73.87

        with open(file_name, 'w') as outfile:
            for i in range(1, int(noofgps)):  # Choose number of Lat Long to be generated
                u = float(random.uniform(0.0, 1.0))
                v = float(random.uniform(0.0, 1.0))
                w = r * math.sqrt(u)
                t = 2 * math.pi * v
                x = w * math.cos(t)
                y = w * math.sin(t)

                xLat = x + x0
                yLong = y + y0
                data = {}
                data['GPS'] = []
                data['GPS'].append({'telematicid': str(telematic_id)})
                data['GPS'].append({'Longitude': str(xLat)})
                data['GPS'].append({'Latitude': str(yLong)})
                outfile.write(json.dumps(data))
                outfile.write("\n")

    except OSError as err:
        print(''.join(['Exception in generate_gps: ', str(err)]))
    except ValueError as err:
        print(''.join(['Exception in generate_gps, value error : ', str(err)]))
    except:
        e = sys.exc_info()[0]
        print(''.join(['Exception in generate_gps:', str(e)]))


def main():
    if len(sys.argv) != 4:
        print(" ")
        print("Usage: generate.py  <telematic_id> <file_name> <noof-gps-cordinates")
        print(" ")
        return

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.ERROR
    )
    telemid = sys.argv[1]
    file_name = sys.argv[2]
    noofgps = sys.argv[3]
    generate_gps(telemid, file_name, noofgps)


if __name__ == "__main__":
    main()
