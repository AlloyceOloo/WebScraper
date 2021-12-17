def main():

    import json
    import numpy as np
    from json import loads
    from json import JSONEncoder
    from kafka import KafkaConsumer
    import pandas as pd
    import googlemaps
    from geopy import distance
    import numpy as np

    gmaps_key = googlemaps.Client(key="")

    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        'scrapeTut10',
        bootstrap_servers = ['localhost:9092'],
        auto_offset_reset = 'earliest',
        value_deserializer = lambda x:loads(x.decode('utf-8'))
    )

    for message in consumer:
        data1 = json.loads(message.value)

        df1 = pd.DataFrame(data1)

        data2 = df1.to_dict('list')

        df2 = pd.DataFrame(data2)

        add_1 = df2['Property Location'][0]

        if(len(add_1) != 0):
            try:

                g_add = gmaps_key.geocode(add_1)

                lat_add = g_add[0]["geometry"]["location"]["lat"]
                long_add = g_add[0]["geometry"]["location"]["lng"]

                g_cbd = gmaps_key.geocode('Central Business District, Nairobi')

                lat_cbd = g_cbd[0]["geometry"]["location"]["lat"]
                long_cbd = g_cbd[0]["geometry"]["location"]["lng"]

                def hav_dist(lat, lat2, long, long2):
                    r = 6371
                    phi1 = np.radians(lat)
                    phi2 = np.radians(lat2)
                    delta_phi = np.radians(lat2 - lat)
                    delta_lambda = np.radians(long2 - long)
                    a = np.sin(delta_phi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2)**2
                    res = r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1-a)))
                    return np.round(res, 2)

                distance = hav_dist(lat_add, lat_cbd, long_add, long_cbd)

                df2['DistanceFromCBD'] = distance

            except Exception as e:
                print(e)
                df2['DistanceFromCBD'] = None
        else:
            df2['DistanceFromCBD'] = None

             

if __name__ == '__main__':
    main()
