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
    from csv import DictWriter

    #need a google maps api key to use geocoding
    gmaps_key = googlemaps.Client(key="")

    #kafka consumer, running locally
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        'scrapeTut10',
        bootstrap_servers = ['localhost:9092'],
        auto_offset_reset = 'earliest',
        value_deserializer = lambda x:loads(x.decode('utf-8'))
    )
    
    #field names used to define the headers for the csv data
    field_names = ['Property Title', 'Property Price', 'Property Location', 'Property Bedrooms', 'Property Bathrooms', 'DistanceFromCBD']

    for message in consumer:
        #python dictionary(nested) to save messages
        data1 = json.loads(message.value)
        
        #needed to switch from dictionary to dataframe for easier geocoding
        df1 = pd.DataFrame(data1)
        
        #eliminates the extra key in the dict, no longer nested
        data2 = df1.to_dict('list')
       
        df2 = pd.DataFrame(data2)
        
        #address for geocoding to coordinates
        add_1 = df2['Property Location'][0]
    
        #makes sure the address contains values else fills in a none value for distance from cbd
        if(len(add_1) != 0):
            try:
                g_add = gmaps_key.geocode(add_1)

                lat_add = g_add[0]["geometry"]["location"]["lat"]
                long_add = g_add[0]["geometry"]["location"]["lng"]
                
                #address for the central business district
                g_cbd = gmaps_key.geocode('Central Business District, Nairobi')

                lat_cbd = g_cbd[0]["geometry"]["location"]["lat"]
                long_cbd = g_cbd[0]["geometry"]["location"]["lng"]
                
                #calculates distance between cbd and property address
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
            
            #in case google maps can't geocode an address returns a none for distance from cbd
            except Exception as e:
                print(e)
                df2['DistanceFromCBD'] = None
        else:
            df2['DistanceFromCBD'] = None
    
        #dictionary to append to a csv file
        data3 = df2.to_dict('list')
        
        #passing data3 dictionary to append to the props1.csv
        with open('props1.csv', 'a') as f_object:
            
            dictwriter_object.writeheader()

            dictwriter_object = DictWriter(f_object, fieldnames = field_names)
            
            dictwriter_object.writerow(data3)


            f_object.close()


if __name__ == '__main__':
    main()
