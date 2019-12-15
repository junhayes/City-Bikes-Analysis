from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import time

connection = psycopg2.connect(
    host='[host_name].rds.amazonaws.com',
    port=5432,
    user='username',
    password='password',
    database='database'
)
cur = connection.cursor()
timestamp = time.strftime('%d-%m-%Y_%H%M',time.localtime())

def kmean(city):
#Variable inputs per city
    if city == "dublin":
        lat = 53.345
        long = -6.26
        zoom = 14
        radius = 7
        colours = ['red', 'blue', 'black', 'green']
    if city == "brussels":
        lat = 50.85
        long = 4.35
        zoom = 12
        radius = 4
        colours = ['black', 'blue', 'green', 'red']
    if city == "goteborg":
        lat = 57.701
        long = 11.97
        zoom = 14
        radius = 7
        colours = ['blue', 'red', 'green', 'black']

#SQL query and transformation of kmeans SQL view (FREDERICK STREET SOUTH was exculded as it recently re-opened and would destort the clusters)
    SQL_Query_kmeans = pd.read_sql_query(
        "SELECT * FROM bikes." + city + "_kmeans WHERE name <> 'FREDERICK STREET SOUTH'",
        connection)

    df = pd.DataFrame(SQL_Query_kmeans, columns=['name','time_interval', 'real_capacity'])
    df_pivot = df.pivot(index='time_interval', columns='name', values='real_capacity')

#SQL query to extract distinct times and the formating of these times
    SQL_Query_time = pd.read_sql_query(
        "SELECT DISTINCT ON (time_interval) * FROM bikes." + city + "_kmeans WHERE name <> 'FREDERICK STREET SOUTH' ",
        connection)

    df1 = pd.DataFrame(SQL_Query_time, columns=['name','time_interval', 'real_capacity'])
    df1_time = df1['time_interval'].apply(lambda t: t.strftime('%H:%M'))
    df1.to_csv(r'C:\Users\eoinr\OneDrive\Desktop\New folder\Dublin Bikes\File Name.csv')

    fig, ax = plt.subplots(figsize=(10, 6))
#K-means clustering
    n_clusters = 1
    kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(df_pivot.T)

#Plotting the cluster centers
    for k, colour in zip(kmeans.cluster_centers_, colours):
        plt.plot(100 * k, color=colour, label=colour)

    ax.set_xlim([0, 144])
    ax.set_ylim([0, 100])
    xticks = range(0,144,12)
    new_ticks = [df1_time[i] for i in [int(_) for _ in xticks]]
    plt.xticks(xticks, new_ticks, rotation='horizontal')
    plt.xlabel('Time')
    plt.ylabel("Capacity (%)")
    plt.savefig(city + ' Kmeans{}.png'.format(timestamp))

    clusters = kmeans.predict(df_pivot.T)

#SLQ query to for the bike station coordinates
    SQL_Query_bikestations = pd.read_sql_query(
        "SELECT id, name, latitude, longtitude FROM bikes.bikestations WHERE city = '" + city + "'", connection)

    df = pd.DataFrame(SQL_Query_bikestations, columns=['id','name', 'latitude','longtitude'])
    bikestations = df.set_index('name')

#Matching bikestations with cluster numbers
    bikestations['Cluster'] = pd.Series(index=df_pivot.T.index.values,data=clusters)

#Mapping bike stations with and assigning them a cluster based on colour
    import folium
    mp = folium.Map(location=[lat, long], zoom_start=zoom,tiles='cartodbpositron')
    for c , colour in zip(range(n_clusters),colours):
        tmp = bikestations[bikestations['Cluster'] == c]

        for location in tmp.iterrows():
            folium.CircleMarker(
                location=[location[1]['latitude'],location[1]['longtitude']],
                radius=radius,
                popup=location[0],
                color=colour,
                fill_color=colour
            ).add_to(mp)

    mp.save(city+'.map{}.html'.format(timestamp))
    mp

kmean("goteborg")
