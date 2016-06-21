import requests
import json
import psycopg2
import pysolr
import time

solr_url = "http://54.221.223.91:8983/solr/featuresearch"
#solr_url = "http://127.0.0.1:8983/solr/featuresearch"

solr = pysolr.Solr(solr_url, timeout=60)

#solr.delete(q='*:*')
##import ipdb;ipdb.set_trace()

response = requests.get('http://worldmap.harvard.edu/data/search/api?start=0&limit=10')
data = json.loads(response.content)
total = data['total']


conn_string = "host='localhost' dbname='geonode_imports' user='geonode' password='geonode'"

conn_string_legacy = "host='localhost' dbname='geonode' user='geonode' password='geonode'"

#conn_string = "host='localhost' dbname='geonode_imports' user='geonode' password='geonode'"
#connString = "PG: host=%s dbname=%s user=%s password=%s" % ('localhost','geonode_imports','geonode','geonode')
# print the connection string we will use to connect
print "Connecting to database\n	->%s" % (conn_string)

#conn = psycopg2.connect(conn_string)

#import ipdb;ipdb.set_trace()
#cursor = conn.cursor('featuresCursor')

def gazeteer_fetch_features():
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    gazeteer_sql = "select * from gazetteer_gazetteerentry"
    cursor.execute(gazeteer_sql)
    rows = []
    done = False
    while not done:
        queryrows = cursor.fetchmany()
        if queryrows == []:
            done = True
        for row in queryrows:
            rows.append(row)
    for row in rows:
        print 'reindexing %s' %(row[1])
        conn = psycopg2.connect(conn_string_legacy)
        cursor = conn.cursor()
        layer_metadata = "select uuid,name from maps_layer where name='%s'" %(row[1])
        cursor.execute(layer_metadata)
        layer = cursor.fetchall()
        if layer:
            feature_id = 'FeatureId:%s' %(row[4])
            layer_id = 'LayerId:%s' %(layer[0][0])
            payload = {'indent': 'true', 'wt': 'json','fq': feature_id, 'fq': layer_id, 'q':'*:*'}
            response = requests.get(solr_url+'/select?q=*%3A*&fq=FeatureId%3A'+str(row[4])+'&fq=LayerId%3A'+str(layer[0][0])+'&wt=json&indent=true')
            json_object = json.loads(response.content)
            results = []
            if json_object['response']['numFound'] == 1:
                items = json_object['response']['docs'][0]
                data = {}
                for item in items:
                    if item == 'FeatureText':
                        data[item] = items[item].encode('utf-8')
                    else:
                        data[item] = items[item]
                data.pop('_version_') 
                if row[8] and row[9]:
                    featuredate = '[%s TO %s]' % (row[8].strip('AD').strip(),row[9].strip('AD').strip()) 
                    data['FeatureDate'] = unicode(featuredate)
                    url_solr_update = '%s/update/json/docs' % solr_url
                    headers = {"content-type": "application/json"}
                    params = {"commitWithin": 100}
                    json_data = json.dumps(data)
                    req = requests.post(url_solr_update, data=json_data, params=params,  headers=headers)
                    #req = requests.post(solr_url+'/update?commit=true',data=json_data, headers=headers)
                    #import ipdb;ipdb.set_trace()
                   
                if row[8] and (row[9] is None):
                    print 'missing'
                    import ipdb;ipdb.set_trace()
                if row[9] and (row[8] is None):
                    print 'missing'
                    import ipdb;ipdb.set_trace()


for i in range(0, total, 10):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    url = 'http://worldmap.harvard.edu/data/search/api?start=%s&limit=10' % i
    response = requests.get(url)
    data = json.loads(response.content)
    start = time.clock()
    for row in data['rows']:
        temporal_extent_start = None
        if 'temporal_extent_start' in row:
            temporal_extent_start = row['temporal_extent_start']
        temporal_extent_end = None
        if 'temporal_extent_end' in row:
            temporal_extent_end = row['temporal_extent_end']
        layerdate = None
        if temporal_extent_start and temporal_extent_end:
            layerdate = "[%s TO %s]" % (temporal_extent_start,temporal_extent_end)
        if temporal_extent_start and temporal_extent_end is None:
            layerdate = temporal_extent_start
        if temporal_extent_end and temporal_extent_start is None:
            layerdate = temporal_extent_end
        table_name = row['name'].split(":")[1]
        layerid = row['uuid']
        layername = row['name']
        try:
            owner = row['owner_username']
        except:
            owner = unicode('empty')
        print table_name
        text_columns = "SELECT column_name from information_schema.columns where table_name = '"+table_name+"' and data_type='character varying'"
        id_columns = "SELECT column_name from information_schema.columns where table_name = '"+table_name+"' and data_type='integer'"
        cursor.execute(text_columns);
        columns = cursor.fetchall()
        cursor.execute(id_columns);
        idcolumns = cursor.fetchall()
        featureid = [item for item in idcolumns if 'fid' in item]
        if featureid:
            featureid = 'fid'
        else:
            featureid = [item for item in idcolumns if 'id' in item]
            if featureid:
                featureid = 'id'
            else:
                if idcolumns:
                    featureid = idcolumns[0][0]
        if columns:
            query_columns = featureid + ','
            for column in columns:
                column = column[0]
                column = '"'+column.decode('utf-8')+'"'
                query_columns += column + ','
            # This is for one projection space but not in default 4326
            bbox = 'ST_XMin(ST_Extent(ST_Transform(the_geom,4326))) as xmin , ST_YMin(ST_Extent(ST_Transform(the_geom,4326))) as ymin, ST_XMax(ST_Extent(ST_Transform(the_geom,4326))) as xmax, ST_YMax(ST_Extent(ST_Transform(the_geom,4326))) as ymax'
            try:
                query = 'SELECT %s %s FROM "%s" GROUP BY %s' %(query_columns, bbox, table_name, featureid)
            except:
                pass
            cursor = conn.cursor()
            cursor.execute('SELECT count (*) from "%s"' %(table_name))
            print 'From the count of featurs'
            count = cursor.fetchone()[0]
            print count
            print 'Querying table "%s"' %(table_name)
            srid_geom_query = "SELECT srid,f_geometry_column from geometry_columns WHERE f_table_name='%s'" %(table_name)
            cursor = conn.cursor()
            cursor.execute(srid_geom_query)
            srids = cursor.fetchall()
            if len(srids) > 1: 
                print 'More than one srid'
            elif len(srids) == 1:
                srid = srids[0][0]
                the_geom = srids[0][1]
                if (srid == 4326) or (srid == 0) or (srid == 42101):
                    query = 'SELECT %s ST_XMin(ST_Extent(%s)) AS xmin, ST_YMin(ST_Extent(%s)) AS ymin, ST_XMax(ST_Extent(%s)) AS xmax, ST_YMax(ST_Extent(%s)) AS ymax FROM "%s" GROUP BY %s' %(query_columns,the_geom,the_geom,the_geom,the_geom,table_name, featureid)
            try:
                querycursor = conn.cursor(name='features')
                querycursor.execute(query)
            except psycopg2.ProgrammingError, e:
                conn.rollback()
                query = 'SELECT %s ST_XMin(ST_Extent(%s)) AS xmin, ST_YMin(ST_Extent(%s)) AS ymin, ST_XMax(ST_Extent(%s)) AS xmax, ST_YMax(ST_Extent(%s)) AS ymax FROM "%s" GROUP BY %s,%s' %(query_columns,the_geom,the_geom,the_geom,the_geom,table_name, featureid,columns[0][0])
                groupcursor = conn.cursor()
                groupcursor.execute(query)
                querycursor = groupcursor
            done = False
            rowcount = 0
            rows = []
            while not done:
                myrows = querycursor.fetchmany()
                if myrows == []:
                    done = True
                for row in myrows:
                    rows.append(row)
                    rowcount+=1
            print rowcount
            print count
            querycursor.close()
            print 'From the number of items in subquery'
            if len(rows) != count:
                import ipdb;ipdb.set_trace()
            if rows:
                columns_number = len(rows[0])
                text_columns_number = columns_number - 4
                layer_documents = []
                for row in rows:
                    text = ''
                    for i in range(1, text_columns_number):
                        if row[i] is not None:
                            text += row[i] + ' '
                    xmin = row[columns_number-4]
                    ymin = row[columns_number-3]
                    xmax = row[columns_number-2]
                    ymax = row[columns_number-1]
                    if (xmin < -180):
                        xmin = -180
                    if (xmax > 180):
                        xmax = 180
                    if (ymin < -90):
                        ymin = -90
                    if (ymax > 90):
                        ymax = 90
                    extent = "ENVELOPE(%s,%s,%s,%s)" % (xmin,xmax,ymax,ymin)
                    halfWidth = (xmax - xmin) / 2.0
                    halfHeight = (ymax - ymin) / 2.0
                    area = (halfWidth * 2) * (halfHeight * 2)
                    solr_record = {
                                   "LayerId": layerid,
                                   "FeatureId": row[0],
                                   "LayerDate": layerdate,
                                   "LayerName": layername,
                                   "Area": area,
                                   "MinX": xmin,
                                   "MaxX": xmax,
                                   "MinY": ymin,
                                   "MaxY": ymax,
                                   "FeatureDate": "",
                                   "bbox": extent,
                                   "FeatureText": text,
                                   "owner": owner
                    }
                    layer_documents.append(solr_record)
                try:
                    url_solr_update = '%s/update/json/docs' % solr_url
                    headers = {"content-type": "application/json"}
                    params = {"commitWithin": 10000}
                    solr_json = json.dumps(layer_documents)
                    requests.post(url_solr_update, data=solr_json, params=params,  headers=headers)
                    print 'Features added to the core for layer %s' % (table_name)
                except:
                    import ipdb;ipdb.set_trace()
                print 'the layer has been indexed'
        else:
            print 'No columns found or table for %s' %(table_name)
    conn.close()
    end = time.clock()
    print "The time was {}".format(end - start)    
    import ipdb;ipdb.set_trace()

gazeteer_fetch_features()
