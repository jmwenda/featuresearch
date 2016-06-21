import requests
import json
import psycopg2
import cProfile
import pysolr
import multiprocessing
import threading
import time
from multiprocessing import Pool 

def push_to_solr(solr_json,layername):
    url_solr_update = '%s/update/json/docs' % solr_url
    headers = {"content-type": "application/json"}
    params = {"commitWithin": 10000}
    requests.post(url_solr_update, data=solr_json, params=params,  headers=headers)
    print "%s Layer added to the coree" % layername
    return "done"

def build_json_docs(rows,layerid,layerdate,layername,owner):
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
    solr_json = json.dumps(layer_documents) 
    return solr_json

def feature_query(query):
    conn_query = psycopg2.connect(conn_string)
    querycursor = conn_query.cursor(name='features')
    querycursor.execute(query)
    done = False
    rowcount = 0
    rows = []
    jobs = []
    while not done:
        myrows = querycursor.fetchmany()
        if myrows == []:
            done = True
        for row in myrows:
            rows.append(row)
            rowcount+=1
    conn_query.close()
    return rows

def build_query(srids,query_columns,table_name,featureid):
    query = ""
    print srids
    print query_columns
    print table_name
    print featureid
    if len(srids) == 1:
        srid = srids[0][0]
        the_geom = srids[0][1]        
        # This is for one projection space but not in default 4326
        bbox = 'ST_XMin(ST_Extent(ST_Transform(the_geom,4326))) as xmin , ST_YMin(ST_Extent(ST_Transform(the_geom,4326))) as ymin, ST_XMax(ST_Extent(ST_Transform(the_geom,4326))) as xmax, ST_YMax(ST_Extent(ST_Transform(the_geom,4326))) as ymax'
        query = 'SELECT %s %s FROM "%s" GROUP BY %s' %(query_columns, bbox, table_name, featureid)
        if (srid == 4326) or (srid == 0) or (srid == 42101):
            query = 'SELECT %s ST_XMin(ST_Extent(%s)) AS xmin, ST_YMin(ST_Extent(%s)) AS ymin, ST_XMax(ST_Extent(%s)) AS xmax, ST_YMax(ST_Extent(%s)) AS ymax FROM "%s" GROUP BY %s' %(query_columns,the_geom,the_geom,the_geom,the_geom,table_name, featureid)
    print query
    return query

def get_srid(table):
    conn_srid = psycopg2.connect(conn_string)
    cursor_srid = conn_srid.cursor()
    srid_geom_query = "SELECT srid,f_geometry_column from geometry_columns WHERE f_table_name='%s'" %(table)
    cursor_srid.execute(srid_geom_query)
    srids = cursor_srid.fetchall()
    conn_srid.close()
    return srids
def get_table_id_column(table):
    conn_id = psycopg2.connect(conn_string)
    cursor_id = conn_id.cursor()
    id_columns = "SELECT column_name from information_schema.columns where table_name = '"+table+"' and data_type='integer'"
    cursor_id.execute(id_columns)
    idcolumns = cursor_id.fetchall()
    conn_id.close()
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
    return featureid

def get_table_columns(table,featureid):
    query_columns = ""
    conn_cols = psycopg2.connect(conn_string)
    cursor_cols = conn_cols.cursor()
    text_columns = "SELECT column_name from information_schema.columns where table_name = '"+table+"' and data_type='character varying'"
    cursor_cols.execute(text_columns);
    columns = cursor_cols.fetchall()
    conn_cols.close()
    if columns:
        query_columns = featureid + ','
        for column in columns:
            column = column[0]
            column = '"'+column.decode('utf-8')+'"'
            query_columns += column + ','
    return query_columns

def worker(row):
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
    featureid = get_table_id_column(table_name)
    if featureid:
        columns = get_table_columns(table_name,featureid)
        if columns:
            srids = get_srid(table_name)
            if srids:
                query = build_query(srids,columns,table_name,featureid)
                rows = feature_query(query)
                if rows:
                    solr_json = build_json_docs(rows,layerid,layerdate,layername,owner)
                    push_to_solr(solr_json,layername)
    return "Worker"


if __name__ == "__main__":
    solr_url = "http://127.0.0.1:8983/solr/featuresearch"
    solr = pysolr.Solr(solr_url, timeout=60)
    solr.delete(q='*:*')
    response = requests.get('http://worldmap.harvard.edu/data/search/api?start=0&limit=10')
    data = json.loads(response.content)
    total = data['total']
    conn_string = "host='localhost' dbname='geonode' user='geonode' password='geonode'"
    print "Connecting to database\n	->%s" % (conn_string)

    for i in range(0, total, 10):
        print "Page %s" % i
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        url = 'http://worldmap.harvard.edu/data/search/api?start=%s&limit=10' % i
        response = requests.get(url)
        data = json.loads(response.content)
        jobs = []
        start = time.clock()
        for row in data['rows']:
            #worker(row)
            p = multiprocessing.Process(target=worker,args=(row,))
            #p = threading.Thread(target=worker, args=(row,))
            #thread = threading.Thread(target=worker, args=(row,))
            #jobs.append(thread)
            #thread.start()
            #thread.join()
            #jobs.append(p)
            p.start()
            p.join()
        end = time.clock()
        print "The time was {}".format(end - start)
        #import ipdb;ipdb.set_trace()
