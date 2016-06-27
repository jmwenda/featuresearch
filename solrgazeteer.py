import requests
import json
import psycopg2
import time

def gazeteer_fetch_features(conn_string,conn_string_legacy):
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

                if row[8] and (row[9] is None):
                    print 'missing'
                    import ipdb;ipdb.set_trace()
                if row[9] and (row[8] is None):
                    print 'missing'
                    import ipdb;ipdb.set_trace()


if __name__ == "__main__":
    solr_url = "http://127.0.0.1:8983/solr/featuresearch"
    #this holds information of the gazetter
    conn_string = "host='localhost' dbname='geonode_imports' user='geonode' password='geonode'"
    #query the layer metadata inorder to do queries/ could be done away by using layername rather than id
    conn_string_legacy = "host='localhost' dbname='geonode' user='geonode' password='geonode'"
    gazeteer_fetch_features(conn_string,conn_string_legacy)
