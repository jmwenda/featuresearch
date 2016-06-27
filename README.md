=========================================================
Worldmap feature search
========================================================

This is a simple set of scripts used to access worldmap features and send them to a solr core.
This process was done in three steps with each file representing the steps taken and improving the scripts iteratively.
The current code which I would consider the best is the threadedsolrgeom.py file. This seems lightweight and seems faster for some reason.

solrfeatures.py
===================
This was the first initial push in taking features to the solr core. This has quite abit of write overhead. 

threadedsolr.py
===================
This indexes the bounding box by creating multiprocesses. This was an approach to try and see if this could speed up indexing feature extents

threadedsolrgeom.py
====================
As mentioned this is what I would consider very stable. This is because in a change in two main approaches.
1. Rather than indexing feature extents, this indexed the geometry's themselves and used to generate the heatmap
2. It also uses multiprocessing to speed up the procees and the area is caluclated from the PostGis query interface.


The solr congfiguration
============================
The solr configuration sits in the folder solrconfig/manage-schema. Of importance here is the the_geom character which spatial geometries are indeced. 
Due to space constrainsts, the indexed is false on this and so is storage on this field. the other fields are as follows
    LayerId: This is the uuid of the layer as depiced on worldmap
    FeatureId : This is the id of the feature
    LayerDate: this is the date from the worldmap api. i.e temporal start date/end date. In future there might be a need to link this to the hypermap API for the other avaiable dates
    LayerName: the layer name as referred to within worldmap
    Area : This is the area of the feature's geometry
    FeatureDate: This is if the date is part of the gazetter. In future, enhancements could be made to detect date columns in postgis
    the_geom : This is the geometry of the feature
    FeatureText: This is a concatenation of all strings that are character varying
    owner : This is the owner of the layer

Test client
================================
For the test client, I have been using the cga-worldmap ogpsearch client. The client could be reduced to a lightweight client to test with, with this
branch it is easy to get up and running
    https://github.com/cga-harvard/cga-worldmap/tree/ogpsearch2
To view data that has been indexed to the core some changed were made to the client inorder to be displayed. A branch with all these changes is here

    https://github.com/jmwenda/geonode/tree/featuresearch

Changes done include
To changes to the template to load the SOLR_URL
https://github.com/cga-harvard/cga-worldmap/blob/ogpsearch2/geonode/ogpsearch/templates/ogpsearch/ogpsearch.html
Need to change to the geometry field that will be used to display the heatmap
To change the geometry field, one needs to change the following files inorder to show the heatmap
    geonode/static/ogpsearch/resources/javascript/lib/models/heatmapModel.js
    https://github.com/cga-harvard/cga-worldmap/blob/ogpsearch2/geonode/static/ogpsearch/resources/javascript/lib/models/heatmapModel.js#L64
Need to change the table querying fields

    https://github.com/cga-harvard/cga-worldmap/blob/ogpsearch2/geonode/static/ogpsearch/resources/javascript/lib/solr.js#L135
    https://github.com/jmwenda/geonode/blob/7de4a2448a9c253131caf525b8604d6a7cba6b3a/geonode/static/ogpsearch/resources/javascript/lib/views/searchResultsTable.js
    https://github.com/cga-harvard/cga-worldmap/blob/ogpsearch2/geonode/static/ogpsearch/resources/javascript/lib/solr.js#L220-L223

Approaches which may need more investigation
As mentioned earlier, there are layers in the API that do not have any corresponsing tables in the database: the list of layers is depicted here
    https://gist.github.com/jmwenda/49d0df13e8452d11383227e7ea4f9579#file-gistfile1-txt
Upon investigation, it is not clear if they private or just do not exist.

The use of fetchmany instead of fetchall:
    When querying very large databases, fetchall does fail. This is mainly due to running out of memory.
    
   http://stackoverflow.com/questions/17199113/psycopg2-leaking-memory-after-large-query

Gazetter Indexing
=================================
This is only used to update features sitting in solr. First queries the gazeteer database, then queries the map_layer to get the id.
These two params are then used to query solr, and if the gazetter column has dates the solr record is the updated

This is done by running solrgazeteer.py. This should be run once the entire core /layers have been indexed

