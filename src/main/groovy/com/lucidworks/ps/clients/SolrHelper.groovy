package com.lucidworks.ps.clients

import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrResponse
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition
import org.apache.solr.client.solrj.request.schema.SchemaRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.response.SolrPingResponse
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation
import org.apache.solr.client.solrj.response.schema.SchemaResponse
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.util.NamedList
/**
 * LW pro-serv helper class to set standard and convenient solr access
 * Low-priority -- possibly remove/deprecate...?
 */
class SolrHelper {
    protected static Logger log = Logger.getLogger(this.class.name);
    String baseUrl
    HttpSolrClient baseClient
    HttpSolrClient collectionClient
    String primaryCollection
    def clusterStatus

    SolrPingResponse collectionPingResponse
    NamedList cluster
    NamedList collections
    Map aliases
    List liveNodes

    SchemaRepresentation schemaRepresentation
    List<Map<String, Object>> fields
    List<Map<String, Object>> dynamicFields
    List<FieldTypeDefinition> fieldTypes
    List<Map<String, Object>> copyFields

    /**
     * simple main app as basic functionality test -- likely out of date or needing customization
     *
     * @param args
     */
    static void main(String[] args) {
        String url = 'http://localhost:8983/solr'
        String coll = 'test'
        SolrHelper solrHelper = new SolrHelper(url, coll)
        SolrQuery sq = new SolrQuery('*:*')
        SolrResponse rsp = solrHelper.query(sq)
        log.debug "Response: $rsp"
        SolrDocumentList sdl = rsp.getResults()
        log.info "Solr Docs size: ${sdl.size()}"
    }


    SolrHelper(String baseUrl, String collection) {
        this.baseUrl = baseUrl
        this.primaryCollection = collection
        log.info "Build solr collection-oriented client with baseUrl: '$baseUrl' -- collection: '$collection' ..."

        // trying out a 2-client approach: base and primary-collection-specific clients
        collectionClient = buildSolrCollectionClient(baseUrl, collection)
        collectionPingResponse = collectionClient.ping()
        log.info "\t\tCollection ($collection) Ping response: $collectionPingResponse"

        baseClient = buildSolrCollectionClient(baseUrl)
        clusterStatus = baseClient.request(new CollectionAdminRequest.ClusterStatus())
        log.debug "Cluster status: $clusterStatus"
        showClusterInformation(clusterStatus)

        schemaRepresentation = loadCollectionSchema()

        log.info "Done...?"
    }

    /**
     * Various information about the chosen cluster -- mostly for development and debugging
     * @param status
     * @return
     */
    def showClusterInformation(def status) {
        NamedList responseHeader = status.responseHeader
        cluster = status.cluster
        collections = cluster.collections
//        collections.each {String key, Map valueMap ->
        collections.each {
            String key = it.key
            Map valueMap = it.value
            log.debug "\t\tkey: $key  -- $valueMap"
            log.info "Collection [$key] -> repFactor:${valueMap.replicationFactor} -- # shards: ${valueMap.shards.size()} -- config: ${valueMap.configName}"
        }
        aliases = cluster.aliases
        liveNodes = cluster.live_nodes
        log.info "Live nodes: $liveNodes"

        log.debug "Status: $status"
    }

    /**
     * Build a solr client focused on a 'primary' collection
     * Assuming there is one SolrHelper per main solr collection -- supported by a baseClient for more general information
     * @param baseUrl
     * @param collection
     * @return
     */
    HttpSolrClient buildSolrCollectionClient(String baseUrl, String collection) {
        log.info "build solr client: url:'$baseUrl' -- collection: $collection"
        HttpSolrClient client
        try {
            String urlColl = "${baseUrl}/$collection"
            log.info "\t\tUrl with collection: $urlColl"
            client = new HttpSolrClient.Builder(urlColl).build();
        } catch (BaseHttpSolrClient.RemoteSolrException rse) {
            String fubarMsg = rse.message
            if (fubarMsg.containsIgnoreCase('problem accessing')) {
                log.warn "Problem accessing ping for url given: '$baseUrl' -- check solr url and collection (if included) -- Example:[http://localhost:8983/solr/mycollection]  "
            } else {
                log.warn "Unknown RemoteSolr exception: $rse"
            }
        }
//        solr.setParser(new XMLResponseParser());
        return client
    }

    def loadCollectionSchema() {
        log.info "Requesting solr core...."
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(collectionClient);
        SchemaRepresentation schemaRepresentation = schemaResponse.getSchemaRepresentation();
          log.debug "Schema representation: $schemaRepresentation"
        log.info "Schema name: ${schemaRepresentation.getName()}"
//          assertEquals(1.6, schemaRepresentation.getVersion(), 0.001f);
//          assertEquals("id", schemaRepresentation.getUniqueKey());
          fields = schemaRepresentation.getFields()
          dynamicFields = schemaRepresentation.getDynamicFields()
          fieldTypes = schemaRepresentation.getFieldTypes()
          copyFields = schemaRepresentation.getCopyFields()

//        CoreAdminRequest request = new CoreAdminRequest();
//        request.setAction(CoreAdminParams.CoreAdminAction.STATUS);

/*        CoreAdminResponse cores
        try {
            cores = request.process(collectionClient);
            log.debug "Cores: $cores"
        } catch (SolrServerException e) {
            log.warn "Solr server exception: $e"
        } catch (IOException e) {
            log.warn "IO Exception: $e"
        } catch (Exception e){
            log.error "Exception: $e"
        }
        */
//        return cores
        return schemaRepresentation
    }

    /**
     * Build a 'base' client for cluster information and other general information and operation
     * @param baseUrl
     * @return
     */
    HttpSolrClient buildSolrCollectionClient(String baseUrl) {
        log.info "build solr client: url:'$baseUrl' -- NO collection (general overview client...?):"
        SolrClient client
        try {
            client = new HttpSolrClient.Builder(baseUrl).build();
        } catch (BaseHttpSolrClient.RemoteSolrException rse) {
            String fubarMsg = rse.message
            if (fubarMsg.containsIgnoreCase('problem accessing')) {
                log.warn "Problem accessing ping for url given: '$baseUrl' -- check solr url and collection (if included) -- Example:[http://localhost:8983/solr/mycollection]  "
            } else {
                log.warn "Unknown RemoteSolr exception: $rse"
            }
        }
        return client
    }

    /**
     * Helper function to submit a simple text query (development/debugging...)
     * @param q
     * @return
     */
    def query(String q) {
        SolrQuery sq = new SolrQuery(q)
        QueryResponse rsp = query(sq)
        return rsp
    }

    /**
     * more advanced query (helper) to accept a proper solr query object,
     * wrapper will evenutally have some opinionated process/wrapper functionality
     * @param sq
     * @return
     */
    def query(SolrQuery sq) {
        log.info "Solrquery:($sq)"
        QueryResponse rsp = collectionClient.query(sq)
        log.info "Solr response: $rsp"
        return rsp
    }


}

/*
todo - cursor mark export
todo - build analysis collections per topic
 */
