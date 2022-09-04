package com.lucidworks.ps.clients

import org.apache.commons.compress.archivers.zip.ZipFile
import spock.lang.Specification

import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Testing fusion client
 * Note: this should be a functional test, not a unit test, refactor as appropriate...
 */
class FusionClientTest extends Specification {
    // todo -- make this more portable, currently limited to 'my' config, with a test app (F4), and hardcoded object names
    String appName = 'test'
    String qrypName = appName
    String idxpName = appName
    String furl = 'http://newmac:8764'
    FusionClient client = new FusionClient(furl, 'sean', 'pass1234', appName)

    def "should successfully buildClient"() {
//        given:

        when:
        def apiInfo = client.getFusionInformation()

        then:
        apiInfo instanceof Map
        client.majorVersion >= 4
    }

    def "should buildGetRequest"() {
        given:
        String url = "$furl/api/apps"

        when:
        HttpRequest request = client.buildGetRequest(url)

        then:
        request instanceof HttpRequest
        request.uri().toString() == url
    }

    def "should getApplications"() {
        when:
        def apps = client.getApplications()

        then:
        apps instanceof List<Map<String, Object>>
    }

    def "should get all querypipelines in an app"() {
        when:
        List qrypList = client.getQueryPipelines(appName)

        then:
        qrypList instanceof List<Map<String, Object>>
        qrypList.size() > 1
    }

    def "should get specific querypipe in app"() {
        when:
        Map qrypDef = client.getQueryPipeline(qrypName, appName)

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "should get specific querypipe WITHOUT app"() {
        when:
        Map qrypDef = client.getQueryPipeline(qrypName)

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "should get all indexpipelines in an app"() {
        when:
        List qrypList = client.getIndexPipelines(appName)

        then:
        qrypList instanceof List<Map<String, Object>>
        qrypList.size() > 1
    }

    def "should get specific indexpipe in app"() {
        when:
        Map qrypDef = client.getIndexPipeline(qrypName, appName)

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "should get specific indexpipe WITHOUT app"() {
        when:
        Map qrypDef = client.getIndexPipeline(qrypName)

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "should get specific datasource in app"() {
        when:
        Map qrypDef = client.getDataSource()

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "should get specific datasource WITHOUT app"() {
        when:
        Map qrypDef = client.getIndexPipeline(qrypName)

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "should get all blob definitions WITHOUT app"() {
        when:
        List<Map<String, Object>> blobDefs = client.getBlobDefinitions()

        then:
        blobDefs instanceof List<Map<String, Object>>
        blobDefs.size() > 1
    }

    def "should get blob definitions WITHOUT app and various filtereing"() {
        when:
        List<Map<String, Object>> blobDefsAll = client.getBlobDefinitions()
        List<Map<String, Object>> blobDefsFile = client.getBlobDefinitions('','file')
        List<Map<String, Object>> blobDefsSingle = client.getBlobDefinitions('','file', 'aw-firefox.json')
        List<Map<String, Object>> blobDefsByPattern = client.getBlobDefinitions('','file', ~/(aw-firefox|.*arxiv.*).json/)

        then:
        blobDefsAll instanceof List<Map<String, Object>>
        blobDefsAll.size() > blobDefsFile.size()
        blobDefsSingle.size()==1
        blobDefsByPattern.size()==2
    }

    def "should get single text blob WITHOUT app"() {
        when:
        def blobFF = client.getBlob('sampleLocations.csv')

        then:
        blobFF
    }

    // todo -- fix this -- add more logic for processing and testing binary blobs -- currently fails
    def "should get binary text blob nlp/models/en-chunker.bin"() {
        when:
        HttpResponse.BodyHandler bodyHandler = HttpResponse.BodyHandlers.ofInputStream()
        def blobFF = client.getBlob('nlp/models/en-chunker.bin', bodyHandler)

        then:
        blobFF
    }


/*
    def "test getFusionInformation"() {
        given:

        when:
        // TODO implement stimulus
        then:
        // TODO implement assertions
    }
*/

    def "isValidFusionClient"() {
//        given:

        when:
        boolean isValid = client.isValidFusionClient()

        then:
        isValid == true
    }

    def "query should return json result object"() {
        given:
        String q = '*:*'
        String collName = appName
        String qrypName = appName
        Map qryParams = [q: q]

        when:
        Map result = client.query(appName, collName, qrypName, qryParams,)
        Map response = result.response                          // standard oddity in solr response naming, so calling the solr 'response' as 'results' rather than response.response ...
        FusionResponseWrapper fwr = client.responses[-1]        // hack getting last response, but ok in this test..?
        def info = fwr.parsedInfo
        def nf = response.numFound

        then:
        info instanceof Map
        fwr.wasSuccess()
        response.keySet().contains('numFound')
        nf instanceof Integer
        nf > 0
    }

/*
    def "get configset map response- test"() {
        given:
        List<String> configsetsToGet = ['upval', 'Keymatches']

        when:
        def results = client.getConfigSets(configsetsToGet)
//        FusionResponseWrapper fwr = client.getConfigSets()
//        Map info = fwr.parsedInfo
//        Map response = info.response

        then:
        results instanceof Map
//        fwr.wasSuccess()
//        response.keySet().contains('numFound')

    }
*/

/*
    def "export objects ofFile response - collection test"() {
        given:
        List<String> collectionToGet = ['upval', 'Keymatches']
        String exportParams = 'collection.ids=test'
        Path exportFolder = Paths.get(getClass().getResource('/').toURI())
        Path exportZip = Paths.get(exportFolder.toAbsolutePath().toString(), 'collection.test.zip')

        when:
        FusionResponseWrapper fusionResponseWrapper = client.exportFusionObjects(exportParams, exportZip)
        def results = fusionResponseWrapper.parsedInfo
        Map map = fusionResponseWrapper.parsedMap
        Map list = fusionResponseWrapper.parsedList


        then:
        map instanceof Map
        map.size() > 0
        map['objects.json']

    }
*/

    def "export objects inputstream response - collection test"() {
        given:
        List<String> collectionToGet = ['upval', 'Keymatches']
        String exportParams = 'collection.ids=test'
        Path exportFolder = Paths.get(getClass().getResource('/').toURI())
        Path exportZip = Paths.get(exportFolder.toAbsolutePath().toString(), 'collection.test.zip')

        when:
        FusionResponseWrapper fusionResponseWrapper = client.exportFusionObjects(exportParams)
        def results = fusionResponseWrapper.parsedInfo
        Map map = fusionResponseWrapper.parsedMap
        List list = fusionResponseWrapper.parsedList


        then:
        map instanceof Map
        map.size() > 0
        map['objects.json'] instanceof ZipFile.Entry
    }
}
