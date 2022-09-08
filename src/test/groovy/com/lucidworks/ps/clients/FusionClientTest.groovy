package com.lucidworks.ps.clients

import org.apache.commons.compress.archivers.zip.ZipFile
import spock.lang.Specification

import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Testing fusion client
 * Note: this should be a functional test, not a unit test, refactor as appropriate...
 */
class FusionClientTest extends Specification {
    // todo -- make this more portable, currently limited to 'my' config, with a test app (F4), and hardcoded object names
    String appName = 'test'         //'Components'
    String qrypName = appName
    String idxpName = appName
    String furl = 'http://newmac:8764'
//    String furl = 'http://foundry.lucidworksproserve.com:6764'
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
        List qrypDefs = client.getIndexPipelines(appName, qrypName)
        Map qrypDef = qrypDefs[0]

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "should get specific indexpipe WITHOUT app"() {
        when:
        List pipelines = client.getIndexPipelines(null, qrypName)
        Map qrypDef = pipelines[0]

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }

    def "broken - should get specific datasource in app"() {
        when:
        List dsDefStr = client.getDataSources(appName, 'test')
        List dsDefPattern = client.getDataSources(appName, ~/t.*t/)

        then:
        dsDefStr instanceof List<Map<String, Object>>
        dsDefStr.size() == 1
        dsDefPattern instanceof List<Map<String, Object>>
        dsDefPattern.size() == 1
    }

    def "should get specific datasource WITHOUT app"() {
        when:
        List pipelines = client.getIndexPipelines('', qrypName)
        Map qrypDef = pipelines[0]

        then:
        qrypDef instanceof Map<String, Object>
        qrypDef.id == appName
    }


    def "should get blob definitions with various filtering"() {
        when:
        List<Map<String, Object>> blobDefsAll = client.getBlobDefinitions()
        List<Map<String, Object>> blobDefsApp = client.getBlobDefinitions(appName)
        List<Map<String, Object>> blobDefsFile = client.getBlobDefinitions('', 'file')
        List<Map<String, Object>> blobDefsSingle = client.getBlobDefinitions('', 'file', 'quickstart/arxiv-fusion.json')
        List<Map<String, Object>> blobDefsByPattern = client.getBlobDefinitions('', 'file', ~/(.*Liquor.*|.*arxiv.*)\.(json|csv)/)

        then:
        blobDefsAll instanceof List<Map<String, Object>>
        blobDefsAll.size() > blobDefsFile.size()
        blobDefsSingle.size() == 1
        blobDefsByPattern.size() == 2
    }

    def "should get single text blob WITHOUT app"() {
        when:
        def blobFF = client.getBlob('')

        then:
        blobFF
    }

    // todo -- fix this -- add more logic for processing and testing binary blobs -- currently fails
    def "should get binary text blob nlp/models/en-chunker.bin"() {
        when:
        HttpResponse.BodyHandler bodyHandler = HttpResponse.BodyHandlers.ofInputStream()
        def blobNlp = client.getBlob('nlp/models/en-chunker.bin', bodyHandler)

        then:
        blobNlp
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


    // probably not a good approach, consider in-memory approach by default? I don't like writing directly to filesystem (justified concern???)
    def "export objects ofFile (zip) response - collection.ids test"() {
        given:
//        List<String> collectionToGet = ['upval', 'Keymatches']
        String exportParams = 'collection.ids=test'
        Path exportFolder = Paths.get(getClass().getResource('/').toURI())
        String zipPath = exportFolder.toAbsolutePath().toString()
        Path exportZip = Paths.get(zipPath, 'collection.test.zip')
        println "Export zip file: ${exportZip.toAbsolutePath()}"
        def bodyHandler = HttpResponse.BodyHandlers.ofFile(exportZip)
//        FileAttributeView fav = Files.getFileAttributeView(exportZip)

        when:
        FusionResponseWrapper fusionResponseWrapper = client.exportFusionObjects(exportParams, bodyHandler)
        def results = fusionResponseWrapper.parsedInfo
        Map map = fusionResponseWrapper.parsedMap
        Map list = fusionResponseWrapper.parsedList

        then:
        Files.exists(exportZip)

    }


    // better approach? keep things in memory for more flexible handling transforming?? BEWARE memory issues...
    def "export objects inputstream response - collection test"() {
        given:
        String exportParams = 'collection.ids=test'
        Path exportFolder = Paths.get(getClass().getResource('/').toURI())
        Path exportZip = Paths.get(exportFolder.toAbsolutePath().toString(), 'collection.test.zip')

        when:
        FusionResponseWrapper fusionResponseWrapper = client.exportFusionObjects(exportParams)
        def zipFile = fusionResponseWrapper.parsedInfo              // does this make sense? FRW calls helper to convert stream to something zip-file-ish...?
        Map map = fusionResponseWrapper.parsedMap
        List list = fusionResponseWrapper.parsedList


        then:
        zipFile
        zipFile instanceof ZipFile
        map == null         // assuming there is nothing parse, beyond the zipfile/stream...
        list == null
    }


    def "get various collectionDefinitions"() {
        given:
        String collName = appName

        when:
        def allColls = client.getCollectionDefinitions()
        def filteredColls = client.getCollectionDefinitions(null, ~/t.*/)
        def filteredAppColls = client.getCollectionDefinitions(appName)
        def filteredAppMainColl = client.getCollectionDefinitions(appName, collName)

        def singleCollection = client.getCollectionDefinition(appName, collName)

        then:
        allColls instanceof List
        allColls.size() > 50         // todo -- improve test logic & abstraction

        filteredColls instanceof List<Map<String, Object>>
        filteredColls.size() < allColls.size()

        filteredAppColls.size() != filteredColls

        filteredAppMainColl.size() == 1
        filteredAppMainColl[0].id == singleCollection.id
    }
}
