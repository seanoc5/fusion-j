package com.lucidworks.ps.clients

import org.apache.commons.compress.archivers.zip.ZipFile
import spock.lang.Specification

import java.net.http.HttpRequest
import java.nio.file.Path
import java.nio.file.Paths
/**
 * Testing fusion client
 * Note: this should be a functional test, not a unit test, refactor as appropriate...
 */
class FusionClientTest extends Specification {
    String appName = 'test'
    String furl = 'http://newmac:8764'
    FusionClient client = new FusionClient(furl, 'sean', 'pass1234', appName)

    def "test buildClient"() {
//        given:

        when:
        def apiInfo = client.getFusionInformation()

        then:
        apiInfo instanceof Map
        client.majorVersion >= 4
    }

    def "test buildGetRequest"() {
        given:
        String url = "$furl/api/apps"

        when:
        HttpRequest request = client.buildGetRequest(url)

        then:
        request instanceof HttpRequest
        request.uri().toString() == url
    }

    def "test getApplications"() {
//        given:

        when:
        def apps = client.getApplications()

        then:
        apps instanceof List<Map<String, Object>>
    }

/*
    def "test getApplication"() {
        given:

        when:
        // TODO implement stimulus
        then:
        // TODO implement assertions
    }
*/

/*
    def "test getFusionInformation"() {
        given:

        when:
        // TODO implement stimulus
        then:
        // TODO implement assertions
    }
*/

    def "test isValidFusionClient"() {
//        given:

        when:
        boolean isValid = client.isValidFusionClient()

        then:
        isValid == true
    }

    def "test query"() {
        given:
        String q = '*:*'
        String collName = appName
        String qrypName = appName
        Map qryParams = [q: q]

        when:
        FusionResponseWrapper fwr = client.query(appName, collName, qrypName, qryParams,)
        Map info = fwr.parsedInfo
        Map response = info.response

        then:
        fwr.wasSuccess()
        response.keySet().contains('numFound')

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
