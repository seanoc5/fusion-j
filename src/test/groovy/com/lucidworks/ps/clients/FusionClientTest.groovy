package com.lucidworks.ps.clients

import spock.lang.Specification

import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * Testing fusion client
 * Note: this should be a functional test, not a unit test, refactor as appropriate...
 */
class FusionClientTest extends Specification {
    String appName= 'test'
    String furl = 'http://localhost:8764/'
    FusionClient client = new FusionClient(furl, 'sean', 'pass1234', appName)

    def "test buildClient"() {
//        given:

        when:
        def apiInfo = client.getFusionInformation()

        then:
        apiInfo instanceof Map
    }

    def "test buildGetRequest"() {
        given:
        String url = "$furl/api/apps"

        when:
        HttpRequest request = client.buildGetRequest(url)

        then:
        request instanceof HttpRequest
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
        Map qryParams = [q:q]

        when:
        def results = client.query(appName, collName, qrypName, qryParams, )
        then:
        results instanceof HttpResponse<String>

    }
}
