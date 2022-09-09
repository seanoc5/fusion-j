package com.lucidworks.ps.model.fusion

import groovy.json.JsonSlurper
import spock.lang.Specification

/**
 * test parsing and evaluating index pipelines
 */
class PipelinesTest extends Specification {

    def "should load an example indexpipeline from file - basic test"() {
        given:
        File idxpJson = new File(getClass().getResource('/pipelines/indexPipeline.1.json').toURI())
        JsonSlurper jsonSlurper = new JsonSlurper()
        def jsonObject = jsonSlurper.parse(idxpJson)

        when:
        Pipelines pipelineWrapper = new Pipelines('unit test', jsonObject)

        then:
        pipelineWrapper.srcItems.size()==1
        pipelineWrapper.srcItems[0].id == 'testIdxPipeline'

    }
}
