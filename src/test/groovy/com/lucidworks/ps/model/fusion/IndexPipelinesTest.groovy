package com.lucidworks.ps.model.fusion

import groovy.json.JsonSlurper
import spock.lang.Specification

/**
 * test parsing and evaluating index pipelines
 */
class IndexPipelinesTest extends Specification {
    File idxpJson = new File(getClass().getResource('/pipelines/indexPipeline.1.json').toURI())
    JsonSlurper jsonSlurper = new JsonSlurper()
    def jsonObject = jsonSlurper.parse(idxpJson)

    def "source Json matches expectation - pre-test"() {
        when:
        List<Map> pipelineList = jsonObject
        Map pipeline1 = pipelineList[0]
        List<Map> stages = pipeline1.stages

        then:
        jsonObject.size() == 1
        jsonObject instanceof List<Map>
        pipeline1.keySet().toList() == ['id', 'stages', 'properties']
        stages.size() == 6

    }

    def "should load an example indexpipeline from file - basic test"() {
        given:
        String singlePipelineId = 'testIdxPipeline'

        when:
        IndexPipelines idxpWrapper = new IndexPipelines('unit test', jsonObject)
        Set keysStages = idxpWrapper.pipelineStagesMap.keySet()
        def stages = idxpWrapper.pipelineStagesMap[singlePipelineId]
        def javascriptStages = idxpWrapper.javascriptStages[singlePipelineId]


        then:
        idxpWrapper instanceof IndexPipelines
        idxpWrapper.itemType == 'IndexPipelines'
        idxpWrapper.pipelineStagesMap.keySet().size() == 1
        stages.size() == 6
        stages[0].id == 'tip1'

        idxpWrapper.javascriptStages.keySet().size() == 1
        javascriptStages.size() == 2
        javascriptStages[0] instanceof Javascript
    }

    def "should export json string"(){
        when:
        IndexPipelines idxpWrapper = new IndexPipelines('unit test', jsonObject)
        def export = idxpWrapper.export()

        then:
        export instanceof String
        ((String)export).size() == 5277
        ((String)export).split('\n').size() == 128
        ((String)export).split('\n')[2] == '        "id": "testIdxPipeline",'

    }
}
