package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.Helper
import com.lucidworks.ps.model.BaseObject
import groovy.json.JsonOutput
import org.apache.log4j.Logger

/**
 * Fusion Application helper class - wraps a group of pipelines (i.e. for a given fusion app)
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class Pipelines extends BaseObject {
    static Logger log2 = Logger.getLogger(this.class.name);
    /** map of the source objects (pipeline jsonobjects), converted from list to map (id as key)  */
    Map<String, List<PipelineStages>> pipelines = [:]

    /** map of stages grouped by pipeline id/key */
    Map<String, List<PipelineStages>> pipelineStagesMap = [:]
    /** map of javascript ALL pipeline stages grouped by pipeline id/key -- helpful in export and assessement */
    Map<String, List<Javascript>> javascriptStages = [:]

    Pipelines(String applicationName, List<Map<String, Object>> pipelineJsonObjects) {
        super(applicationName, pipelineJsonObjects)
        pipelineJsonObjects.each { Map pipelineJsonMap ->
            String pipelineID = pipelineJsonMap.id
            // todo --refactor, this is redundant, and a temp hack as I work through exporting/evaluating javascript en-mass...
            pipelines[pipelineID] = pipelineJsonMap

            if (pipelineJsonMap.stages) {
                List<Javascript> jsStages = []
                pipelineJsonMap.stages.each {
                    String stageId = it.id
                    pipelineStagesMap["${pipelineID}-${stageId}"] = it
                    String type = it.type
                    if (type.containsIgnoreCase('javascript')) {
                        String label = "$applicationName.$pipelineID.$stageId.$type"
                        String script = it.script?.trim()
                        if (script) {
                            Javascript js = new Javascript(label, script, this.itemType)
                            jsStages << js
                        } else {
                            log2.warn "Problem finding script in JS stage ($label)...?"
                        }
                    }
                }

                if (jsStages) {
                    javascriptStages[pipelineID] = jsStages
                } else {
                    log2.debug "no js stages in $pipelineID"
                }
                log2.debug "jsStages: ${jsStages.size()}"
            } else {
                log2.warn "!!!!!!!!!!!! No stages in pipeline ($pipelineID): $pipelineJsonMap"
            }
        }
        log2.debug "Javascript stages: $javascriptStages"
    }

    @Override
    def export(File exportFolder) {
        log2.info "export Pipelines (count:${this.srcJsonList.size()}) to folder: ${exportFolder.absolutePath}"
        List<File> exportedFiles = []
        srcJsonList.each { Map pipeline ->
            String id = pipeline.id
            // todo -- look at library (apache commons-text??) to sanitize filenames...?
            String outname = id           //  "${appName}.${id}" // appname overkill???
            File outfile = new File(exportFolder, outname + ".json")
            // todo -- handle non-text output...
            String jsonText = jsonDefaultOutput.toJson(pipeline)
            String prettyJson = JsonOutput.prettyPrint(jsonText)
            outfile.text = prettyJson
            exportedFiles << outfile
            log2.info "\t\texported index pipeline with id ($id) to file: ${outfile.absolutePath}"

            def jsStages = pipeline.stages.findAll { ((String) it.type).containsIgnoreCase('javascript') }
            jsStages.each {
                String stageId = it.id
//                log.debug "export javascript for this stage ($stageId)"
                String jsname = "${outname}.javascript.${id}.js"
                File jsOutfile = new File(exportFolder, jsname)
                jsOutfile.text = it.script
                log2.info "\t\twrote javascript (helper) file: ${jsOutfile.absolutePath}"
            }

            File outStageFolder = new File(exportFolder, 'stages')
            def rc = Helper.getOrMakeDirectory(outStageFolder)
            pipeline.stages.each { def stage ->
                log2.debug "stage to export: ${stage.type}"
                File f = new File(outStageFolder, "${stage.type}.${outname}.${stage.id}.json")
                String jsonTextStage = jsonDefaultOutput.toJson(stage)
                String prettyJsonStage = JsonOutput.prettyPrint(jsonTextStage)
                f.text = prettyJsonStage
                exportedFiles << f
            }
        }
        return exportedFiles
    }

    @Override
    Map<String, Object> assessItem(def item) {
        Map itemAssessment = super.assessItem(item)

    }


}
