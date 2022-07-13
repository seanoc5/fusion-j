package com.lucidworks.ps.model.fusion


import org.apache.log4j.Logger
/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class IndexPipelines extends Pipelines {
    Logger log = Logger.getLogger(this.class.name);

    IndexPipelines(String applicationName, List<Map<String, Object>> items) {
        super(applicationName, items)
        this.type = this.getClass().simpleName
/*
        items.each { Map pipelineJsonMap ->
            String pipelineID = pipelineJsonMap.id
            if (pipelineJsonMap.stages) {
                pipelineStagesMap[pipelineID] = pipelineJsonMap.stages
                List<Javascript> jsStages = []
                pipelineJsonMap.stages.each {
                    String stageId = it.id
                    String type = it.type
                    if(type.containsIgnoreCase('javascript')){
                        String label = "$applicationName :: $pipelineID :: $stageId :: $type"
                        String script = it.script?.trim()
                        if(script) {
                            Javascript js = new Javascript(label, script)
                            jsStages << js
                        } else {
                            log.warn "Problem finding script in JS stage ($label)...?"
                        }
                    }
                }

                if(jsStages) {
                    javascriptStages[pipelineID] = jsStages
                } else {
                    log.debug "no js stages in $pipelineID"
                }
                log.debug "jsStages: ${jsStages.size()}"
            } else {
                log.warn "No stages in pipeline ($pipelineID): $pipelineJsonMap"
            }
        }
*/
        log.debug "Javascript stages: $javascriptStages"
    }

    def getJavascriptStages(){

    }

/*
    @Override
    def export(File exportFolder) {
        log.info "export indexPipelines (count:${this.srcJsonList.size()}) to folder: ${exportFolder.absolutePath}"
        List<File> exportedFiles = []
        srcJsonList.each { Map pipeline ->
            String id = pipeline.id
            // todo -- look at library (apache commons-text??) to sanitize filenames...?
            String outname = "indexPipeline.${appName}.${id}"
            File outfile = new File(exportFolder, outname + ".json")
            // todo -- handle non-text output...
            String jsonText = jsonDefaultOutput.toJson(pipeline)
            String prettyJson = JsonOutput.prettyPrint(jsonText)
            outfile.text = prettyJson
            exportedFiles << outfile
            log.debug "exported index pipeline with id ($id) to file: ${outfile.absolutePath}"

            def jsStages = pipeline.stages.findAll { ((String) it.type).containsIgnoreCase('javascript') }
            jsStages.each {
                String stageId = it.id
                log.debug "export javascript for this stage ($stageId)"
                String jsname = "${outname}.javascript.${id}.js"
                File jsOutfile = new File(exportFolder, jsname)
                jsOutfile.text = it.script
//                log.info "wrote javascript (helper) file: ${jsOutfile.absolutePath}"
            }

        }
        return exportedFiles

        return null
    }
*/

//    @Override
//    Map<String, Object> assessItem(def item) {
//        Map itemAssessment = super.assessItem(item)
//
//    }


}
