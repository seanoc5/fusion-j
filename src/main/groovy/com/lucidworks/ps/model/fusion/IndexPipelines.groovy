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
        log.debug "Javascript stages: $javascriptStages"
    }

//    def getJavascriptStages(){
//
//    }


//    @Override
//    Map<String, Object> assessItem(def item) {
//        Map itemAssessment = super.assessItem(item)
//
//    }


}
