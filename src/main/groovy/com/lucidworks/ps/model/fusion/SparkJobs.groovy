package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class SparkJobs extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);

    SparkJobs(String appName, List<Map<String, Object>> srcJsonList) {
        super(appName, srcJsonList)
        log.debug "more code here...?"
    }

    SparkJobs(String appName, Map<String, Object> srcJsonMap) {
        super(appName, srcJsonMap)
        log.debug "more code here...?"
    }


    @Override
    Map<String, Object> assessItem(def item) {
        log.warn "\t\tmore code here: any gotchas in spark code???"
        Map itemAssessment = super.assessItem(item)
    }

    @Override
    int size() {
        return 1
    }
}
