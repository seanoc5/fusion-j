package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class ObjectGroups extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);

    ObjectGroups(String appName, List<Map<String, Object>> srcJsonList) {
        super(appName, srcJsonList)
    }

    ObjectGroups(String appName, Map<String, Object> srcJsonMap) {
        super(appName, srcJsonMap)
    }


    @Override
    Map<String, Object> assessItem(def item) {
        Map itemAssessment = super.assessItem(item)
    }

    @Override
    int size() {
        return 1
    }
}
