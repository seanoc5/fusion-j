package com.lucidworks.ps.model.fusion


import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * placeholder component app for Datasources (when/if necessary)
 * todo -- remove me, or make me useful, I am space at the moment....
 */
class AppKits extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);

    AppKits(String appName, List<Map<String, Object>> srcJsonList) {
        super(appName, srcJsonList)
    }

    AppKits(String appName, Map<String, Object> srcJsonMap) {
        super(appName, srcJsonMap)
    }

    @Override
    def export(File exportFolder) {
        log.info "export to folder: ${exportFolder.absolutePath}"
        return null
    }

    @Override
    Map<String, Object> assessComplexity() {
        return super.assessComplexity()
    }

    @Override
    Map<String, Object> assessItem(Object item) {
        return super.assessItem(item)
    }

    @Override
    Map<String, Object> assessItem(String itemName, Object item) {
        return super.assessItem(itemName, item)
    }

    @Override
    String getItemName(Map item) {
        String name = "appkitApp-" + item['appkitApp'].id
    }
}
