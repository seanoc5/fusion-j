package com.lucidworks.ps.model.fusion


import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * placeholder component app for Datasources (when/if necessary)
 * todo -- remove me, or make me useful, I am space at the moment....
 */
class Parsers extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);
//    String appName = 'n.a.'
//    List<Map> jsonItems

    Parsers(String applicationName, List<Map<String, Object>> items) {
        super(applicationName, items)
    }

    @Override
    Map<String, Object> assessItem(Object item) {
        Map assessItem = super.assessItem(item)
    }
}
