package com.lucidworks.ps.model.fusion


import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * @author :    sean
 * @mailto :    seanoc5@gmail.com
 * @created :   6/15/22, Wednesday
 * @description:
 */

/**
 * wrapper class to allow feature-based logic and processing
 * i.e. help determine what are "support" collections vs "main" collections (this wrapper object is probably not necessary)
 * see https://doc.lucidworks.com/fusion/5.5/333/collection-features-api
 */
class Features extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);
    List<Map<String, Object>> items = []

    /**
     * Features from json (app export or rest api call) are a map, different from most cases
     * @param name of application
     * @param items slurped json items (map/list combo)
     */
    Features(String applicationName, Map map) {
        super(applicationName, map)
    }


}
