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
 * todo -- review and either remove, or flesh out...
 */
class Feature implements BaseObject{
    Logger log = Logger.getLogger(this.class.name);
    List<Map<String, Object>> items = []

    /**
     * normal constructor
     * @param name
     * @param items
     */
    Feature(List<Map<String, Object>> items) {
        log.debug "Normal constructor: $items"
        this.items = items
    }
}
