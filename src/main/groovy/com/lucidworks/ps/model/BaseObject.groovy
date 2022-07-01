package com.lucidworks.ps.model

import org.apache.log4j.Logger

/**
 * Base object to facilitate exporting to various destinations (filesystem, fusion instance,...)
 */
public interface BaseObject {
    static final Logger log = Logger.getLogger(this.class.name);

    default def export() {
        // todo add default functionality here...?
        log.info "export me"
    }
}
