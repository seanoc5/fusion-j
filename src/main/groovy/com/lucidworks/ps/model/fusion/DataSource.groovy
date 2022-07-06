package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger

/**
 * placeholder component app for Datasources (when/if necessary)
 * todo -- remove me, or make me useful, I am space at the moment....
 */
class DataSource implements BaseObject {
    Logger log = Logger.getLogger(this.class.name);

    @Override
    def export(File exportFolder) {
        log.info "export datasource to folder: ${exportFolder.absolutePath}"

        return null
    }

    @Override
    def export(FusionClient fusionClient) {
        throw new RuntimeException("Export to live fusion client not implemented yet...")
        return null
    }

}
