package com.lucidworks.ps.model.fusion


import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger

/**
 * placeholder component app for Datasources (when/if necessary)
 * todo -- remove me, or make me useful, I am space at the moment....
 */
class DataSources extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);


    DataSources(String applicationName, List<Map<String, Object>> items) {
        super(applicationName, items)
    }

    @Override
    Map<String, Object> assessItem(Object item) {
        Map assessment = super.assessItem(item)
        int comp = getComplexity(item)
        def c = assessment.complexity
        c+= comp
        assessment["complexity"] = c
        return assessment
    }

    @Override
    Map<String, Object> assessItem(String itemName, Object item) {
        Map assessment = super.assessItem(itemName, item)
    }

    /**
     * Calculate complexity based on (programmer) domian knowledege -- can and should be improved and/or extened
     * @param dsMap
     * @return semi-arbitrary complexity number (can be improved)
     */
    int getComplexity(Map dsMap) {
        int comp = 0
        switch (dsMap.type) {
            case 'web':
            case 'solr':
                log.debug "${dsMap.type} upgrade is simple...(?)"
                comp = 1
                break

            case 'jdbc':
            case 'confluence':
                log.info "${dsMap.type} upgrade might have complexity, but should on the simple side..."
                comp = 2
                break

            case 'fileupload':
                log.info "${dsMap.type} upgrade is simple...(?)"
                comp = 1
                break

            default:
                log.info "(${dsMap.type}) datasource upgrade is not yet address, assume '1'?"
                comp = 1
                break

        }
        return comp
    }

/*
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
*/

}
