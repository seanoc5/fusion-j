package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class QueryPipelines implements BaseObject{
    Logger log = Logger.getLogger(this.class.name);
    List<Map> jsonItems
    String appName

/*
    Pipelines(File appOrJson) {
        log.info "Parsing source file: ${appOrJson.absolutePath} (app export, or json...)"
        def parseResult = parseSourceFile(appOrJson)
    }
*/
    Pipelines(String applicationName, List<Map<String,Object>> items){
        appName = applicationName
        jsonItems = items
    }

    @Override
    def export(File exportFolder) {
        log.info "export collections to folder: ${exportFolder.absolutePath}"
        List<File> exportedFiles = []
        jsonItems.each { Map pipeline ->
            String id = pipeline.id
            // todo -- look at library (apache commons-text??) to sanitize filenames...?
            String outname = "queryPipeline.${appName}.${id}.json"
            File outfile = new File(exportFolder, outname )
            // todo -- handle non-text output...
            outfile.text = value
            exportedFiles << outfile
        }
        return exportedFiles

        return null
    }

    @Override
    def export(FusionClient fusionClient) {
        throw new RuntimeException("Export to live fusion client not implemented yet...")
        return null
    }


    Map<String, Object> parseSourceFile(File appOrJson) {
        Application app = new Application(appOrJson)
        log.warn "refactor... assume app is calling the creation of a new collections set..."
        jsonItems = app.queryPipelines
    }


}
