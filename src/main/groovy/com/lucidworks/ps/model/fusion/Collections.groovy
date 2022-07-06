package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class Collections implements BaseObject{
    Logger log = Logger.getLogger(this.class.name);
    List<Map> jsonItems
    /**
     * helper main function to test functionality, change the file arg accordingly...
     * @param args
     */
//    static void main(String[] args) {
//        File src = new File('/home/sean/work/lucidworks/Intel/CircuitSearch.F5.zip')
//        Collections app = new Collections(src)
////        app.getThingsToCompare()
//        app.log.info(app)
//    }


    Collections(File appOrJson) {
        log.info "Parsing source file: ${appOrJson.absolutePath} (app export, or json...)"
        def parseResult = parseSourceFile(appOrJson)
    }

    @Override
    def export(File exportFolder) {
        log.info "export collections to folder: ${exportFolder.absolutePath}"

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
        jsonItems = app.collections

    }


}
