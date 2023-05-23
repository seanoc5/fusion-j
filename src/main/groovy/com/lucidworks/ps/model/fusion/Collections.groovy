package com.lucidworks.ps.model.fusion


import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class Collections extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);
//    String appName = 'n.a.'
//    List<Map> jsonItems

//    Collections(File appOrJson) {
//        log.info "Parsing source file: ${appOrJson.absolutePath} (app export, or json...)"
//        def parseResult = parseSourceFile(appOrJson)
//    }

    Collections(String applicationName, List<Map<String, Object>> items) {
        super(applicationName, items)
        log.info "\t\tcalled super(applicationName, items) constructor: "
    }

    @Override
    def export(File exportFolder) {
        def outFile = super.export(exportFolder)
        log.info "export collections to folder: ${exportFolder.absolutePath}"
        return outFile
    }

//    @Override
//    def export(FusionClient fusionClient) {
//        throw new RuntimeException("Export to live fusion client not implemented yet...")
//        return null
//    }

    @Override
    Map<String, Object> assessItem(def collection) {
        Map assessment = super.assessItem(collection)
        int complexity = 0
        String clusterId = collection.searchClusterId

        Map solrParams = collection.solrParams
        String name = solrParams.name
        log.debug "\t\tProcessing collection: ${name.padRight(30)}  --  clusterid ($clusterId)"

        complexity = assessCollectionName(name, complexity, assessment)

        Integer commitWithin = collection.commitWithin
        if (commitWithin && commitWithin < 1000) {
            Map item = [:]
            if (commitWithin < 500) {
                item.complexity = 5
                complexity += 5
                item.message = "Commitwithin < 500, could mean complexity(?)"
            } else {
                item.complexity = 1
                complexity++
                item.message = "Commitwithin < 1000, could mean complexity(?)"
            }
            assessment.commitWithin = item
        } else {
            assessment.commitWithin = [message: "Commitwithin ($commitWithin) normal, no added compexity", complexity: 0]
        }

        // todo parse createdAt, check for age/outdated

        int numShards = solrParams?.numShards ?: 0
        if (numShards > 3) {
            int i = numShards / 3
            complexity += i
            assessment.numShards = [message: "NumShards ($numShards) added complexity: $i", complexity: i]
        } else {
            assessment.numShards = [message: "NumShards ($numShards) normal, no added compexity", complexity: 0]
        }

        int replicationFactor = solrParams?.replicationFactor ?: 0
        if (replicationFactor > 3) {
            int i = replicationFactor / 3
            complexity += i
            assessment.replicationFactor = [message: "Replication factor ($replicationFactor) added complexity: $i", complexity: i]
        } else {
            assessment.replicationFactor = [message: "Replication factor ($replicationFactor) normal, no added compexity", complexity: 0]
        }

        String type = collection.type
        if (type.equalsIgnoreCase('data')) {
            complexity++
            assessment.type = [message: "$name) type ${type} (data?) adding minor complexity of 1", complexity: 1]
        } else {
            assessment.type = [message: "Type ($type) normal, no added compexity", complexity: 0]
        }

        assessment.complexity = complexity
        log.debug "\t\tcollection: ${name.padLeft(30)}  :: complexity (${assessment.complexity}) ::: $assessment"

        return assessment
    }

    public int assessCollectionName(String name, int complexity, Map<String, Object> assessment) {
        if (name.containsIgnoreCase('alias')) {
            complexity++
            Map item = [message: "${name}) Alias in name, added complexity: 1", complexity: 1]
            assessment.alias = item
//        } else if(name.matches('.*(signals'){

        } else {
            log.debug "No alias, ignore..."
            //            collComplexity.alias = [message: "Not alias, no added compexity", complexity: 0]
        }
        complexity
    }
//
//    Map<String, Object> parseSourceFile(File appOrJson) {
//        Application app = new Application(appOrJson)
//        log.warn "refactor... assume app is calling the creation of a new collections set..."
//        jsonItems = app.collections
//
//    }


}
