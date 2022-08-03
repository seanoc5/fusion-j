package com.lucidworks.ps.model.solr

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * wrapper object to bundle groups of config sets
 */
class ConfigSetCollection extends BaseObject{
    Logger log = Logger.getLogger(this.class.name);
    String deploymentName          // can be empty if getting from solr direcltly (i.e. all apps all configsets...?)
    Map<String, List<ConfigSet>> configsetMap = [:]
    // first group is the configset name (collection-name), second group is the path to use in the configset
    public static final Pattern DEFAULT_KEY_PATTERN = ~/.*configsets\/([^\/]+)(\/.*)/
    int SIZE_DIVIDER = 10


    ConfigSetCollection(Map configsetCollection, String deploymentName) {
        Integer collectionSize = parseConfigsetCollection(configsetCollection, deploymentName)
        log.debug "Fusion configsetCollection (with ${configsetCollection.size()} source items) constructor, with (${collectionSize} grouped config sets)..."
    }


    @Override
    Map<String, Object> assessComplexity() {
        log.warn "todo -- add more code to assess complexity of migrating solr from older version to newer..."

        Map assessment = super.assessComplexity()
        assessment.size = configsetMap.size()
        int sizeComplexity = configsetMap.size() / SIZE_DIVIDER
        if(sizeComplexity> 0){
            String msg = "Configsets have ${configsetMap.size()} items, which we divided by SIZE_DIVIDER(${SIZE_DIVIDER}) to calculate: $sizeComplexity for complexity"
            Map sizeAssess = [assessmentType:'ConfigsetSize', complexity:sizeComplexity, items:[msg]]
            assessment.complexity += sizeComplexity
            assessment.items << sizeAssess
        }
//        configsetMap.each {String name, List<ConfigSet> configsets ->
        configsetMap.each {String name, def configsets ->
            log.info "\t\t$name) Assess list of configsets: $configsets (${configsets.getClass().simpleName})"
            int i = 0
            configsets.each { ConfigSet cfg ->
                i++
                log.info "\t\t$i) configset: $cfg"
                Map csAssessment = cfg.assessComplexity()
                int cmplx = csAssessment.complexity
                if(cmplx) {
                    assessment.complexity += cmplx
                }
                assessment.items << csAssessment
            }
        }
        return assessment
    }

    @Override
    Map<String, Object> assessItem(Object item) {
        Map assessment = super.assessItem(item)
        return assessment
    }

    @Override
    Map<String, Object> assessItem(String itemName, Object item) {
        Map assessment = super.assessItem(itemName, item)
        return assessment
    }



    @Override
    String export(){
        configsetMap.each { String name, ConfigSet configSet ->
            log.info "export configset ($name): ${configSet}"
        }
        log.info "exported config sets keys: ${configsetMap.keySet()}"
    }

    @Override
    def export(File exportFolder) {
        configsetMap.each { String name, ConfigSet configSet ->
            log.info "\t\texport configset ($name): ${configSet}"
            configSet.export(exportFolder)
            log.debug "\t\t exported configset $name"
        }
        log.info "exported config sets with  keys: ${configsetMap.keySet()}"
        return exportFolder
    }

    @Override
    def export(FusionClient fusionClient) {
        return null
    }

    @Override
    public String toString() {
        return "ConfigSetCollection: $deploymentName with (${configsetMap.size()}) collection configsets, keys: ${configsetMap.keySet()}"
    }

    /**
     * split the flat=list entries into implicit collection groups, doing some extra parsing in the ConfigSet constructor
     * @param configsetEntries
     * @param keyPattern
     * @return
     */
    protected Integer parseConfigsetCollection(LinkedHashMap<String, Object> configsetEntries, String deploymentName = 'Unnamed', Pattern keyPattern = DEFAULT_KEY_PATTERN) {
        log.info "parseConfigsetCollection with ${configsetEntries.size()} (flat/ungrouped) entries, need to group them by collection..."
        if (deploymentName) {
            log.debug "setting deploymentName: $deploymentName"
            this.deploymentName = deploymentName
        }
        Map groupedEntries = groupConfigsetEntries(configsetEntries, keyPattern)
        groupedEntries.each { String configsetName, Map<String,Object> entries ->
            try {
                def configset = new ConfigSet(configsetName, entries)
                configsetMap[configsetName] = configset
            } catch (Exception e){
                log.warn "Error: $e"
            }
        }

        // is there a better return value? true/false?  void?
        return configsetMap.size()
    }

    Map<String, Object> groupConfigsetEntries(LinkedHashMap<String, Object> configsetEntries, Pattern keyPattern) {
        Map<String, Map<String, Object>> groupedItems = [:].withDefault { [:] }
        configsetEntries.each { String configPath, def val ->
            if (val) {
                Matcher match = (configPath =~ keyPattern)
                if (match.matches()) {
                    String configName = match[0][1]
                    String childPath = match[0][2]
                    groupedItems[configName][childPath] = val
                } else {
                    log.warn "Pattern does not match-- path:$configPath - val:$val"
                }
            } else {
                log.debug "No value for item: $configPath -> $val -- skipping(...?)"
            }
        }
        return groupedItems
    }
}
