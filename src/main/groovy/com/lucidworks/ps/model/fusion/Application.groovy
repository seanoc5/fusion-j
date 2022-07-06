package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import com.lucidworks.ps.model.solr.ConfigSetCollection
import groovy.json.JsonSlurper
import org.apache.log4j.Logger

import java.util.regex.Pattern
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class Application implements BaseObject{
    Logger log = Logger.getLogger(this.class.name);
    public static final List<String> DEFAULT_APP_OBJECTS = "configsets collections dataSources indexPipelines queryPipelines parsers blobs appkitApps features objectGroups links sparkJobs".split(' ')

    def objectsJson = null
    String appName = 'unknown'
    String appID = 'unknown'

    Map metadata
    List appProperties
    Map<String,Object> parsedMap

    List fusionApps
    List<Map> collections
    List<Map> dataSources
    List<Map> indexPipelines
    List<Map> queryPipelines
    List<Map> parsers
    Map<String, Object> blobs = [:]
    List<Map> appkitApps
    Map featuresMap         // https://doc.lucidworks.com/fusion/5.5/333/collection-features-api
    List<Map> objectGroups
    List<Map> links
    List<Map> sparkJobs
//    Map<String, Object> configsetMap = [:]
    ConfigSetCollection configsets
    Map<String, String> queryRewriteJson
    Map<String, Object> queryRewriteRules

    Map<String, Object> unknownItems = [:]  // map of items (from zip file export??) that we do not (yet) explicitly handle...

    /**
     * helper main function to test functionality, change the file arg accordingly...
     * @param args
     */
    static void main(String[] args) {
        File src = new File('/home/sean/work/lucidworks/Intel/CircuitSearch.F5.zip')
        Application app = new Application(src)
//        app.getThingsToCompare()
        app.log.info(app)
    }


    Application(File appOrJson) {
        log.info "Parsing source file: ${appOrJson.absolutePath} (app export, or json...)"
        def parseResult = parseSourceFile(appOrJson)

        // todo -- move all this setup code to a more flexible method
        Map<String, Object> parsedObjects = parsedMap.objects

        this.metadata = parsedMap.metadata
        this.appProperties = parsedObjects['properties']

        if (parsedMap.configsets) {
            configsets = new ConfigSetCollection(((Map) parsedMap.configsets), appName)
            log.info "\t\tGot configsets from parsed source file..."
        }
        collections = parsedObjects.collections
        dataSources = parsedObjects.dataSources
        indexPipelines = parsedObjects.indexPipelines
        queryPipelines = parsedObjects.queryPipelines
        parsers = parsedObjects.parsers
        blobs = parsedObjects.blobs
        appkitApps = parsedObjects.appkitApps
        featuresMap = parseFeaturesMap(parsedObjects.features)
        objectGroups = parsedObjects.objectGroups
        links = parsedObjects.links
        sparkJobs = parsedObjects.sparkJobs

        log.debug "loaded application: $this"
    }

    def export(File destinationFile, Pattern thingsToExport){
        log.info "more code here: export application with things matching pattern: ($thingsToExport)"
    }

    def export(FusionClient destinationClient, Pattern thingsToExport){
        log.info "more code here: export application with things matching pattern: ($thingsToExport)"
    }

    @Override
    def export(File exportFolder) {
        log.info "export Application to folder: ${exportFolder.absolutePath}"

        return null
    }

    @Override
    def export(FusionClient fusionClient) {
        throw new RuntimeException("Export to live fusion client not implemented yet...")
        return null
    }


    public Map<String, Object> parseAppMetadata(Map<String, Object> parsedMap) {
        Map parsedMetadata = [:]
        Map objects
        if (parsedMap.objects) {
            log.info "\t\tGot full source map ${parsedMap.keySet()} "
            objects = parsedMap.objects
            appProperties = parsedMap.properties
            parsedMetadata.appProperties = appProperties
            metadata = parsedMap.metadata
            parsedMetadata.metadata = metadata
        } else {
            log.info "\t\tGot some unxpected source map ${parsedMap.keySet()}, hoping we were called with [objects.json].objects (subset)...? won't have full metadata "
            objects = parsedMap
        }

        if (objects.fusionApps) {
            log.info "We have a fusion app export/zip (assume fusion 4 and above...)"
            fusionApps = objects.fusionApps
            if (fusionApps.size() == 1) {
                appName = fusionApps.name[0]
                appID = fusionApps.id[0]
                parsedMetadata.appName = appName
                parsedMetadata.appID = appID
            } else {
                log.warn "Expected one (1) app in App export!?!?! But we have (${fusionApps.size()} ... Mind blown... how....why... who? Consider everything from here on suspect!!"
            }
        } else {
            log.warn "No fusionApps in export!!?! What is this, year 2016??"
        }
        parsedMetadata
    }


    /**
     * helper to get various fusion things from this fusion app model
     * todo -- move all the untyped things to wrapper objects to implement BaseObject
     * @param String thingType - property of this Application holding the things (e.g.datasources, pipelines, configsets...) to export
     * @return the things exported
     */
    def getThings(String thingType) {
        def things = this.properties[thingType]
        return things
    }

    Map<String, Object> parseSourceFile(File appOrJson) {
        Map parsed = null
        Map<String, Object> configsets = [:]
        if (appOrJson?.exists()) {
            objectsJson = null      //todo - remove?
            if (appOrJson?.exists() && appOrJson.isFile()) {
                if (appOrJson.name.endsWith('.zip')) {
                    loadFromAppExportArchive(appOrJson)
                } else if (appOrJson.name.endsWith('json')) {
                    loadFromObjectsJsonFile(appOrJson)

                } else {
                    log.warn "Unknow file for objects.json contents: $appOrJson (${appOrJson.absolutePath}"
                }
            } else {
                log.warn "File arg ($appOrJson) either does not exist, or is not a (readable) file. Nothing to read from. Cancelling..."
                throw new IllegalArgumentException("No valid source file: $appOrJson")

            }
        } else {
            throw new IllegalArgumentException("No valid source file: $appOrJson")
        }
        if (configsets) {
            log.debug "\t\tadding configsets: ${configsets.keySet()}"
            parsed.configsets = configsets
        }
        log.debug "Parsed Map: $parsed"
        return parsed
    }

    public void loadFromAppExportArchive(File appExportZipFile) {
        log.info "Load app from export ZIP archive: ${appExportZipFile.absolutePath} ..."
        ZipFile zipFile = new ZipFile(appExportZipFile)
        Enumeration<? extends ZipEntry> entries = zipFile.entries()
        Map<String,String> cfgSets = [:]
        JsonSlurper jsonSlurper = new JsonSlurper()
        entries.each { ZipEntry zipEntry ->
            if (zipEntry.name.contains('objects.json')) {
                objectsJson = extractZipEntryText(zipFile, zipEntry)
                parsedMap = jsonSlurper.parseText(objectsJson)
                log.debug "\t\textracted json text from zip entry: ${((Map)parsedMap).keySet()}"
            } else if (zipEntry.name.contains('configsets')) {
                String name = zipEntry.name
                String content = extractZipEntryText(zipFile, zipEntry)
                cfgSets[name] = content
                log.debug "Configset: $zipEntry"

            } else if (zipEntry.name.startsWith('blobs')) {
                String name = zipEntry.name
                String content = extractZipEntryText(zipFile, zipEntry)
                blobs[name] = content
                log.debug "Blob: $zipEntry"

            } else if (zipEntry.name.startsWith('query_rewrite')) {
                String name = zipEntry.name
                String content = extractZipEntryText(zipFile, zipEntry)
                queryRewriteJson[name] = content
                queryRewriteRules[name] = jsonSlurper.parseText(content)
                log.info "Query Rewrite rules count: ${queryRewriteRules.size()} from zip entry: $zipEntry"

            } else {
                String name = zipEntry.name
                String content = extractZipEntryText(zipFile, zipEntry)
                unknownItems[name] = content
                log.info "Storing UNKNOWN zip entry: ${zipEntry} (in application.unknownItems map) -- is this a problem? anything valuable we should be processing?"
            }
            log.debug "ZipEntry: $zipEntry"
        }
        Map parsedMetadata = parseAppMetadata(parsedMap)
        log.info "Parsed app metadata: ${parsedMetadata.keySet()}"

        if (cfgSets) {
            configsets = new ConfigSetCollection(cfgSets, this.appName)
            log.debug "configsets: $configsets"
        }
        log.info "Config sets (${configsets.toString()}) and parsed Map keyset: (${parsedMap.keySet()})"
    }

    public Object loadFromObjectsJsonFile(File appOrJson) {
        String jsonString = appOrJson.text
        log.info "Get json from json file: $appOrJson -- length: ${jsonString.size()} characters"
        parsedMap = new JsonSlurper().parseText(jsonString)
        log.warn "More code here: do more parsing"
        Map parsedMetadata = parseAppMetadata(parsedMap)
        log.debug "Parsed app metadata: $parsedMetadata"


    }

    public String extractZipEntryText(ZipFile zipFile, ZipEntry zipEntry) {
        String jsonString
        InputStream inputStream = zipFile.getInputStream(zipEntry)
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        jsonString = br.text
        jsonString
    }


    @Override
    public String toString() {
        return "Application";
    }

    Map<String, List<Feature>> parseFeaturesMap(Map featuresMap) {
        Map<String, List<Feature>> featuresParsed = featuresMap.collectEntries { String name, List items ->
            ["$name": new Feature(items)]
        }

    }

}


