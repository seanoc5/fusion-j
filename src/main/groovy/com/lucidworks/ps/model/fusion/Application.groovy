package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import com.lucidworks.ps.model.solr.ConfigSetCollection
import groovy.json.JsonSlurper
import org.apache.log4j.Logger

import java.util.regex.Pattern
import java.util.zip.ZipEntry
import java.util.zip.ZipException
import java.util.zip.ZipFile

/**
 * Fusion Application helper class
 * Mix of composite objects (@see ConfigSetCollection) and regular lists/maps
 * We may convert to more explicit composite objects as necessary
 */
class Application extends BaseObject {
    static final Logger log = Logger.getLogger(this.class.name);
    public static final List<String> DEFAULT_APP_OBJECTS = "configsets collections dataSources indexPipelines indexProfiles queryPipelines queryProfiles parsers blobs appkitApps features objectGroups links sparkJobs jobs".split(' ')

    /** Object holding source for loading this app (file, fusion-j client, git repo... */
    def source
    String appName = 'unknown'
    String appID = 'unknown'
    String fusionVersion
    Map metadata
    List appProperties

    /** typically we load an objects.json like structure, and one of three top-level structures is called 'objects' which is poo naming, so over describing this source field... */
    Map<String, Object> exportedObjectsSourceMap

    /** this should only ever be a single item, but leaving as a list in case I am wrong... */
    List fusionApps

    Collections collections
    DataSources dataSources
    IndexPipelines indexPipelines
    QueryPipelines queryPipelines
    /** profiles change structure between F3 and higher, leaving as untyped 'def' for the moment */
    IndexProfiles indexProfiles
    def queryProfiles
    Parsers parsers

    /** list of blob objects -- often zipEntries from an app export zip... */
    List blobObjects = []
    Blobs blobs

    AppKits appkitApps
    Features features
    ObjectGroups objectGroups
    Links links
    SparkJobs sparkJobs
    Jobs jobs

    ConfigSetCollection configsets
    Map<String, String> queryRewriteJson = [:]
    Map<String, Object> queryRewriteRules = [:]

    Map<String, Object> unknownItems = [:]
    // map of items (from zip file export??) that we do not (yet) explicitly handle...

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
        parseSourceFile(appOrJson)
//        def parseResult = parseSourceFile(appOrJson)
        if (exportedObjectsSourceMap) {
            // todo -- move all this setup code to a more flexible method
            Map<String, Object> parsedObjects = exportedObjectsSourceMap.objects

            if (parsedObjects.metadata) {
                this.metadata = exportedObjectsSourceMap.metadata
            }
            if (parsedObjects.appProperties) {
                this.appProperties = parsedObjects['properties']
            }

            if (parsedObjects.configsets) {
                log.warn "Found configsets in parsedObjects json...??!? this is unexpected, but we will try to process anyhow...."
                configsets = new ConfigSetCollection((Map) exportedObjectsSourceMap.configsets, appName)
            } else {
                log.debug "\t\tNo configsets in parsedObjects source (this is expected)"
            }
            if (parsedObjects.collections) {
                collections = new Collections(appName, (List) parsedObjects.collections)
            }
            if (parsedObjects.dataSources) {
                dataSources = new DataSources(appName, parsedObjects.dataSources)
            }
            if (parsedObjects.indexPipelines) {
                indexPipelines = new IndexPipelines(appName, parsedObjects.indexPipelines)
            }
            if (parsedObjects.indexProfiles) {
                indexProfiles = new IndexProfiles(appName, parsedObjects.indexProfiles)
            }
            if (parsedObjects.queryPipelines) {
                queryPipelines = new QueryPipelines(appName, parsedObjects.queryPipelines)
            }
            if (parsedObjects.queryProfiles) {
                queryProfiles = new QueryProfiles(appName, parsedObjects.queryProfiles)
            }
            if (parsedObjects.parsers) {
                parsers = new Parsers(appName, parsedObjects.parsers)
            }
            if (parsedObjects.blobs) {
                blobs = new Blobs(appName, parsedObjects.blobs)
            } else {
                log.info "\t\tno blobs available (subset of appexport or pre F4...?)"
            }
            if (parsedObjects.appkitApps) {
                appkitApps = new AppKits(appName, parsedObjects.appkitApps)
            } else {
                log.info "\t\tno Appkit apps found; this is fine--unless you have appkit apps...?"
            }
            if (parsedObjects.features) {
                features = new Features(appName, parsedObjects.features)
            }

            if (parsedObjects.objectGroups) {
                objectGroups = new ObjectGroups(appName, parsedObjects.objectGroups)
            }
            if (parsedObjects.links) {
                links = new Links(appName, parsedObjects.links)
            }
            if (parsedObjects.jobs) {
                jobs = new Jobs(appName, parsedObjects.jobs)
            }
            if (parsedObjects.sparkJobs) {
                sparkJobs = new SparkJobs(appName, parsedObjects.sparkJobs)
            }

            log.info "loaded application: $this"
        } else {
            log.warn "Could not parse app! Source: $source (no exportedObjectsSourceMap...?)"
        }
    }

    /**
     * get objects.json info from exported app (F4+)
     * shortcut if we don't want to load the entire zip file (configsets, rules,...
     * @param sourceZip
     */
    static Map<String, Object> getObjectsJsonMap(File sourceZip) {
        ZipFile zipFile = new ZipFile(sourceZip)
        Enumeration<? extends ZipEntry> entries = zipFile.entries()
        ZipEntry objectsJsonZipEntry = entries.find { it.name.equalsIgnoreCase('objects.json') }
        String objectsJson = extractZipEntryText(zipFile, objectsJsonZipEntry)
        JsonSlurper jsonSlurper = new JsonSlurper()
        Map<String, Object> objectsMap = jsonSlurper.parseText(objectsJson)
        log.info "Got Map from objects json in zip: $sourceZip"
        return objectsMap
    }

    def export(File destinationFile, Pattern thingsToExport) {
        log.warn "more code here: export application with things matching pattern: ($thingsToExport)"
    }

    def export(FusionClient destinationClient, Pattern thingsToExport) {
        log.warn "more code here: export application with things matching pattern: ($thingsToExport)"
    }

    @Override
    def export(File exportFolder) {
        log.warn "export Application to folder: ${exportFolder.absolutePath}"
        return null
    }

    @Override
    def export(FusionClient fusionClient) {
        throw new RuntimeException("Export to live fusion client not implemented yet...")
        return null
    }

    def transform(def thingsToTransform, def transformRules){
        Map<String, Object> resultsMap = [:]
        thingsToTransform.each { thingType ->
            String msg = "Transform fusion app thing: $thingType"
            log.info msg
            def fusionObjectWrapper = this.getThings(thingType)
            def results = fusionObjectWrapper.tr
            // todo -- add actual code and real results here, WIP
            resultsMap[thingType] = msg
        }
        return resultsMap
    }

    public Map<String, Object> parseAppMetadata(Map<String, Object> parsedMap) {
        Map parsedMetadata = [:]
        Map objects
        if (parsedMap?.objects) {
            log.info "\t\tGot full source map ${parsedMap.keySet()} "
            objects = parsedMap.objects
            appProperties = parsedMap.properties
            parsedMetadata.appProperties = appProperties
            metadata = parsedMap.metadata
            Integer majorVersion = null
            fusionVersion = metadata.fusionVersion
            List verParts = fusionVersion.split('\\.')
            if (verParts) {
                majorVersion = java.lang.Integer.parseInt(verParts[0])
                if (!majorVersion || majorVersion < 4) {
                    if (source instanceof File) {
                        appName = ((File) source).name
                        log.info "\t\tNo fusion apps in this version ($fusionVersion), setting name to source fileName: $appName"
                    } else {
                        appName = fusionVersion
                        log.info "\t\tNo fusion apps in this version ($fusionVersion), setting name to version: $appName"
                    }
                }
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
            } else if (majorVersion < 4) {
                log.debug "Major version ($majorVersion) suggests version before Fusion 4, and thus no app info to parse"
            } else {
                log.debug "No fusionApps in export!!?! Major version ($majorVersion) seems >= 4, which SHOULD have app info..."
            }

            parsedMetadata.metadata = metadata
        } else {
            if (parsedMap) {
                log.warn "\t\tGot some unxpected source map ${parsedMap.keySet()}, hoping we were called with [objects.json].objects (subset)...? won't have full metadata "
            } else {
                log.warn "\t\tGot some unxpected source map NO PARSED MAP, nothing to do for ${this.source}"
            }
            objects = parsedMap
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

//    Map<String, Object> parseSourceFile(File appOrJson) {
    void parseSourceFile(File appOrJson) {
//        Map parsed = null
        Map<String, Object> configsets = [:]
        if (appOrJson?.exists()) {
//            objectsJson = null      //todo - remove?
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
//            parsed.configsets = configsets
        }
//        log.debug "Parsed Map: $parsed"
//        return parsed
    }

    public void loadFromAppExportArchive(File appExportZipFile) {
        log.info "\t\tLoad app from export ZIP archive: ${appExportZipFile.absolutePath} ..."
        source = appExportZipFile
        try {
            ZipFile zipFile = new ZipFile(appExportZipFile)
            Enumeration<? extends ZipEntry> entries = zipFile.entries()
            Map<String, String> cfgSets = [:]
            int LOG_BATCH_SIZE = 500
            int counter = 0
            JsonSlurper jsonSlurper = new JsonSlurper()
            entries.each { ZipEntry zipEntry ->
                counter++
                if (counter % LOG_BATCH_SIZE == 0) {
                    log.info "\t\t$counter) progress update... working through zipEntry: $zipEntry"
                }

                if (zipEntry.name.contains('objects.json')) {
                    String objectsJson = extractZipEntryText(zipFile, zipEntry)
                    exportedObjectsSourceMap = jsonSlurper.parseText(objectsJson)
                    log.debug "\t\textracted json text from zip entry: ${((Map) exportedObjectsSourceMap).keySet()}"
                } else if (zipEntry.name.contains('configsets')) {
                    String name = zipEntry.name
                    String content = extractZipEntryText(zipFile, zipEntry)
                    cfgSets[name] = content
                    log.debug "Configset: $zipEntry"

                } else if (zipEntry.name.startsWith('blobs')) {
                    String name = zipEntry.name
                    // todo -- write code to get/store blob object intelligently
                    log.debug "\t\t$name) zipEntry "
//                String content = extractZipEntryText(zipFile, zipEntry)
                    blobObjects << zipEntry
                    log.debug "\t\tBlob object: $zipEntry"

                } else if (zipEntry.name.startsWith('query_rewrite')) {
                    String name = zipEntry.name
                    String content = extractZipEntryText(zipFile, zipEntry)
                    queryRewriteJson[name] = content
                    queryRewriteRules[name] = jsonSlurper.parseText(content)
                    log.info "\t\tQuery Rewrite rules count: ${queryRewriteRules.size()} from zip entry: $zipEntry"

                } else {
                    String name = zipEntry.name
                    if (name.endsWith('/')) {
                        log.info "Skip zip file folder: $name"
                    } else {
                        // todo -- add logic to handle non-text blobs...
                        String content = extractZipEntryText(zipFile, zipEntry)
                        unknownItems[name] = content
                        log.warn "Storing UNKNOWN zip entry: ${zipEntry} (in application.unknownItems map) -- is this a problem? anything valuable we should be processing?"
                    }
                }
                log.debug "ZipEntry: $zipEntry"
            }
            Map parsedMetadata = parseAppMetadata(exportedObjectsSourceMap)
            log.debug "\t\tParsed app metadata: ${parsedMetadata.keySet()}"

            if (cfgSets) {
                configsets = new ConfigSetCollection(cfgSets, this.appName)
                log.debug "configsets: $configsets"
            }
            log.info "\t\tConfig sets (${configsets?.toString()}) and parsed Map keyset: (${exportedObjectsSourceMap?.keySet()})"
        } catch(ZipException ze) {
            log.warn "Zip file error: $ze"
        }
    }

    public Object loadFromObjectsJsonFile(File jsonSourceFile) {
        String jsonString = jsonSourceFile.text
        source = jsonSourceFile
        log.info "\t\tGet json from json file: $jsonSourceFile -- length: ${jsonString.size()} characters"
        exportedObjectsSourceMap = new JsonSlurper().parseText(jsonString)
        Map parsedMetadata = parseAppMetadata(exportedObjectsSourceMap)
        log.debug "Parsed app metadata: $parsedMetadata"
        return parsedMetadata
    }

    /**
     * limited function to read text content from zip entries (blobs come to mind as a problem?)
     * @param zipFile
     * @param zipEntry
     * @return
     */
    static public String extractZipEntryText(ZipFile zipFile, ZipEntry zipEntry) {
        String jsonString
        InputStream inputStream = zipFile.getInputStream(zipEntry)
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        jsonString = br.text
        jsonString
    }


    @Override
    public String toString() {
        if (!appName) {
            log.warn "No appName present??"
        }
        Map<String, String> infoMap = toInfoMap()
        String s = infoMap.toString()
        return s
    }

    public Map<String, String> toInfoMap() {
        Map<String, String> infoMap = [
                name          : appName,
                source        : source,
                collections   : collections?.size(),
                dataSources   : dataSources?.size(),
                indexPipelines: indexPipelines?.size(),
                queryPipelines: queryPipelines?.size(),
                parsers       : parsers?.size(),
        ]
        infoMap
    }

}


