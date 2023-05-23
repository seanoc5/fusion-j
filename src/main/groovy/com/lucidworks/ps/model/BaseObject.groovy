package com.lucidworks.ps.model

import com.lucidworks.ps.Helper
import com.lucidworks.ps.clients.FusionClient
import groovy.json.JsonGenerator
import groovy.json.JsonOutput
import org.apache.log4j.Logger

/**
 * Base object to facilitate exporting to various destinations (filesystem, fusion instance,...)
 */
public class BaseObject {
    static Logger log = Logger.getLogger(this.class.name);
//    static final Logger log = Logger.getLogger(this.class.name);
    static def jsonDefaultOutput = new JsonGenerator.Options().dateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").build()

    List<Map> srcJsonList = []
    Map srcJsonMap = [:]
    def srcItems
    String itemType = this.getClass().simpleName

    String appName = 'n.a. base'

    /**
     * blank constructor for things that don't map well to json parsed items (list or map)
     */
    BaseObject() {
        log.debug "${this.getClass().simpleName}) BaseObject default constructor..."
    }

    /**
     * handle base object with json parsed source list
     * (this is the most common format: Datasources, Pipelines,...)
     * @param srcJsonList
     */
    BaseObject(List<Map> srcJsonList) {
        log.debug "BaseObject(List jsonItems)..."
        srcItems = srcJsonList
        this.srcJsonList = srcJsonList
    }

    /**
     * handle base object with json parsed source list
     * (this is the most common format: Datasources, Pipelines,...)
     * @param srcJsonList
     */
    BaseObject(String appName, List<Map<String, Object>> srcJsonList) {
        log.debug "$itemType: constructor(String appName, List jsonItems)..."
        this.srcJsonList = srcJsonList
        this.appName = appName
        srcItems = srcJsonList
    }

    /**
     * handle base object constructor with a json parsed source Map
     * (this is somewhat uncommon, things like ... FeatureMap,
     * @param appName
     * @param srcJsonList
     */
    BaseObject(String appName, Map<String, Object> srcJsonMap) {
        log.debug "$itemType: constructor(String appName, MAP jsonmap)... (uncommon to get a map rather than list...)"
        this.srcJsonMap = srcJsonMap
        this.appName = appName
        srcItems = srcJsonMap
    }


    String export(Map rules = [set: [], remove: [/created|lastUpdated/]]) {
        String s = null
        if (srcItems) {
            log.debug "export srcItems to String (override for non-strings and anything else that did not come from a JsonSluper..."
            String json = jsonDefaultOutput.toJson(srcItems)
            String pretty = JsonOutput.prettyPrint(json)
            s = pretty
        } else {
            String msg = "Unknown item time (no srcItems) to export for itemType: $itemType"
            log.error msg
            throw new IllegalArgumentException(msg)
        }
        return s

    }

    def export(File exportFolder) {
        log.debug "\t\tExport item (${itemType})...."
        File outFile
        if (srcJsonList) {
            srcJsonList.each {
                String name = getItemName(it)
                name = Helper.sanitizeFilename(name)
//                if(name.endsWith()
                outFile = new File(exportFolder, name + '.json')
                String json = toJson(it)
                log.debug "$itemType::$name) ${it.size()}"
                outFile.text = json
            }
        } else if (srcJsonMap) {
            srcJsonMap.each { def key, def val ->
                String name = Helper.sanitizeFilename("${key}.json")
                outFile = new File(exportFolder, name)
                String json = toJson(val)
                log.debug "Key ($key) --> val ($val)"
                outFile.text = json
            }
        } else {
            log.warn "Unknown thing type to export: $this (no srcJsonMap or srcJsonList...??)"
        }
        return outFile
    }

    def export(FusionClient fusionClient) {
        throw new IllegalArgumentException("Not implemented yet!!")
    }

/*
    def compare(BaseObject objectToCompare){
        def leftThings = this.srcItems
        def rightThings = objectToCompare.srcItems
        LeftRightCollectionResults collectionResults = new LeftRightCollectionResults(thingType, valueDiffsToIgnore)

        if (leftThings && rightThings) {
            if (leftThings?.class?.name == rightThings?.class?.name) {
                if(leftThings instanceof BaseObject){
                    // todo -- refactor to more elegant handling of BaseObject comparison, for now, just get the underlying JsonObjects to compare...
                    leftThings = ((BaseObject)leftThings).srcItems
                    rightThings = ((BaseObject)rightThings).srcItems
                } else {
                    log.debug "Comparing things of class type: ${leftThings.getClass().simpleName}"
                }

                if (leftThings instanceof List) {
                    compareIds(leftThings, rightThings, collectionResults)
                    this.collectionComparisons[thingType] = collectionResults

                    collectionResults.sharedIds.each { String id ->
                        log.debug "\t\tComparing shared object with id: $id"
                        def leftObject = leftThings.find { it.id == id }
                        def rightObject = rightThings.find { it.id == id }

                        CompareJsonObjectResults objectsResults = compareJsonObjects(thingType, leftObject, rightObject)
                        collectionResults.objectsResults[id] = objectsResults
                        log.debug "Compare results: $objectsResults"
                    }
                } else if (leftThings instanceof BaseObject) {
                    log.warn "Compare BaseObjects: ${leftThings.getClass().simpleName}"

                } else if (leftThings instanceof Map) {
                    log.warn "TODO:: Process map (Left:${leftThings.keySet().size()}) for thing type: $thingType"
                } else {
                    log.warn "No a list? Is this features?..."
                }
            } else {
                String msg = "Left thing type (${leftThings.class.name}) different than right things type (${rightThings.class.name})"
                throw new IllegalArgumentException(msg)
            }
        } else {
            log.warn "${thingType}) leftthing($leftThings) and/or right things($rightThings) not valid..?"
        }

    }
*/

    String toJson(boolean prettyPrint = true) {
        String json = null
        if (srcJsonMap) {
            json = jsonDefaultOutput.toJson(srcJsonMap)
        } else if (srcJsonList) {
            json = jsonDefaultOutput.toJson(srcJsonList)
        } else {
            String msg = "Unknown object type (no srcJsonMap or srcJsonList to convert to JSON--did we miss overriding 'toJson' method in a child/extension class???"
            log.error "$msg"
            throw new IllegalArgumentException(msg)
        }
        if (prettyPrint) {
            log.debug "convert to pretty print..."
            json = JsonOutput.prettyPrint(json)
        }
        return json
    }

    String toJson(def item, boolean prettyPrint = true) {
        String json = jsonDefaultOutput.toJson(item)
        if (prettyPrint) {
            log.debug "convert to pretty print..."
            json = JsonOutput.prettyPrint(json)
        }
        return json
    }


    Map<String, Object> assessComplexity() {
        Integer sumComplexity = Integer.valueOf(0)
        Map complexityAssessment = [assessmentType: this.itemType, size: size(), complexity: sumComplexity, items: []]
        if (srcJsonList) {
            log.debug "Assess complexity (${itemType}) with ${size()} LIST Items..."
            srcJsonList.each {
                def assessment = assessItem(it)
                sumComplexity += assessment.complexity
                complexityAssessment.items << assessment
            }
        } else if (srcJsonMap) {
//            log.warn "\t\t(${this.itemType}) Map processing... not implemented yet...?"
            log.debug "Assess complexity (${itemType}) with ${size()} MAP entries..."
            srcJsonMap.each { def key, def val ->
                log.debug "\t\tKey ($key) --> val ($val)"
                def assessment = assessItem(key, val)
                if (assessment.complexity) {
                    sumComplexity += assessment.complexity
                } else {
                    log.debug "No complexity for this map entry: key:$key -> val:$val"
                }
                complexityAssessment.items << assessment
            }
        } else if (this.itemType.startsWith('ConfigSet')) {
            log.debug "Base object assessComplexity() called, but all assessment should be done in child class method... (ignore me) "
        } else {

            log.warn "Unknown thing type ${this.itemType}) to export: $this (no srcJsonMap or srcJsonList...??)"
        }
        complexityAssessment.complexity = sumComplexity
        return complexityAssessment
    }


    /**
     * Base asessment, close to empty, expect child class to have the assessment logic...
     * @param item thing to assess
     * @return map with assessment information
     */
    Map<String, Object> assessItem(def item) {
        String name = getItemName(item)
        log.debug "\t\t\tBaseObject assessItem: $name -- type: ${item.getClass().simpleName}"
        Map itemAssessment = [name: name, size: size(), complexity: 0, items: []]
    }

    /**
     * Base asessment with item name (processing an element in a map perhaps?_
     * close to empty, expect child class to have the assessment logic...
     * @param item thing to assess
     * @return map with assessment information
     */
    Map<String, Object> assessItem(String itemName, def item) {
        Map itemAssessment = [name: itemName, size: size(), complexity: 0, items: []]
    }

    /** convenience method to get the count of 'items' we are dealing with, part of complexity assessment */
    int size() {
        int size = srcJsonList.size()
        return size
    }


/*
    String getItemName(){
        String name = 'n.a.'
        if(srcJsonMap){
            name = getItemName(srcJsonMap)
        } else if(srcJsonList) {
            name = getItemName(srcJsonList)
        } else {
            throw new IllegalArgumentException("Unknown item type to get name!!")
        }
        return name
    }
*/

    /**
     * convenience method to get id or name, or some other value from the 'item' we are assessing
     * @param item
     * @return appropriate name/id for this item
     */
    String getItemName(Map item) {
        String name = 'n.a.'
        if (item.name) {
            name = item.name
        } else if (item.id) {
            name = item.id
        } else {
            log.warn "$itemType) Could not find a suitable name source (no 'name', or 'id' in map: ${item.keySet()}"
        }
        return name
    }

//    String getItemName(List item){
//        log.warn "Unknown how to get name from a list..."
//        String name = item.toString().md5()
//        return name
//    }
}
