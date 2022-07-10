package com.lucidworks.ps.model

import com.lucidworks.ps.clients.FusionClient
import groovy.json.JsonGenerator
import org.apache.log4j.Logger
/**
 * Base object to facilitate exporting to various destinations (filesystem, fusion instance,...)
 */
public class BaseObject {
    final Logger log = Logger.getLogger(this.class.name);
    static def jsonDefaultOutput = new JsonGenerator.Options().dateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").build()

    List<Map> srcJsonList = []
    Map srcJsonMap = [:]
    String itemType = this.getClass().simpleName

    String appName = 'n.a. base'

    BaseObject() {
        log.info "${this.getClass().simpleName}) BaseObject default constructor..."
    }

    BaseObject(List<Map> srcJsonList) {
        log.debug "BaseObject(List jsonItems)..."
        this.srcJsonList = srcJsonList
    }

    BaseObject(String appName, List<Map<String, Object>> srcJsonList) {
        log.debug "$itemType: constructor(String appName, List jsonItems)..."
        this.srcJsonList = srcJsonList
        this.appName = appName
    }


    BaseObject(String appName, Map<String, Object> srcJsonMap) {
        log.debug "$itemType: constructor(String appName, MAP jsonmap)..."
        this.srcJsonMap = srcJsonMap
        this.appName = appName
    }

//    BaseObject(File jsonFile) {
//        this.jsonItems = new JsonSlurper().parse(jsonFile)
//    }


    def export() {
        if(srcJsonList) {
            srcJsonList.each {
                log.info "$it"
            }
        } else if(srcJsonMap){
            srcJsonMap.each { def key, def val ->
                log.info "Key ($key) --> val ($val)"
            }
        } else {
            log.warn "Unknown thing type to export: $this (no srcJsonMap or srcJsonList...??)"
        }
    }

    def export(File exportFolder) {
        if(srcJsonList) {
            srcJsonList.each {
                log.info "$it"
            }
        } else if(srcJsonMap){
            srcJsonMap.each { def key, def val ->
                log.info "Key ($key) --> val ($val)"
            }
        } else {
            log.warn "Unknown thing type to export: $this (no srcJsonMap or srcJsonList...??)"
        }
    }

    def export(FusionClient fusionClient) {
        throw new IllegalArgumentException("Not implemented yet!!")
    }

    Map<String, Object> assessComplexity() {
        Integer sumComplexity = Integer.valueOf(0)
        Map complexityAssessment = [assessmentType: this.itemType, size: size(), complexity: sumComplexity, items: []]
        if(srcJsonList) {
            log.info "Assess complexity (${itemType}) with ${size()} LIST Items..."
            srcJsonList.each {
                def assessment = assessItem(it)
                sumComplexity += assessment.complexity
                complexityAssessment.items << assessment
            }
        } else if(srcJsonMap){
//            log.warn "\t\t(${this.itemType}) Map processing... not implemented yet...?"
            log.info "Assess complexity (${itemType}) with ${size()} MAP entries..."
            srcJsonMap.each { def key, def val ->
                log.debug "\t\tKey ($key) --> val ($val)"
                def assessment = assessItem(key, val)
                if(assessment.complexity) {
                    sumComplexity += assessment.complexity
                } else {
                    log.debug "No complexity for this map entry: key:$key -> val:$val"
                }
                complexityAssessment.items << assessment
            }
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
        Map itemAssessment = [name: name, size: size(), complexity: 0]
    }

    /**
     * Base asessment with item name (processing an element in a map perhaps?_
     * close to empty, expect child class to have the assessment logic...
     * @param item thing to assess
     * @return map with assessment information
     */
    Map<String, Object> assessItem(String itemName, def item) {
        Map itemAssessment = [name:itemName, size: size(), complexity: 0]
    }

    /** convenience method to get the count of 'items' we are dealing with, part of complexity assessment */
    int size() {
        int size = srcJsonList.size()
        return size
    }


    /**
     * convenience method to get id or name, or some other value from the 'item' we are assessing
      * @param item
     * @return appropriate name/id for this item
     */
    String getItemName(Object item) {
        String name = 'n.a.'
        if (item instanceof Map) {
            if (item.name) {
                name = item.name
            } else if (item.id) {
                name = item.id
            }
        } else if (item instanceof Collection) {
            log.warn "Collection name...?"
        } else {
            log.warn "get unknown item type name (override getItemName for this item??) ${this.getClass().simpleName}"

        }

        return name
    }
}
