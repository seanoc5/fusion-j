package com.lucidworks.ps.solr

import groovy.json.JsonSlurper
import groovy.xml.XmlParser

/**
 * Helper class and utils to parse and manipulate solr config information from/to Solr API calls
 *
 * IE: handle api/collections/test/solr-config?recursive=true
 * and the "configThings" that come from that call,
 * then parse flat-text, json, or xml
 *
 * This assumes the thing.value is base64 encoded, and comes from a solr (or Fusion->solr) api call
 * @see https://doc.lucidworks.com/fusion/5.5/323/solr-configuration-api
 */
class SolrConfigHelper {
    String name
    Integer version
    Boolean idDir
    String valueType
    String value
    String decodedValue
    def parsedValue

    SolrConfigHelper(Map<String, Object> thingMap) {
        this.name = thingMap.name
        this.version = thingMap.version
        this.idDir =thingMap.isDir
        if(thingMap.value){
            value = thingMap.value
            decodedValue = dec
        }
    }

    SolrConfigHelper(String name, Integer version, Boolean idDir, String value) {
        this.name = name
        this.version = version
        this.idDir = idDir
        this.value = value
        decodedValue = value.decodeBase64()

    }


    def parsealue(){
        if(name.endsWithIgnoreCase('json')){
            parsedValue = new JsonSlurper().parseText(decodedValue)
        } else if(name.endsWithIgnoreCase('txt')){
            parsedValue = decodedValue
        } else if(name.endsWithIgnoreCase('xml') || name.equalsIgnoreCase('managed-schema')){
            XmlParser parser = new XmlParser()
        }
    }
}
