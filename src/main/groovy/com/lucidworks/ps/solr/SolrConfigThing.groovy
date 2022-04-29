package com.lucidworks.ps.solr

import groovy.json.JsonSlurper
import groovy.xml.XmlParser
import org.apache.log4j.Logger

/**
 * Helper class and utils to parse and manipulate solr config information from/to Solr API calls
 *
 * IE: handle api/collections/test/solr-config?recursive=true
 * and the "configThings" that come from that call,
 * then parse flat-text, json, or xml
 *
 * This assumes the thing.value is base64 encoded, and comes from a solr (or Fusion->solr) api call
 * @link https://doc.lucidworks.com/fusion/5.5/323/solr-configuration-api
 */
class SolrConfigThing {
    Logger log = Logger.getLogger(this.class.name);
    String name
    Integer version
    Boolean idDir
    String valueType
    String value
    String decodedValue
    def parsedValue
    public static final String JSON = 'json'
    public static final String XML = 'xml'
    public static final String TEXT = 'text'
    public static final String DIR = 'directory'
    public static final String UNKNOWN = 'unknown'


    SolrConfigThing(Map<String, Object> thingMap) {
        this.name = thingMap.name
        this.version = thingMap.version
        this.idDir =thingMap.isDir
        if(thingMap.value){
            value = thingMap.value
            byte[] bytes = value.decodeBase64()
            decodedValue = new String(bytes)
            parseValue()
            log.debug "\t\t solr config thing value(s): $decodedValue -> $parsedValue"

        } else {
            log.debug "No value entry to parse..."
        }
    }

    SolrConfigThing(String name, Integer version, Boolean idDir, String value) {
        this.name = name
        this.version = version
        this.idDir = idDir
        this.value = value
        decodedValue = value.decodeBase64()
        parsedValue = parseValue()

    }


    def parseValue(){
        if(name.endsWithIgnoreCase('json')){
            valueType = JSON
            parsedValue = new JsonSlurper().parseText(decodedValue)
        } else if(name.endsWithIgnoreCase('txt')){
            valueType = TEXT
            parsedValue = decodedValue
        } else if(name.endsWithIgnoreCase('xml') || name.equalsIgnoreCase('managed-schema') || decodedValue.startsWith('<?')){
            valueType = XML
            XmlParser parser = new XmlParser()
            parsedValue = parser.parseText(decodedValue)
            log.info "\t\t$name parseValue() with xml format? Name: $name..."
        } else {
            valueType = UNKNOWN
            parsedValue = decodedValue
            log.warn "\t\t$name:$valueType) parsing unknown format"
        }
    }
}
