package com.lucidworks.ps.model.solr

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import groovy.xml.XmlParser
import org.apache.log4j.Logger

/**
 * @author :    sean
 * @mailto :    seanoc5@gmail.com
 * @created :   6/15/22, Wednesday
 * @description:
 */

/**
 * wrapper class ato provide 'solrconfig.xml' specific processing and structure
 * @NOTE assuming XML format only, JSON format may come in the future...
 */
class SolrConfig implements BaseObject {
    Logger log = Logger.getLogger(this.class.name);
    String sourceContent
    def sourceURI           // optional? useful??
    Node xml
    String luceneMatchVersion

    SolrConfig(String s) {
        this.sourceContent = s
        xml = parseXml(s)
        def ver = xml.luceneMatchVersion
        if(ver && ver instanceof List){
            this.luceneMatchVersion = ver[0]
        }
        log.debug "String based constructor (ASSUME XML format)..."
    }

    SolrConfig(URI uri) {
        log.debug "File (${uri}) based constructor (ASSUME XML format)..."
        String content = uri.get
        this.sourceContent = uri
        xml = parseXml(uri)
    }
//    SolrConfig(Map items){
//        def sc = items['/solrconfig.xml']
//    }


    Node parseXml(String s) {
        XmlParser xmlParser = new XmlParser()
        this.xml = xmlParser.parseText(s)
    }

    Node parseXml(File f) {
        XmlParser xmlParser = new XmlParser()
        this.xml = xmlParser.parse(f)
    }

    Node parseXml(URI uri) {
        XmlParser xmlParser = new XmlParser()
        this.xml = xmlParser.parse(uri)
    }

    @Override
    def export(File exportFolder) {
        log.info "export solrconfig to folder: ${exportFolder.absolutePath}"
        return null
    }

    @Override
    def export(FusionClient fusionClient) {
        throw new RuntimeException("Export to live fusion client not implemented yet...")
        return null
    }
}
