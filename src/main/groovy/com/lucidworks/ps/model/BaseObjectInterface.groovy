//package com.lucidworks.ps.model

//import com.lucidworks.ps.clients.FusionClient
//import groovy.json.JsonGenerator
//import org.apache.log4j.Logger

// removing interface, and sticking with BaseObject as abstract class

///**
// * Base object to facilitate exporting to various destinations (filesystem, fusion instance,...)
// */
//public interface BaseObjectInterface {
//    static final Logger log = Logger.getLogger(this.class.name);
//    static def jsonDefaultOutput = new JsonGenerator.Options()
//            .dateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
//            .build()
//
//    List<Map> jsonItems = []
//
//    default def export() {
//        // todo add default functionality here...?
//        log.info "export me"
//    }
//
//    def export(File exportFolder)
//
//    def export(FusionClient fusionClient)
//
//    default int size(){
//        return jsonItems.size()
//    }
//}
