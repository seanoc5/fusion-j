package com.lucidworks.ps.clients

import groovy.json.JsonSlurper
import org.apache.log4j.Logger

import java.net.http.HttpRequest
import java.net.http.HttpResponse

class FusionResponseWrapper {
    Logger log = Logger.getLogger(this.class.name);
    HttpRequest origRequest
    HttpResponse response
    String responseText
    Map parsedMap
    List parsedList
    def parsedInfo
    Integer statusCode
    Date timestamp

    FusionResponseWrapper(HttpRequest origRequest, HttpResponse fusionResponse) {
        // todo -- revisit if we process anything other than JSON responses (request/response specify json --text)
        log.debug "\t\tStandard FusionResponseWrapper Constructor (req, resp): req:$origRequest -- resp:$fusionResponse"
        statusCode = fusionResponse.statusCode()
        this.timestamp = new Date()
        this.origRequest = origRequest
        this.response = fusionResponse

        if(wasSuccess()){
            log.debug "Successfull call:$origRequest -- response:$response"
            this.responseText = response.body()
            if(this.responseText) {
                JsonSlurper jsonSlurper = new JsonSlurper()
                def obj = jsonSlurper.parseText(this.responseText)
                try {
                    if(obj instanceof List) {
                        parsedList = obj
                        log.debug "\t\tGot back a list for request: $origRequest"
                    } else if(obj instanceof Map){
                        parsedMap = obj
                        log.debug "\t\tGot back a Map for request: $origRequest"
                    }
                    parsedInfo = obj
                } catch (Exception e){
                    log.warn "Bad groovy-ness?? Map vs List vs 'def' issues?? error: $e"
                }
                log.debug "parsed..."
            } else {
                log.info "No response text??? $fusionResponse"
            }
        } else {
            log.warn "Status: ${response.statusCode()} -- Not a successful request ($origRequest) -> response:($response)??  --body: ${response?.body()}"
        }

    }

    boolean wasSuccess() {
        if(origRequest && response){
            if(response.statusCode() >=200 && response.statusCode()< 300){
                return true
            }
        }
        return false
    }
}
