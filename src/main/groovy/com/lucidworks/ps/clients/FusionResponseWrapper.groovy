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
    Collection parsedInfo
    Integer statusCode
    Date timestamp

    FusionResponseWrapper(HttpRequest origRequest, HttpResponse fusionResponse) {
        // todo -- revisit if we process anything other than JSON responses (request/response specify json --text)
        log.info "Standard Constructor (req, resp): req:$origRequest -- resp:$fusionResponse"
        statusCode = fusionResponse.statusCode()
        this.timestamp = new Date()
        this.origRequest = origRequest
        this.response = fusionResponse

        this.responseText = response.body()
        if(this.responseText) {
            JsonSlurper jsonSlurper = new JsonSlurper()
            parsedInfo = jsonSlurper.parseText(this.responseText)
        } else {
            log.info "No response text??? $fusionResponse"
        }
    }
}
