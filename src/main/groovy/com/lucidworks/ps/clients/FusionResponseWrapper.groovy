package com.lucidworks.ps.clients

import groovy.json.JsonSlurper
import org.apache.log4j.Logger

import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * Helper class to provide some basic handling of requests and responses.
 * These objects may be collected/stored in the calling code for a history of actions performed (and any errors) during an upgrade/migration
 * Todo: this assumes most responses will have a string body handler; may need to revisit this assumption
 * <p>
 *     parsedInfo is the untype-cast parsed object. We assume either a list or a collection, the wrapper will assign one of those type case properies accordingly
 */
class FusionResponseWrapper {
    Logger log = Logger.getLogger(this.class.name);
    /** original request sent to fusion */
    HttpRequest origRequest
    /** standard response object, strong assumption this will be handled by a string body parser */
    HttpResponse response
    /** convenience property for the actual string response -- todo revisit if we need anything other than string response */
    String responseText
    /** Java Map version of the parsed object (as opposed to list version--should be either/or)  */
    Map parsedMap
    /** Java List version of the parsed object (as opposed to Map version--should be either/or)  */
    List parsedList
    /** untyped parsed response -- assume either list or map */
    def parsedInfo
    /** convenience prop for the response.statusCode */
    Integer statusCode
     /** helper property to track timeline (roughly) -- time of this wrapper's creation, might potentially deviate from actual request time */
    Date timestamp

    /**
     * Typical constructor -- usually done after request has been sent and response has been received
     * This is an optional object intended for saving/reviewing what has taken place during an fusion management (e.g. upgrade/migration) process
     * @param origRequest
     * @param fusionResponse
     */
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
