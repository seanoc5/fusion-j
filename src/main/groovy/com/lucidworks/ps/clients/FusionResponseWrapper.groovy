package com.lucidworks.ps.clients

import com.lucidworks.ps.Helper
import groovy.json.JsonSlurper
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.groovy.json.internal.LazyMap
import org.apache.log4j.Logger

import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandler
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
    HttpRequest httpRequest
    /** standard response object, strong assumption this will be handled by a string body parser */
    HttpResponse httpResponse
    /** convenience property for the actual string response -- todo revisit if we need anything other than string response */
    String responseText
    /** Java Map version of the parsed object (as opposed to list version--should be either/or)  */
    Map parsedMap
    /** Java List version of the parsed object (as opposed to Map version--should be either/or)  */
    List parsedList
    /**
     * untyped parsed response -- assume either list or map
     * */
    def parsedInfo
    /** convenience prop for the response.statusCode */
    Integer statusCode
    /** convenience access to result/message from call (useful for error summary) */
    String statusMessage
    /** helper property to track timeline (roughly) -- time of this wrapper's creation, might potentially deviate from actual request time */
    Date timestamp
    BodyHandler bodyHandler

    /**
     * Typical constructor -- usually done after request has been sent and response has been received
     * This is an optional object intended for saving/reviewing what has taken place during an fusion management (e.g. upgrade/migration) process
     * @param httpRequest* @param httpResponse
     */
    FusionResponseWrapper(HttpRequest httpRequest, HttpResponse httpResponse, BodyHandler handler) {
        // todo -- revisit if we process anything other than JSON responses (request/response specify json --text)
        log.debug "\t\tStandard FusionResponseWrapper Constructor (req, resp): req:$httpRequest -- resp:$httpResponse"
        statusCode = httpResponse.statusCode()
        this.timestamp = new Date()
        this.httpRequest = httpRequest
        this.httpResponse = httpResponse
        bodyHandler = handler

        if (wasSuccess()) {
            log.debug "Successfull call:$httpRequest -- response:$this.httpResponse "
            def body = this.httpResponse.body()
            if (body) {
                String bodyType = body.getClass().name
                log.debug "Body class: ${bodyType}"

                if (body instanceof String) {
                    this.responseText = this.httpResponse.body()
                    this.parsedInfo = this.httpResponse.body()
                    if (isJson(body)) {
                        log.debug "Response body starts with what looks like json '{' so parse as json (not xml)"
                        def obj = new JsonSlurper().parseText(body)

                        try {
                            if (obj instanceof List) {
                                parsedList = obj
                                log.debug "\t\tGot back a list for request: $httpRequest"
                            } else if (obj instanceof Map) {
                                parsedMap = obj
                                log.debug "\t\tGot back a Map for request: $httpRequest"
                            }
                            parsedInfo = obj
                        } catch (Exception e) {
                            log.warn "Bad groovy-ness?? Map vs List vs 'def' issues?? error: $e"
                        }

                    } else if (responseText.startsWith('<')) {
                        log.warn "Looks like an xml response?!? MORE CODE here"
                    }
                } else if (bodyType.containsIgnoreCase('file')) {
                    log.warn "Processing FILE response.... MORE CODE HERE"

                } else if (body instanceof InputStream) {
//                } else if (bodyType.containsIgnoreCase('stream')) {
                    // todo add code for OTHER streams, or files that are not zipfiles
                    log.info "Processing stream response.... MORE CODE HERE (i.e. non-zipfile requests, \"what else floats?\" -- small pebbles? what other calls, NLP models seem to be zips, as are jars...)"
                    ZipFile zipFileMemory = Helper.processStream((InputStream)body)         // todo -- is there a better approach for determining/abstracting stream...???
                    parsedInfo = zipFileMemory
                }

//            } else if() {

            } else {
                String uri = httpRequest.uri().toString()
                if (uri.containsIgnoreCase('api/session')) {
                    log.debug "No response text for session"

                } else {
                    log.warn "\t\tNo body ($body) returned!!!?!"
                }
            }
        } else {
            String msg = httpResponse.body()

            this.statusMessage = httpResponse.body()
            log.warn("Request failed??) Status: ${this.httpResponse.statusCode()} -- Not a successful request ($httpRequest) -> response:($this.httpResponse)??  --body: ${this.httpResponse?.body()}")
        }
    }

/**
 * helper wrapper method to process sucess/failure
 * @return
 */
    boolean wasSuccess() {
        if (httpRequest && httpResponse) {
            if (httpResponse.statusCode() >= 200 && httpResponse.statusCode() < 300) {
                return true
            }
        }
        return false
    }

/**
 * create short descriptive text for req/resp etc
 * @return
 */
    String toString() {
        String verb = httpRequest.method()
        String infoType = parsedInfo instanceof LazyMap ? "LazyMap" : parsedInfo?.class?.name
        String preview = null
        if (parsedList) {
            int previewItemCount = Math.min(2, parsedList.size())
            StringBuilder sb = new StringBuilder()
            def snippets = parsedList[0..previewItemCount].collect {
                String s = it.toString()
                s[0..Math.min(15, s.size())]
            }
            preview = "List(${parsedList.size()}): ${snippets}"
        } else if (parsedMap) {
            preview = "Map.keys: ${parsedMap.keySet()}"
        } else {
            if (responseText?.size()) {
                preview = responseText[0..Math.min(50, responseText.size())]
            } else {
                preview = 'n.a.'
            }
        }
        String s = "$statusCode ${verb.padRight(5)} (${getRequestType()}): ${httpRequest.uri().toString()} -- response snippets: $preview"
        //-- Response: $preview"
    }

    Boolean shouldExportRequest() {
        boolean export = false
        if (httpRequest?.method() == 'POST' || httpRequest?.method() == 'PUT') {
            if (httpRequest.uri().toString().containsIgnoreCase('api/session')) {
                log.debug "do not export session calls (POST): $httpRequest..."
            } else {
                log.info "Export $httpRequest"
                export = true
            }
        } else {
            log.debug "do not export request (not POST or PUT): $httpRequest"
        }
        return export
    }

    String getRequestType() {
        String url = httpRequest?.uri()?.toString()
        String type = null
        switch (url) {
            case ~/.*api.session.*/:
                type = 'session'
                break

            case url.containsIgnoreCase('api/session'):
                type = 'session'
                break

            case { ((String) it).endsWith('api/apps') }:
                type = 'apps'
                break

            case ~/jobs.*schedule/:
                type = 'schedules'
                break

            case ~/link/:
                type = 'link'
                break

            case ~/api\/apps.*collections/:
                type = 'collections'
                break

            case ~/parsers/:
                type = 'parsers'
                break

            case ~/index-pipelines/:
                type = 'index-pipelines'
                break

            case ~/query-pipelines/:
                type = 'query-pipelines'
                break

            case ~/connectors.plugins/:
                type = 'connector-plugins-repositry'
                break

            case ~/connectors.datasources/:
                type = 'datasources'
                break

            case ~/links/:
                type = 'links'
                break

//            case ~//:
//                group = ''
//                break

            default:
                type = 'unknown'
                break

        }
        return type
    }

    /**
     * quick hack to see if response is JSON or XML, or other (stream, file,...)
     * @return
     */
    boolean isJson(String body) {
        String btrim = body?.trim()
        boolean isjson = false
        if (btrim) {
            if (btrim.startsWith('{') || btrim.startsWith('[')) {
                log.debug "Trimmed body started with '{' or '[', so we assume this is JSON"
                isjson = true
            } else {
                log.debug "Trimmed body DID NOT start with '{' or '[', so we assume this is NOT VALID json"
            }
        } else {
            log.info "Trimmed body was blank or null, should catch this before call, returning false (i.e. not JSON)"
        }
        return isjson
    }
}
