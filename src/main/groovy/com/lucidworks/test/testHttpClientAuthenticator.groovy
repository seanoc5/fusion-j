package com.lucidworks.test

import groovy.json.JsonSlurper
import groovy.transform.Field
import org.apache.log4j.Logger

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

@Field
Logger log = Logger.getLogger(this.class.name)

log.info "Starting script: ${this.class.name}"

String host = 'http://newmac'
int port = 8764
String user = 'sean'
String pass = 'pass1234'
String app = 'lucy'
String qryp = 'lucy'
String coll = app
HttpClient httpClient = getClient("${host}:${port}", user, pass)


// simple test query using previous session cookie (i.e. staying logged in....)
String qryBase = "${host}${port ? ":" + port : ''}/api/apps/${app}/query-pipelines/$qryp/collections/$coll/select"
String q = 'spark'
String qryUrl = qryBase + "?q=${q}"
log.info "Try query with authenticated client on url: $qryUrl"

request = HttpRequest.newBuilder()
        .GET()
        .uri(URI.create(qryUrl))
        .build()

HttpResponse<String> queryResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
JsonSlurper jsonSlurper = new JsonSlurper()
def json = jsonSlurper.parseText(queryResponse.body())
List<Map> docs = json.response.docs
int numFound = json.response.numFound

log.info "Query Response: $queryResponse -- numFound:$numFound -- doc count:${docs.size()}"

log.info " done...?"

// ----------------------- Functions ---------------------------
/**
 * Get a JDK HttpClient with some defaults set for convenience
 * @param baseUrl
 * @param user
 * @param pass
 * @return
 */
HttpClient getClient(String baseUrl, String user, String pass) {

    String urlSession = "${baseUrl}/api/session"
    log.info "Post to session url: $urlSession"

    HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .cookieHandler(new CookieManager())
            .authenticator(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(user, pass.toCharArray())
                }
            })
            .build()

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(urlSession))
            .build()

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    log.info("Test http client response: " + response.statusCode())
    String sessionCookie = response.headers().firstValue("set-cookie")
    log.info(" Session cookie:  $sessionCookie ")
    return client
}

