package com.lucidworks.ps.clients

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * attempt to replicate solrj client-ness for fusion
 * using cookies and session api to start with
 * sample rest calls: http://localhost:8764/api
 * POST http://localhost:8764/api/session
 * GET  http://localhost:8764/api/apps/${app}/query-pipelines/$qryp/collections/$coll/select?q=spark
 * POST http://localhost:8764/api/index-pipelines/$idxp/collections/$coll/index?commit=true
 *
 * todo beef up wt support, assuming json for everything at the moment...
 *
 */
class FusionClient {
    protected static Logger log = Logger.getLogger(this.class.name);
    String host
    Integer port
    String user
    String password
    String application
    String collection

    /** e.g. http://localhost:8764/api */
    String fusionBase
    HttpClient httpFusionClient
    private String sessionCookie
    Map introspectInfo
    Map apiInfo
    Long cookieMS
    long MAX_COOKIE_AGE_MS = 1000 * 60 * 15
    // starting with default of 15 minutes for cookie age?      // todo -- revisit, make this more accessible

    FusionClient(String baseUrl, Integer port, String user, String pass, String app, String coll) {
//        this.host = host
        this.fusionBase = baseUrl
        this.port = port
        this.user = user
        this.password = pass
        this.application = app
        this.collection = coll

        if (baseUrl.endsWith('/')) {
            log.info "\tstripping trailing slash from host: '$host' to build base with a leading slash..."
            host = host[0..-2]
        }
        if (port) {
            fusionBase = "${baseUrl}:${port}/api"
            log.info "\tPort: $port:: fusion base: $fusionBase"
        } else {
            fusionBase = "${baseUrl}/api"
            log.info "\tNo Port:: fusion base: $fusionBase"
        }
        log.debug "constructor: $baseUrl, $port, $user, $pass, $app, $coll, calling buildClient..."
        httpFusionClient = buildClient(fusionBase, user, password)
//        introspectInfo = getFusionInformation()
    }

    /**
     * map based constructor
     * todo -- find good-practice for how to do this with mandatory and optional params, sanity checks, etc
     * @param params
     */
    FusionClient(Map<String, Object> params){
        this.host = params.host
        this.port = params.port
        this.user = params.user
        this.password =params.password ?: params.pass
        this.application = params.app ?: params.application
        this.collection = params.coll ?: params.collection
        fusionBase = "${host}${port ? ":" + port : ''}/api"

        httpFusionClient = buildClient(fusionBase, user, password)
        log.info "Map based constructor got base urL:'$fusionBase' from map: $params"
    }

    public HttpClient buildClient() {
        if (isValidFusionClient()) {
            return httpFusionClient
        } else {
            log.info "\tFusion client not valid, trying to get a valid version now (first call, or timeout...?)..."
            log.debug "\t${this.class.name} getClient() using contructor set vars(baseurl: $fusionBase, user:$user, password <hidden>...)   [should only need to call this once...]"
            httpFusionClient = buildClient(fusionBase, user, password)
            return httpFusionClient
        }
    }


/**
 * Get a JDK HttpClient with some defaults set for convenience
 * @param baseUrl
 * @param user
 * @param pass
 * @return
 */
    public HttpClient buildClient(String baseUrl, String user, String pass) {
        log.info "\t${this.class.simpleName} getClient(baseurl: $baseUrl, user:$user, password <hidden>...)   [should only need to call this once...]"
        HttpClient client = HttpClient.newBuilder()
                .cookieHandler(new CookieManager())
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        // groovy string template for speed, rather than json builder
        String authJson = """{"username":"$user", "password":"$pass"}"""

        String urlSession = "${baseUrl}/session"
        log.info "\tInitializing Fusion client session via POST to session url: $urlSession"

        try {
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(urlSession))
                    .timeout(Duration.ofMinutes(2))
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(authJson))        // redundant with client.send below? no?? this adds the payload, BodyHandlers.ofString below builds the request string?
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            log.debug("\t\tResponse status: " + response.statusCode());
            sessionCookie = response.headers().firstValue("set-cookie")
            cookieMS = System.currentTimeMillis()
            Date ts = new Date(cookieMS)
            log.info("\tSession cookie: ${this.sessionCookie} set/refreshed at timestamp: $cookieMS (${ts})");

        } catch (Exception e) {
            log.warn "Problem getting client: $e"
            client = null
        }

        return client
    }


    /**
     * trying to get general information, primarily fusion version, so we can adjust api syntax accordingly
     *
     * @return
     */
    def getFusionInformation() {
        var reqApi = HttpRequest.newBuilder()
                .uri(URI.create("${this.fusionBase}"))
                .GET()
                .timeout(Duration.ofMinutes(2))
                .setHeader("User-Agent", "Java 11+ HttpClient FusionPS Bot") // add request header
//                .header("Content-Type", "application/json")
                .build();

        JsonSlurper jsonSlurper = new JsonSlurper()

        HttpResponse<String> response = httpFusionClient.send(reqApi, HttpResponse.BodyHandlers.ofString());
        int rc = response.statusCode()
        if (rc >= 200 && rc < 300) {
            log.debug("Response from getFusionInformation() ${response.statusCode()}");
            apiInfo = jsonSlurper.parseText(response.body())
            log.debug "Fusion API info: $apiInfo"
        } else {
            log.warn "Could not get valid API information: ${this.fusionBase}"
        }

        URI uri = (URI.create("${this.fusionBase}/introspect"))
        var reqIntro = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .timeout(Duration.ofMinutes(2))
                .setHeader("User-Agent", "Java 11+ HttpClient FusionPS Bot") // add request header
//                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> respIntro = httpFusionClient.send(reqApi, HttpResponse.BodyHandlers.ofString());
        rc = respIntro.statusCode()
        if (rc >= 200 && rc < 300) {

            log.debug("Response introspection result code: ${respIntro.statusCode()}");
            introspectInfo = jsonSlurper.parseText(response.body())
            log.debug "\tFusion Introspection info: $introspectInfo"
        } else {
            log.warn "Could not get valid Introspection information: ${this.fusionBase}"
        }
        return apiInfo
    }


    /**
     * get fusion version, likely part of setting the rest api structure in future calls
     */
    def getFusionVersion() {
        // todo :: figure out what is a good way to get relevant version info, the calls in getFusionVersion don't have what I expect (version is 'local' for localhost 4.2.6)
        return "more to come RSN..."
    }


    /**
     * execute 'basic query'
     * e.g. https://radar.lucidworks.com/api/apps/Lucy/query-pipelines/lucy-basic-qryp/collections/Lucy/select?echoParams=all&wt=json&json.nl=arrarr&debug=timing&debug=query&debug=results&fl=score,*&sort&start=0&q=Joti dhillon (csm "customer success")&rows=10
     */
    def query(String baseUrl, String app, String coll, String qryp, Map<String, Object> qparams, String reqHandler = 'select') {

        URIBuilder ub = new URIBuilder("${baseUrl}/apps/${app}/query-pipelines/${qryp}/collections/${coll}/${reqHandler}");
        qparams.each { String name, def val ->
            log.debug "\t\t Adding param: $name => $val"
            ub.addParameter(name, "${val}")         // needs to be a string?
        }
        URI uri = URI.create(ub.toString())
        log.debug "\t\tprepared uri: $uri"

        var request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .build();

        // todo -- any difference if we do not use String type/generic?
        HttpResponse<String> queryResponse = httpFusionClient.send(request, HttpResponse.BodyHandlers.ofString());
        log.debug "Query response: $queryResponse"
        return queryResponse
    }


    HttpResponse query(String qryPipeline, Map<String, Object> qparams) {
        log.info "\tQuery with params: $qparams"
        HttpResponse r = this.query(fusionBase, application, collection, qryPipeline, qparams)
        return r
    }

    HttpResponse query(String qryPipeline, String q) {
        Map qparams = [q: q]
        HttpResponse r = this.query(fusionBase, application, collection, qryPipeline, qparams)
        return r
    }

    HttpResponse query(String q) {
        Map qparams = [q: q]
        String qryp = collection
        log.info "\t\t defaulting to querypipeline same name as collection: $qryp..."
        HttpResponse r = this.query(fusionBase, application, collection, qryp, qparams)
        return r
    }

    /**
     * helper function to standardize/streamline? code for parsing & handling query response
     * todo -- much work to harden and expand this functionality....
     * @param queryResponse
     * @return
     */
    List<Map<String, Object>> getQueryResponseDocs(HttpResponse queryResponse) {
        log.info "parse query response and return docs (with highlighting integrated)"
        Map<String, Object> json = parseResponse(queryResponse)
        List docs = json?.response?.docs
        return docs
    }

    Map<String, Object> parseResponse(HttpResponse response){
        String respStr = response.body()
        //todo -- parse other formats (xml...)
//        def headers = res
        JsonSlurper jsonSlurper = new JsonSlurper()
        Map<String, Object> json = jsonSlurper.parseText(respStr)
        return json
    }

    /**
     * post content (Map) to fusion index pipeline (or profile?)
     * e.g. api/index-pipelines/{{idxp}}/collections/{{coll}}/index?commit=false
     * default commit=false, allow solr autocommit to work it's magic
     * @return
     */
    HttpResponse indexContent(List<Map<String, Object>> docs, String idxp = '', String coll = '', boolean commit = false) {
        String jsonToIndex = new JsonBuilder(docs).toString()

        HttpResponse<String> indexResponse = indexContent(jsonToIndex, coll, idxp, commit)
        //if(indexResponse.resp
        return indexResponse
    }


    /**
     * post content (Map) to fusion index pipeline (or profile?)
     * e.g. api/index-pipelines/{{idxp}}/collections/{{coll}}/index?commit=false
     * default commit=false, allow solr autocommit to work it's magic
     * @return
     */
    HttpResponse indexContent(String jsonToIndex, String idxp = '', String coll = '', boolean commit = false) {
        HttpResponse<String> indexResponse = null

        if (!coll) {
            log.info "\t indexContent: defaulting collection to app:$application..."
            coll = application
        }
        if (!idxp) {
            log.info "\t idnexContent: defaulting index pipeline to collection name: $coll"
            idxp = coll
        }

        try {
            String url = "$fusionBase/index-pipelines/${idxp}/collections/${coll}/index?commit=${commit}"
            if (log.isDebugEnabled()) {
                log.debug "\t indexContent url: $url -- Json:\t $jsonToIndex"
            } else {
                log.info "\t indexContent url: $url -- Json text size::\t ${jsonToIndex.size()}"
            }
            var indexRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
            // .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .POST(HttpRequest.BodyPublishers.ofString(jsonToIndex))
                    .build();

            indexResponse = httpFusionClient.send(indexRequest, HttpResponse.BodyHandlers.ofString());
            int statusCode = indexResponse.statusCode()
            if (isSuccessResponse(indexResponse)) {
                log.info("\t\t Index response: ${indexResponse.statusCode()}")
            } else {
                String body = indexResponse.body()
                log.warn "Response shows unsuccessful? Status code: $statusCode -- ${body}"
            }

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException: $iae"
        }

        return indexResponse
    }


    boolean isValidFusionClient() {
        boolean valid = false
        if (httpFusionClient && sessionCookie && cookieMS) {
            long nowMS = System.currentTimeMillis()
            long cookieAgeMS = (nowMS - cookieMS)
            if (cookieAgeMS > MAX_COOKIE_AGE_MS) {
                log.warn "Cookie is older than MAX_COOKIE_AGE_MS ($MAX_COOKIE_AGE_MS), need to refresh... returning false for isValidFusionClient()"
                valid = false
            }
            log.debug "\tFusion client seems valid, and we have a session cookie"
            valid = true
        } else {
            log.info "fusionClient($httpFusionClient) is not valid/current:: sessionCookie:$sessionCookie -- cookieMS:$cookieMS"
            valid = false
        }
        return valid
    }

    boolean isSuccessResponse(HttpResponse<String> response) {
        int statusCode = response.statusCode()
        if (statusCode >= 200 && statusCode < 300) {
            log.debug "\t\tSuccessful request/response, code: $statusCode"
            return true
        } else {
            if (response.body()) {
                JsonSlurper jsonSlurper = new JsonSlurper()
                Map jsonBody = jsonSlurper.parseText(response.body())
                def lwStacks = jsonBody?.cause?.stackTrace?.findAll { cn ->
                    !(cn.className =~ /google|jetty|jvnet|sun|glassfish|reflect/)
                }

                log.warn "\t\t Failure?? request/response code: $statusCode. Response details: ${jsonBody.details}\n${lwStacks?.join('\n')}"
            } else {
                log.warn "\\t\\t Failure?? request/response code: $statusCode. Response: ${response}"
            }
            return false
        }
    }

    def useLuke(){
//        oldmac:8764/api/solr/lucy/admin/luke
        //        def rsp = query(fusionBase, application, collection, )
//                URIBuilder ub = new URIBuilder("${baseUrl}/api/solr/$collection/");
//                qparams.each { String name, def val ->
//                    log.debug "\t\t Adding param: $name => $val"
//                    ub.addParameter(name, "${val}")         // needs to be a string?
//                }
//                URI uri = URI.create(ub.toString())
//                log.debug "\t\tprepared uri: $uri"
//
//                var request = HttpRequest.newBuilder()
//                        .GET()
//                        .uri(uri)
//                        .build();
//
//                // todo -- any difference if we do not use String type/generic?
//                HttpResponse<String> queryResponse = httpFusionClient.send(request, HttpResponse.BodyHandlers.ofString());
//                log.debug "Query response: $queryResponse"
//                return queryResponse
//

    }

    /**
     * get all the terms for the current collection
     */
    HttpResponse getTermsResponse(String field, int limitCount=1000000, String regex = '[a-z]+'){
//        def rsp = query(fusionBase, application, collection, )
//        URIBuilder ub = new URIBuilder("${fusionBase}/solr/$collection/terms?terms.fl=${field}");
//        URIBuilder ub = new URIBuilder("${fusionBase}/solr/$collection/terms?terms.fl=${field}&terms.limit=$limitCount&terms.regex=$regex");
        URIBuilder ub = new URIBuilder("${fusionBase}/solr/$collection/terms")
                .addParameter('terms.fl', field)
                .addParameter('terms.limit', "$limitCount")
                .addParameter('terms.regex', regex)
        URI uri = URI.create(ub.toString())
        log.info "\t\tprepared uri: $uri"

        var request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .build();

        // todo -- any difference if we do not use String type/generic?
        HttpResponse<String> queryResponse = httpFusionClient.send(request, HttpResponse.BodyHandlers.ofString());
        log.debug "Query response: $queryResponse"
        return queryResponse
    }

    Map<String, Integer> parseTermsResponse(HttpResponse rsp){
        Map responseMap = parseResponse(rsp)
        def fieldList = responseMap.terms.keySet()
        Map<String, Map<String, Integer>> fieldsTermsMap = [:]
        fieldList.each { String field ->
            Map<String, Integer> termsMap = [:]
            List termsAndCounts = responseMap.terms[field]

            for (int i = 0; i < termsAndCounts.size() / 2; i++) {
                String term = termsAndCounts[i*2]
                int count = termsAndCounts[(i*2) + 1]
                termsMap[term] = count
                if(i % 1000 == 0) {
                    log.info "$i) term: $term "
                }
            }
            fieldsTermsMap[field] = termsMap
        }
        return fieldsTermsMap
    }


    HttpResponse deleteByQuery(String deleteQuery, boolean commit = false) {
        Map json = [delete: [query: deleteQuery]]
        JsonBuilder jsonBuilder = new JsonBuilder(json)
        String js = jsonBuilder.toString()
        HttpResponse response = null
        try {
            String url = "$fusionBase/solr/$collection/update?commit=${commit}"
            log.info "\t Delete Content (solr) url: $url -- js body: $js"
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(js))
                    .build();

            response = httpFusionClient.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode()
            if (isSuccessResponse(response)) {
                log.info("\t\t Index response: ${response.statusCode()}")
            } else {
                log.warn "Response shows unsuccessful? Status code: $statusCode -- $response"
            }

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException: $iae"
        }

        return response
    }
}
