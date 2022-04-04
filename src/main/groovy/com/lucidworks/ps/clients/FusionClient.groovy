package com.lucidworks.ps.clients

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.time.Duration

/**
 * attempt to replicate solrj client-ness for fusion
 * using cookies and session api to start with
 * this client will 'remember' the base fusion connection info (base url, user/pass,...) and allow flexibility among apps, collections, pipelines...
 *
 * sample rest calls: http://localhost:8764/api
 * POST http://localhost:8764/api/session
 * GET  http://localhost:8764/api/apps/${app}/query-pipelines/$qryp/collections/$coll/select?q=spark
 * POST http://localhost:8764/api/index-pipelines/$idxp/collections/$coll/index?commit=true
 *
 * todo beef up wt support, assuming json for everything at the moment...
 * todo handle f4 vs f5 (and managed fusion?) differences
 * todo consider httpclient cache for peformance and multithreading
 * todo add parallelism and scaling for bulk data transfers
 */
class FusionClient {
    protected static Logger log = Logger.getLogger(this.class.name)
    String user
    String password

    String fusionBase
    HttpClient httpClient
    private String sessionCookie
    Map introspectInfo
    Map apiInfo
    Long cookieMS
    long MAX_COOKIE_AGE_MS = 1000 * 60 * 15
    // starting with default of 15 minutes for cookie age?      // todo -- revisit, make this more accessible
    List<FusionResponseWrapper> responses = []

    FusionClient(String baseUrl, String user, String pass) {
        this(new URL(baseUrl), user, pass)
    }


    FusionClient(URL baseUrl, String user, String pass) {
        this.fusionBase = baseUrl
        this.user = user
        this.password = pass

        String s = baseUrl.toString()
        if (s.endsWith('/')) {
            this.fusionBase = new URL(s[0..-2])
            log.warn "\tstripping trailing slash from baseUrl: '$s' => "
        }
        httpClient = buildClient(fusionBase, user, password)
        log.debug "constructor: $baseUrl, $user, $pass :: calling buildClient..."

    }


/**
 * Get a JDK HttpClient with some defaults set for convenience
 * @param baseUrl
 * @param user
 * @param pass
 * @return
 */
    HttpClient buildClient(String baseUrl, String user, String pass) {
        log.info "\t${this.class.simpleName} getClient(baseurl: $baseUrl, user:$user, password <hidden>...)   [should only need to call this once...]"
        HttpClient client = HttpClient.newBuilder()
                .cookieHandler(new CookieManager())
                .version(HttpClient.Version.HTTP_1_1)
                .build()

        // groovy string template for speed, rather than json builder
        String authJson = """{"username":"$user", "password":"$pass"}"""

        String urlSession = "${baseUrl}/api/session"
        log.info "\tInitializing Fusion client session via POST to session url: $urlSession"

        try {
            HttpRequest request = buildPostRequest(urlSession, authJson)

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString())
            FusionResponseWrapper fusionResponseWrapper = new FusionResponseWrapper(request,response)
            responses << fusionResponseWrapper
            log.debug("\t\tResponse status: " + response.statusCode())
            sessionCookie = response.headers().firstValue("set-cookie")
            cookieMS = System.currentTimeMillis()
            Date ts = new Date(cookieMS)
            log.info("\tSession cookie: ${this.sessionCookie} set/refreshed at timestamp: $cookieMS (${ts})")

        } catch (Exception e) {
            log.warn "Problem getting client: $e"
            client = null
        }

        return client
    }


    /**
     * Get json "list" of applications defined in the cluster. See also: export
     * @return
     */
    List<Map<String, Object>> getApplications() {
        HttpResponse response = null
        String url = "$fusionBase/api/apps"
        log.info "\t\tExport Fusion applications url: $url"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        List<Map<String, Object>> applications = fusionResponseWrapper.parsedInfo
        return applications
    }


    /**
     * trying to get general information, primarily fusion version, so we can adjust api syntax accordingly
     *
     * @return
     */
    def getFusionInformation() {
        JsonSlurper jsonSlurper = new JsonSlurper()
        HttpRequest request = buildGetRequest(url)

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        int rc = response.statusCode()
        if (rc >= 200 && rc < 300) {
            log.debug("Response from getFusionInformation() ${response.statusCode()}")
            apiInfo = jsonSlurper.parseText(response.body())
            log.debug "Fusion API info: $apiInfo"
        } else {
            log.warn "Could not get valid API information: ${this.fusionBase}"
        }

        URI uri = (URI.create("${this.fusionBase}/api/introspect"))
        var reqIntro = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .timeout(Duration.ofMinutes(2))
                .setHeader("User-Agent", "Java 11+ HttpClient FusionPS Bot") // add request header
//                .header("Content-Type", "application/json")
                .build()

        HttpResponse<String> respIntro = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        rc = respIntro.statusCode()
        if (rc >= 200 && rc < 300) {

            log.debug("Response introspection result code: ${respIntro.statusCode()}")
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
//    def getFusionVersion() {
//        // todo :: figure out what is a good way to get relevant version info, the calls in getFusionVersion don't have what I expect (version is 'local' for localhost 4.2.6)
//        return "more to come RSN..."
//    }


    boolean isValidFusionClient() {
        boolean valid = false
        if (httpClient && sessionCookie && cookieMS) {
            long nowMS = System.currentTimeMillis()
            long cookieAgeMS = (nowMS - cookieMS)
            if (cookieAgeMS > MAX_COOKIE_AGE_MS) {
                log.warn "Cookie is older than MAX_COOKIE_AGE_MS ($MAX_COOKIE_AGE_MS), need to refresh... returning false for isValidFusionClient()"
                valid = false
            }
            log.debug "\tFusion client seems valid, and we have a session cookie"
            valid = true
        } else {
            log.info "fusionClient($httpClient) is not valid/current:: sessionCookie:$sessionCookie -- cookieMS:$cookieMS"
            valid = false
        }
        return valid
    }


    /**
     * execute 'basic query'
     * e.g. https://radar.lucidworks.com/api/apps/Lucy/query-pipelines/lucy-basic-qryp/collections/Lucy/select?echoParams=all&wt=json&json.nl=arrarr&debug=timing&debug=query&debug=results&fl=score,*&sort&start=0&q=Joti dhillon (csm "customer success")&rows=10
     */
    def query(String app, String collection, String queryPipeline, Map<String, Object> qparams = [:], String reqHandler = 'select') {

        URIBuilder ub = new URIBuilder("${this.fusionBase}/api/apps/${app}/query-pipelines/${queryPipeline}/collections/${collection}/${reqHandler}")
//        URI uri = URI.create("${this.fusionBase}/api/apps/${app}/query-pipelines/${queryPipeline}/collections/${collection}/${reqHandler}")
        qparams.each { String name, def val ->
            log.debug "\t\t Adding param: $name => $val"
            uriaddParameter(name, "${val}")         // needs to be a string?
        }
        URI uri = URI.create(ub.toString())
        log.info "\t\tprepared uri: $uri"

        HttpRequest request = buildGetRequest(url)

        HttpResponse<String> queryResponse = null
        try {
            // todo -- any difference if we do not use String type/generic?
            queryResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            log.info "Query response: $queryResponse"
        } catch (ConnectException ce) {
            log.info "Connection error...? $ce"
        }
        return queryResponse
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

    Map<String, Object> parseResponse(HttpResponse response) {
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
    HttpResponse indexContentByCollectionPipeline(String collection, String indexPipeline, List<Map<String, Object>> docs, boolean commit) {
        String jsonToIndex = new JsonBuilder(docs).toString()

        HttpResponse<String> indexResponse = indexContent(collection, indexPipeline, jsonToIndex, commit)
        return indexResponse
    }

    /**
     * post content (Map) to fusion index profile
     * e.g. api/index-pipelines/{{idxp}}/collections/{{coll}}/index?commit=false
     * default commit=false, allow solr autocommit to work it's magic
     * @return
     */
/*
    HttpResponse indexDocumentsByApplicationProfile(String application, String indexProfile, List<Map<String, Object>> docs, boolean commit) {
        String jsonToIndex = new JsonBuilder(docs).toString()

        HttpResponse<String> indexResponse = indexDocumentsByApplicationProfile(application, indexProfile, jsonToIndex, commit)
        //if(indexResponse.resp
        return indexResponse
    }
*/


    /**
     * post content (Map) to fusion index pipeline (or profile?)
     * e.g. api/index-pipelines/{{idxp}}/collections/{{coll}}/index?commit=false
     * default commit=false, allow solr autocommit to work it's magic
     * @return
     */
    HttpResponse indexContent(String collection, String indexPipeline, String jsonToIndex, boolean commit) {
        HttpResponse<String> indexResponse = null
        try {
            String url = "$fusionBase/api/index-pipelines/${indexPipeline}/collections/${collection}/index?commit=${commit}"
            var indexRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .POST(HttpRequest.BodyPublishers.ofString(jsonToIndex))
                    .build()

            indexResponse = httpClient.send(indexRequest, HttpResponse.BodyHandlers.ofString())
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

    HttpResponse indexContentByProfile(Collection collToIndex, String app, String idxprofile, boolean commit) {
        String jsonToIndex = new JsonBuilder(collToIndex).toString()
        indexContentByProfile(jsonToIndex, app, idxprofile, commit)
    }

    /**
     * post content (Map) to fusion index profile
     * e.g. http://34.82.69.71:6764/api/apps/Lucy/index/upval-json
     * default commit=false, allow solr autocommit to work it's magic
     * @return
     */
    HttpResponse indexContentByProfile(String jsonToIndex, String app, String idxprofile, boolean commit) {
        HttpResponse<String> indexResponse = null
        try {
            JsonSlurper jsonSlurper = new JsonSlurper()

            // todo -- revisit base url... constructor includes '/api' by default... is this wise?
            String url = "$fusionBase/api/apps/${app}/index/$idxprofile?commit=${commit}"
            log.debug "\t indexContent url: $url -- Json:\t $jsonToIndex"
            long start = System.currentTimeMillis()

            var request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .POST(HttpRequest.BodyPublishers.ofString(jsonToIndex))
                    .build()
            long end = System.currentTimeMillis()
            long elapsed = end - start
            log.debug "\t\tElapsed time1: ${elapsed}ms (${elapsed / 1000} sec) -- build HttpRequest to post/index content"

            indexResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

            end = System.currentTimeMillis()
            elapsed = end - start
            log.info "\t\tElapsed time: ${elapsed}ms (${elapsed / 1000} sec) - to actually index content"

            int statusCode = indexResponse.statusCode()
            String body = indexResponse.body()
            Map bodyMap = jsonSlurper.parseText(body)
            if (isSuccessResponse(indexResponse)) {
                log.debug("\t\tIndex response: ${indexResponse.statusCode()}")
                log.debug "\t\tBody response map: $bodyMap"
            } else {
                log.warn "Response shows unsuccessful? Status code: $statusCode -- ${body}"
            }

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException: $iae"
        }

        return indexResponse
    }


    boolean isSuccessResponse(HttpResponse response) {
        int statusCode = response.statusCode()
        if (statusCode >= 200 && statusCode < 300) {
            log.debug "\t\tSuccessful request/response, code: $statusCode"
            return true
        } else {
            if (response.body()) {
                JsonSlurper jsonSlurper = new JsonSlurper()
                Map jsonBody = jsonSlurper.parseText(response.body())
//                def lwStacks = jsonBody?.cause?.stackTrace?.findAll { cn ->
//                    !(cn.className =~ /google|jetty|jvnet|sun|glassfish|reflect/)
//                }
//                log.warn "\t\t Failure?? request/response code: $statusCode. Response details: ${jsonBody.details}\n${lwStacks?.join('\n')}"
                String msg = jsonBody.message
                String details = jsonBody.details

                log.warn "(code: $statusCode) Failed call, message:[$msg] \n\t\t details: $details"
                String causeMessage = jsonBody?.cause?.message
                if (causeMessage) {
                    log.warn "\t\tCause message: $causeMessage"
                }
            } else {
                log.warn "Failure?? request/response code: $statusCode. Response: ${response} -- ${response.body()}"
            }
            return false
        }
    }

    def useLuke() {
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
/*
    HttpResponse getTermsResponse(String collection, String field, int limitCount = 1000000, String regex = '[a-z]+') {
//        def rsp = query(fusionBase, application, collection, )
//        URIBuilder ub = new URIBuilder("${fusionBase}/solr/$collection/terms?terms.fl=${field}");
//        URIBuilder ub = new URIBuilder("${fusionBase}/solr/$collection/terms?terms.fl=${field}&terms.limit=$limitCount&terms.regex=$regex");
        URIBuilder ub = new URIBuilder("${fusionBase}/api/solr/$this.collection/terms")
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
*/

    Map<String, Integer> parseTermsResponse(HttpResponse rsp) {
        Map responseMap = parseResponse(rsp)
        def fieldList = responseMap.terms.keySet()
        Map<String, Map<String, Integer>> fieldsTermsMap = [:]
        fieldList.each { String field ->
            Map<String, Integer> termsMap = [:]
            List termsAndCounts = responseMap.terms[field]

            for (int i = 0; i < termsAndCounts.size() / 2; i++) {
                String term = termsAndCounts[i * 2]
                int count = termsAndCounts[(i * 2) + 1]
                termsMap[term] = count
                if (i % 1000 == 0) {
                    log.info "$i) term: $term "
                }
            }
            fieldsTermsMap[field] = termsMap
        }
        return fieldsTermsMap
    }


    HttpResponse deleteByQuery(String collection, String deleteQuery, boolean commit = false) {
        Map delCommand = [delete: [query: deleteQuery]]
        JsonBuilder jsonBuilder = new JsonBuilder(delCommand)
        String jsonString = jsonBuilder.toString()
        HttpResponse response = null
        try {
            String url = "$fusionBase/api/solr/$collection/update?commit=${commit}"
            log.info "\t\tDelete Content (solr) url: $url -- json delete command: $jsonString"
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonString))
                    .build()

            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            int statusCode = response.statusCode()
            if (isSuccessResponse(response)) {
                log.info("\t\t Delete by query response: ${response.statusCode()}")
            } else {
                log.warn "Response shows unsuccessful? Status code: $statusCode -- $response"
            }

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException: $iae"
        }

        return response
    }


    HttpResponse<Path> exportFusionObjects(String exportParams, Path outputPath) {
        String url = "$fusionBase/api/objects/export?${exportParams}"
        log.info "\t\tExport Fusion objects url: $url"
        HttpRequest collectionRequest = buildGetRequest(url)

        HttpResponse<Path> fileResponse = null
        try {
            fileResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofFile(outputPath))

            int statusCode = fileResponse.statusCode()
            if (isSuccessResponse(fileResponse)) {
                log.info "\t\tOutput file: ${fileResponse} --> ${outputPath.toAbsolutePath()} \t response: ${fileResponse.statusCode()}"
            } else {
                log.warn "Response shows unsuccessful? Status code: $statusCode -- $fileResponse"
            }
        } catch (Exception e) {
            log.error "Export exception: $e"
        }
        return fileResponse
    }


    /**
     * create an application
     * TODO -- more code - implement
     * @param properties
     */
    def createApplication(Map properties, boolean relatedObjects = true) {
        HttpResponse<String> response = null
        JsonBuilder jsonBuilder = new JsonBuilder(properties)
        String jsonToIndex = jsonBuilder.toString()
        String id = properties.id
        String name = properties.name
        def info = null
        try {
            String url = "$fusionBase/api/apps"
            log.info "\t Create APP ($name) url: $url -- Json text size::\t ${jsonToIndex.size()} with object id: $id"
            HttpRequest request = buildPostRequest(url, jsonToIndex)

            FusionResponseWrapper fusionResponse = sendFusionRequest(request)
            info = fusionResponse.parsedInfo
        } catch (Exception e) {
            log.error "Error: $e"
        }
        return info
    }

    public FusionResponseWrapper sendFusionRequest(HttpRequest request) {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        FusionResponseWrapper fusionResponse = new FusionResponseWrapper(request,response)
        responses << fusionResponse
        return fusionResponse
    }

    List<Map> getCollections(String appName) {
        HttpResponse<String> response = null
        String url = "$fusionBase/api/apps/${appName}/collections"
        log.info "\t list collections for app $appName url: $url "
        HttpRequest request = buildGetRequest(url)

        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }


    /**
     *
     * @param collection
     * @param appName
     * @param defaultFeatures (set to false, since user is likely to create an app, which would have primary coll with default features) --here we assume supporting collections
     * @return
     */
    def createCollection(Map<String, Object> collection, String appName, boolean defaultFeatures = false) {
        HttpResponse<String> collectionResponse = null
        String collName = collection.id
        JsonBuilder jsonBuilder = new JsonBuilder([id: collName])
        String jsonToIndex = jsonBuilder.toString()
        try {
            String url = "$fusionBase/api/apps/${appName}/collections"      // todo: add defaultFeatures?
            log.info "\t Create COLLECTION ($collName) url: $url -- Json text size::\t ${jsonToIndex.size()}"
            HttpRequest request = buildPostRequest(url, jsonToIndex)

            FusionResponseWrapper responseWrapper = sendFusionRequest(request)

            collectionResponse = responseWrapper.response

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException: $iae"
        }

        return collectionResponse
    }

    List getParsers(String appName) {
        HttpResponse<String> response = null
        String url = "$fusionBase/api/parsers"
        log.info "\t list parsers for url: $url "
        HttpRequest  request = buildGetRequest(url)

        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }

    def     createParser(Map<String, Object> map, String app) {
        HttpResponse<String> response = null
        JsonBuilder jsonBuilder = new JsonBuilder(map)
        String name = map.id
        String jsonToIndex = jsonBuilder.toString()
        String url = "$fusionBase/api/apps/${app}/parsers"
        try {
            log.info "\t Create PARSER ($name) url: $url -- Json text size::\t ${jsonToIndex.size()}"
            HttpRequest request = buildPostRequest(url, jsonToIndex)

            FusionResponseWrapper responseWrapper = sendFusionRequest(request)

            return responseWrapper.parsedInfo

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating parser($name): $iae"
        }

        return response
    }

    Object getIndexPipelines(String app) {
        HttpResponse<String> response = null
        String url = null
        if (app) {
            url = "$fusionBase/api/apps/${app}/index-pipelines"
        } else {
            url = "$fusionBase/api/index-pipelines"
            log.info "No app given, getting all index pipelines..."
        }
        log.info "\t list idx pipelines for url: $url "
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }

    def createIndexPipeline(Map<String, Object> map, String app) {
        HttpResponse<String> response = null
        JsonBuilder jsonBuilder = new JsonBuilder(map)
        String name = map.id
        String jsonToIndex = jsonBuilder.toString()
        try {
            String url = "$fusionBase/api/apps/${app}/index-pipelines"
            log.info "\t Create INDEX PIPELINE ($name) url: $url -- Json text size::\t ${jsonToIndex.size()}"
            HttpRequest request = buildPostRequest(url, jsonToIndex)

            FusionResponseWrapper responseWrapper = sendFusionRequest(request)

            response = responseWrapper.response

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating INDEX PIPELINE($name): $iae"
        }

        return response
    }


    Object getConnectorsRepository() {
        String url = "$fusionBase/connectors/repository"
        log.info "getting all available connectors"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }


    Object getConnectorsInstalled() {
        HttpResponse<String> response = null
        String url = null
        url = "$fusionBase/api/connectors/plugins"
        log.info "getting all installed connectors"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }


    Object getConnectorsUsed(Map objectsMap) {
        def dataSources = objectsMap['dataSources']
        def connectorsGrouped = dataSources.groupBy { Map ds ->
            ds.connector
        }
        return connectorsGrouped
    }

    HttpResponse<String> installConnector(String connectorId) {
        HttpResponse<String> response = null
        try {
            String url = "$fusionBase/api/connectors/plugins?id=${connectorId}"
            log.info "\t install connector plugin ($connectorId) from repository with url: $url"
            HttpRequest request = buildPostRequest(url, jsonToIndex)
            FusionResponseWrapper responseWrapper = sendFusionRequest(request)
            response = responseWrapper.response
        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating datasources($connectorId): $iae"
        }

        return response
    }


    Object getDataSources() {
        HttpResponse<String> response = null
        String url = "$fusionBase/api/connectors/datasources"
        log.info "No app given, getting all datasources"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }


    List<Map<String, Object>> getDataSources(String appName) {
        HttpResponse<String> response = null
        String url = "$fusionBase/api/apps/${appName}/connectors/datasources"
        log.info "app (${appName}) given, getting all datasources"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
        return responseWrapper.parsedInfo
    }


    def createDataSource(Map<String, Object> map, String app) {
        HttpResponse<String> response = null
        JsonBuilder jsonBuilder = new JsonBuilder(map)
        String name = map.id
        String jsonToIndex = jsonBuilder.toString()
        String connector = map.connector
        try {
            String url = "$fusionBase/api/apps/${app}/connectors/datasources"
            log.info "\t Create DATASOURCES ($name) type($connector) url: $url -- Json text size::\t ${jsonToIndex.size()}"
            HttpRequest request = buildPostRequest(url, jsonToIndex)

            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            int statusCode = response.statusCode()
            if (isSuccessResponse(response)) {
                log.info("\t\t Create DATASOURCES response($name): ${response.statusCode()}")
            } else {
                String body = response.body()
                String type = map.type
                log.warn "Failed to create DATASOURCES (type:$type): $name? Status code: $statusCode \n\t\t object map: $map}"
            }

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating datasources($name): $iae"
        }

        return response
    }

    List<Map<String, String>> getLinks() {
        HttpResponse<String> response = null
        String url = "$fusionBase/api/links"
        HttpRequest request = buildGetRequest(url)

        response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        int statusCode = response.statusCode()
        String body = response.body()
        def info = null
        if (isSuccessResponse(response)) {
            info = new JsonSlurper().parseText(body)
            log.info("\t\t get links response: ${response.statusCode()}")
        } else {
            log.warn "Failed to get links: Status code: $statusCode -- ${body}"
        }
        return info
    }

    HttpResponse<String> createLink(Map<String, Object> map, String app) {
        HttpResponse<String> response = null
        String name = "${map.subject}:${map.object}::${map.linkType}"
        JsonBuilder jsonBuilder = new JsonBuilder(map)
        String jsonToIndex = jsonBuilder.toString()
        try {
            String url = "$fusionBase/api/links"
            log.info "\t Create LINK ($name)  url: $url -- Json text size::\t ${jsonToIndex.size()}"
            HttpRequest request = buildPutRequest(url, jsonToIndex)

            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            int statusCode = response.statusCode()
            if (isSuccessResponse(response)) {
                log.info("\t\t Create LINK response($name): ${response.statusCode()}")
            } else {
                String body = response.body()
                log.warn "Failed to create LINK: $name? Status code: $statusCode "
            }

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating LINK($name): $iae"
        }

        return response
    }

    HttpRequest buildPutRequest(String url, String jsonToIndex) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(1))
                .header("Content-Type", "application/json")
                .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                .PUT(HttpRequest.BodyPublishers.ofString(jsonToIndex))
                .build()
        return request
    }


    HttpRequest buildGetRequest(String url) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(1))
                .header("Content-Type", "application/json")
                .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                .GET()
                .build()
        return request
    }

    HttpRequest buildPostRequest(String url, String jsonToIndex) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(1))
                .header("Content-Type", "application/json")
                .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                .POST(HttpRequest.BodyPublishers.ofString(jsonToIndex))
                .build()
        return request
    }


}
