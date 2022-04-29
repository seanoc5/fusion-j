package com.lucidworks.ps.clients

import com.lucidworks.ps.solr.SolrConfigThing
import groovy.cli.picocli.OptionAccessor
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.http.auth.AuthenticationException
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
    String user = null
    String password = null

    String fusionBase = null
    HttpClient httpClient = null
    private String sessionCookie = null
    File objectsJsonFile = null
    File exportDirectory = null
    String objectsGroup = null
    Map introspectInfo = null
    Map apiInfo = null
    Long cookieMS = null
    long MAX_COOKIE_AGE_MS = 1000 * 60 * 15
    // starting with default of 15 minutes for cookie age?   todo -- revisit, make this more accessible
    List<FusionResponseWrapper> responses = []
    // todo -- look at potential OOM issues for long running client

    /**
     * typical constructor with information on how to connect to a fusion (F4 or F5, possibly other versions) - convert String url to proper Java URL
     * @param baseUrl e.g. http://yourdemocluster.lucidworks.com:6764   (trailing slash will be removed if included)
     * @param user
     * @param pass
     */
    FusionClient(String baseUrl, String user, String pass) {
        this(new URL(baseUrl), user, pass)
    }


    /**
     * typical constructor with information on how to connect to a fusion (F4 or F5, possibly other versions) - proper Java URL argument version
     * @param baseUrl e.g. http://yourdemocluster.lucidworks.com:6764   (trailing slash will be removed if included)
     * @param user
     * @param pass
     */
    FusionClient(URL baseUrl, String user, String pass) {
        this.fusionBase = baseUrl
        this.user = user
        this.password = pass

        String s = baseUrl.toString()
        if (s.endsWith('/')) {
            this.fusionBase = new URL(s[0..-2])
            log.warn "\tstripping trailing slash from baseUrl: '$s' => "
        }
        log.debug "constructor: $baseUrl, $user, $pass :: calling buildClient..."
        httpClient = buildClient(fusionBase, user, password)
    }


    /**
     * Build HttpClient based on CliBuilder parsed commandline options
     * todo -- revisit this code/placement, likely this is the wrong place for this helper/shortcut config
     * @param options
     */
    FusionClient(OptionAccessor options) {
        fusionBase = options.f
        user = options.u                            // fusion username for destination of new/migrated app
        password = options.p
        String srcObjectJsonPath = options.s
        // todo -- add logic to read configs from live fusion (needs source: url, pass, furl, appname)
        File srcFile = new File(srcObjectJsonPath)
        if (srcFile.isFile() && srcFile.exists()) {
            objectsJsonFile = srcFile
            log.info "Source file exists: ${objectsJsonFile.absolutePath}"
        } else {
            log.debug "Source file: ${srcFile.absolutePath} does NOT EXIST..."
        }

        objectsGroup = options.g
        String x = options.x ?: null
        if (x) {
            exportDirectory = new File(x)
            if (exportDirectory.exists()) {
                if (exportDirectory.isDirectory()) {
                    log.debug "Got reasonable export dir: ${exportDirectory.absolutePath}"
                } else {
                    String msg = "Export directory not a directory: ${exportDirectory.absolutePath}"
                    log.error msg
                    throw new IllegalArgumentException(msg)
                }
            } else {
                exportDirectory = new File(exportDir)
                if (exportDirectory.mkdir()) {
                    log.warn "Export dir($exportDir) did not exist, but we were able to make it (one child deep): ${exportDirectory.absolutePath}"
                }
            }
        }

        log.info """Initialized FusionClient with command line options:
            Fusion Url:     ${fusionBase}
            Src json:       $objectsJsonFile
            User:           $user
            Password:       [redacted]
            Group Name:     $objectsGroup
            Export dir:     $exportDirectory
        """
        httpClient = buildClient(fusionBase, user, password)
        log.debug "finished clibuilder options constructor..."
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
        log.info "\t\tInitializing Fusion client session via POST to session url: $urlSession"

//        try {
        HttpRequest request = buildPostRequest(urlSession, authJson)

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString())
        FusionResponseWrapper fusionResponseWrapper = new FusionResponseWrapper(request, response)
        responses << fusionResponseWrapper
        if (response.statusCode() < 300) {
            log.debug("\t\tResponse status: " + response.statusCode())
            sessionCookie = response.headers().firstValue("set-cookie")
            cookieMS = System.currentTimeMillis()
            Date ts = new Date(cookieMS)
            log.debug("\tSession cookie: ${this.sessionCookie} set/refreshed at timestamp: $cookieMS (${ts})")
        } else {
            log.error "Bad status code creating client (incorrect auth??), Status Code: ${response.statusCode()} -- body: ${response.body()}"
            throw new AuthenticationException("Could not create Fusion Client (${response.body()})")
        }
//        } catch (Exception e) {
//            log.warn "Problem getting client: $e"
//            client = null
//        }

        return client
    }


    /**
     * encapsulate GET (introspect/export) requests
     * @param url
     * @return
     */
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

    /**
     * encapsulate put (update/replace) requests
     * @param url
     * @param jsonToIndex
     * @return
     */
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

    /**
     * encapsulate POST (create) requests
     * @param url
     * @param jsonToIndex
     * @return
     */
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
     * Helper function that accepts a Map of applications in a source (older?) fusion, and gets a collection of existing applications, and adds any that are missing.
     * @param appMap
     * @return
     */
    FusionResponseWrapper addAppIfMissing(Map appMap) {
        String appName = appMap.name
        log.info "Add app if missing: $appName"
        // --------------- Create Application ------------------
        Map appDefinition = [id          : appMap.id,
                             name        : appName,
                             description : "${appMap.description}\n Automigrated by LW UpVal utility -- ${new Date()}",
                             "properties": [headerImageName: appMap.properties.headerImageName, tileColor: appMap.properties.tileColor]]
        boolean relatedObjects = true
        List<Map<String, Object>> existingApps = getApplications()
        def currentApp = existingApps.find { it.name == appName }
        FusionResponseWrapper fusionResponseWrapper = null
        if (currentApp) {
            log.info "Found existing app: $currentApp"
        } else {
            fusionResponseWrapper = createApplication(appDefinition, relatedObjects)
        }
        return fusionResponseWrapper
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

    /**
     * placeholder function to check for session validity (i.e. expired cookie?)
     * todo -- more code and testing here
     * @return
     */
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


    /**
     * helper function to process Fusion responses
     * todo -- add other response formats, or other missing functionality...?
     * @param response
     * @return
     */
    Map<String, Object> parseResponse(HttpResponse response) {
        String respStr = response.body()
        //todo -- parse other formats (xml...)
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

    /**
     * Accept a java-collection of things to index, convert them to json/string,
     * send them to string-based function to index with index PROFILE
     *
     * @param docsToIndex
     * @param app
     * @param idxprofile
     * @param commit
     * @return
     */
    HttpResponse indexContentByProfile(Collection docsToIndex, String app, String idxprofile, boolean commit) {
        String jsonToIndex = new JsonBuilder(docsToIndex).toString()
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


    /**
     * helper function to process response
     * @param response
     * @return
     */
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

    /*    def useLuke() {
        oldmac:8764/api/solr/lucy/admin/luke
                def rsp = query(fusionBase, application, collection, )
                URIBuilder ub = new URIBuilder("${baseUrl}/api/solr/$collection/");
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


    }*/

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

    /**
     * helper function to get terms from solr
     * TODO allow incoming params rather than just the defaults here
     * @param rsp
     * @return
     */

    /*   Map<String, Integer> parseTermsResponse(HttpResponse rsp) {
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
*/

    /**
     * helper call to perform a delete by query
     * Note: this can be expensive on the solr side, consider more performant options for production or anywhere performance matters
     * @param collection
     * @param deleteQuery
     * @param commit
     * @return
     */
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


    /**
     * call fusion rest api to get all the objects defined in fusion
     *
     * @param exportParams
     * @param outputPath
     * @return
     */
    HttpResponse<Path> exportFusionObjects(String exportParams, Path outputPath) {
        String url = "$fusionBase/api/objects/export?${exportParams}"
        log.info "\t\tExport Fusion objects url: $url"
        HttpRequest request = buildGetRequest(url)

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
     *
     * @param properties definition of application details,
     *
     * e.g.
     *
     * Map appDefinition = [id          : appMap.id,
     *                     name        : appName,
     *                     description : "${appMap.description}\n Automigrated by LW UpVal utility -- ${new Date()}",
     *                     "properties": [headerImageName: appMap.properties.headerImageName, tileColor: appMap.properties.tileColor]]
     */
    def createApplication(Map properties, boolean relatedObjects = true) {
//        HttpResponse<String> response = null
        JsonBuilder jsonBuilder = new JsonBuilder(properties)
        String jsonToIndex = jsonBuilder.toString()
        String id = properties.id
        String name = properties.name
        FusionResponseWrapper fusionResponseWrapper = null
        try {
            String url = "$fusionBase/api/apps"
            log.info "\t Create APP ($name) url: $url -- Json text size::\t ${jsonToIndex.size()} with object id: $id"
            HttpRequest request = buildPostRequest(url, jsonToIndex)

            fusionResponseWrapper = sendFusionRequest(request)
            log.info "Created app? $info"
        } catch (Exception e) {
            log.error "Error: $e"
        }
        return fusionResponseWrapper
    }

    /**
     * helper function to send a request to fusion
     * <p>
     *     todo -- review any memory/resource issues with keeping these responses in memory, we assume this client is somewhat short-lived, and saving in memory should not be a problem...
     * @param request object already built
     * @link buildGetRequest* @return the custom FusionResponseWrapper that will have the original request, the response, and some additional helper info/functionality
     */
    public FusionResponseWrapper sendFusionRequest(HttpRequest request, HttpResponse.BodyHandler bodyHandler = HttpResponse.BodyHandlers.ofString()) {
        HttpResponse<String> response = httpClient.send(request, bodyHandler)
        FusionResponseWrapper fusionResponse = new FusionResponseWrapper(request, response)

        responses << fusionResponse         // add this response to the client's collection of responses
        return fusionResponse
    }

    /**
     * Get a list of existing collections in a given application
     * @param appName
     * @return
     */
    List<Map<String, Object>> getCollections(String appName) {
        String url = "$fusionBase/api/apps/${appName}/collections"
        log.info "List collections for app $appName url: $url "
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
        return responseWrapper.parsedInfo
    }


    /**
     * Create a collection in the given fusion application.
     * Note: the defaultFeatures controls things like signals sidecar collections, query rewrite, etc.
     * @param collection
     * @param appName
     * @param defaultFeatures (set to false, since user is likely to create an app, which would have primary coll with default features) --here we assume supporting collections
     * @return
     */
    FusionResponseWrapper createCollection(Map<String, Object> collection, String appName, boolean defaultFeatures = false) {
        FusionResponseWrapper responseWrapper = null
        String collName = collection.id
        JsonBuilder jsonBuilder = new JsonBuilder([id: collName])
        String jsonToIndex = jsonBuilder.toString()
        try {
            String url = "$fusionBase/api/apps/${appName}/collections"      // todo: add defaultFeatures?
            log.info "\t Create COLLECTION ($collName) url: $url -- Json text size::\t ${jsonToIndex.size()}"
            HttpRequest request = buildPostRequest(url, jsonToIndex)
            responseWrapper = sendFusionRequest(request)
        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException: $iae"
        }

        return responseWrapper
    }

    /**
     * get a list of 'known' parsers in the Fusion deployment
     *
     * @param appName
     * @return
     */
    List getParsers(String appName) {
        HttpResponse<String> response = null
        String url = "$fusionBase/api/parsers"
        log.info "\t list parsers for url: $url "
        HttpRequest request = buildGetRequest(url)

        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }

    /**
     * Create a parser object to be used during indexing (i.e. the parser for a given connector)
     *
     * @param map
     * @param app
     * @return
     */
    def createParser(Map<String, Object> map, String app) {
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

    /**
     * get a list index pipelines already defined/existing in the (destination/F5) Fusion deployment
     * @param app
     * @return
     */
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

    /**
     * create and index pipeline by converting map object to json payload including destination application to connect it to
     * @param map
     * @param app
     * @return
     */
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


    /**
     * get a list of connector plugins availble in the Lucidworks plugin repository
     * some of these may already be installed.
     *
     * see also: https://plugins.lucidworks.com/
     * @return
     */
    List<Map<String, String>> getConnectorsRepository() {
        String url = "$fusionBase/connectors/repository"
        log.info "getting all ...available... connectors"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }


    /**
     * Get a list of connectors that are already installed in the Fusion deployment
     * @return
     */
    List<Map<String, String>> getConnectorsInstalled() {
        HttpResponse<String> response = null
        String url = null
        url = "$fusionBase/api/connectors/plugins"
        log.info "getting all installed connectors"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        return responseWrapper.parsedInfo
    }


    /**
     * scan the source/exported objects info, and find all the connector plugins
     * <br>
     * <i>Note:(making this a simple function to be able to expand/override in the future</i>
     * @param objectsMap
     * @return
     */
    List<Map<String, Object>> getConnectorsUsed(Map objectsMap) {
        List<Map<String, Object>> dataSources = objectsMap['dataSources']
    }


    /**
     * add the connector from the F5+ repository to destination fusion
     * @param connectorId
     * @return
     */
    FusionResponseWrapper installConnectorFromRepository(String connectorId) {
        FusionResponseWrapper responseWrapper = null
        try {
            String url = "$fusionBase/api/connectors/plugins?id=${connectorId}"
            log.info "\t install connector plugin ($connectorId) from repository with url: $url"
            HttpRequest request = buildPostRequest(url, jsonToIndex)
            responseWrapper = sendFusionRequest(request)
        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating datasources($connectorId): $iae"
        }

        return responseWrapper
    }


    /**
     * get a list of datasources defined in the fusion cluster
     * @return
     */
    List<Map<String, Object>> getDataSources() {
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


    FusionResponseWrapper createDataSource(Map<String, Object> map, String app) {
        String jsonToIndex = new JsonBuilder(map).toString()
        String url = "$fusionBase/api/apps/${app}/connectors/datasources"
        log.info "\t Create DATASOURCES (${map.id}) type(${map.connector}) url: $url -- Json text size::\t ${jsonToIndex.size()}"
        HttpRequest request = buildPostRequest(url, jsonToIndex)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
    }

    List<Map<String, String>> getLinks() {
        String url = "$fusionBase/api/links"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            return fusionResponseWrapper.parsedInfo
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return null     // todo -- consider refactoring to return FusionResponseWrapper like other calls...
    }

    FusionResponseWrapper createLink(Map<String, Object> map, String app) {
        HttpResponse<String> response = null
        JsonBuilder jsonBuilder = new JsonBuilder(map)
        String jsonToIndex = jsonBuilder.toString()
        String url = "$fusionBase/api/links"
        String name = "sub:${map.subject}    obj:${map.object}    type:${map.linkType}"
        log.info "\t Create LINK ($name)  url: $url -- Json text size::\t ${jsonToIndex.size()}"
        HttpRequest request = buildPutRequest(url, jsonToIndex)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)

        return fusionResponseWrapper
    }


    /**
     * Scan source objects map, load existing connector plugins from Destination fusion, and if F5, ask about the repository
     * add any plugins needed based on those in source DataSoources/connectors-used
     * @param srcFusionObjects
     * @return
     */
    Map<String, List<Object>> addConnectorPluginsIfMissing(Map srcFusionObjects) {
        log.info "Installed Connectors: ${connectorsInstalled.size()}"
        Map connectorStatus = [used: [], installed: [], repository: [], ready: [], added: [], missing: [], remapped: []]
        List<Map<String, String>> connectorsInstalled = getConnectorsInstalled()
        log.debug "\t\t Connectors installed: $connectorsInstalled"
        connectorStatus.installed = connectorsInstalled

        List<Map<String, String>> connectorsRepository = getConnectorsRepository()
        connectorStatus.repository = connectorsRepository
        log.info "Repository Connectors: ${connectorsRepository.size()}"

        List<Map<String, Object>> connectorsUsed = getConnectorsUsed(srcFusionObjects)
        connectorStatus.used = connectorsUsed
        log.info "\t\tUsed Connectors: ${connectorsUsed?.size()}"
        Map usedConnectorsByType = connectorsUsed?.groupBy { it.type }
        usedConnectorsByType?.each { String type, List<Map<String, Object>> usedDataSources ->
            Map usedDatasourcesByConnector = usedDataSources.groupBy { it.connector }      // have to handle fubar mixup between 'connector' value from export and 'type' value in api call
            if (usedDatasourcesByConnector.size() > 1) {
                log.info "More than one source Datasource connector for type ($type) -- used connectors: $usedConnectorsByType???"
            }
            log.debug "\t\tUsed: '$type' with (${usedDataSources.size()}) datasources "
            List<Map<String, String>> installedMatches = connectorsInstalled.findAll {
                it.type.containsIgnoreCase(type)
            }
            if (installedMatches) {
                if (installedMatches.size() > 1) {
                    log.warn "More than one ready plugin match?? ($installedMatches)"
                    // -- picking the first (${installedMatches[0]})"
                }
                Set dsKeys = usedDatasourcesByConnector.keySet()
                String usedDsMatch = null
                if (usedDatasourcesByConnector.size() == 1) {
                    usedDsMatch = dsKeys[0]
                    log.debug "one datasource type: ${usedDsMatch}"
                    if (usedDsMatch.equalsIgnoreCase(installedMatches.type)) {
                        log.info "\t\tdirect match on type: ready:$installedMatches --  dskeys:$dsKeys"
                        connectorStatus.ready << installedMatches
                    } else {
                        log.warn "no direct match, what to do here?? ready:$installedMatches --  dskeys:$dsKeys"
                        connectorStatus.remapped << [source: installedMatches, destination: usedDsMatch]
                    }
                } else {
                    log.warn "More than one datasource plugin type?? ready:$installedMatches --  dskeys:$dsKeys"
                }
                Map<String, String> match = installedMatches[0]
                connectorStatus.ready << match
                String readyType = match.type
                if (usedDsMatch.equalsIgnoreCase(readyType)) {
                    log.info "\t\tFound connector used: ($type) is already installed and has (${usedDataSources.size()}) datasources using it"
                } else {
                    log.warn "\t\tFound connector used: ($usedDsMatch) but it seems to map to new type: ($readyType), watch datasource mapping/change..."
                }
            } else {
                def repo = connectorsRepository.find { it.id == type }
                if (repo) {
                    log.warn "Found connector used: ($type) with (${usedDataSources.size()}) datasources NOT installed, but IS IN the repo, so install it..."
                    FusionResponseWrapper responseWrapper = installConnectorFromRepository(type)
                    if (responseWrapper.wasSuccess()) {
                        connectorStatus.added << repo
                    } else {
                        connectorStatus.missing << repo
                        log.info "Could not add repository plugin?? $repo\n\t\tresponse wrapper: $responseWrapper"
                    }
                    log.info "Result of install connector: '$type': $responseWrapper"
                } else {
                    connectorStatus.missing.addAll(usedDataSources)
                    log.warn "Connector used: ($type) with (${usedDataSources.size()}) datasources NOT INSTALLED, AND NOT in REPO!!! Panic?"
                }
            }
        }
        return connectorStatus
    }

    List<FusionResponseWrapper> addCollectionsIfMissing(String appName, Map srcFusionObjects) {
        List<FusionResponseWrapper> responses = []
        List<Map<String, Object>> existingCollections = getCollections(appName)

        // --------------- Create Collections ------------------
        srcFusionObjects['collections'].each { Map<String, Object> coll ->
            String newCollname = coll.id
            def existingColl = existingCollections.find { it.id == newCollname }
            if (existingColl) {
                log.info "\t\tSkipping existing collection ($newCollname)"
                log.debug "\t\tSkipped existing collection ($newCollname): $existingColl"
            } else {
                boolean defaultFeatures = false
                FusionResponseWrapper responseWrapper = createCollection(coll, appName, defaultFeatures)
                if (responseWrapper.wasSuccess()) {
                    log.info "Created Collection: ($coll)"
                } else {
                    log.warn "Had a problem creating collection: $coll??? Response wrapper: $responseWrapper"
                }
                responses << responseWrapper
            }
        }
        return responses
    }

    List<FusionResponseWrapper> addDatasourcesIfMissing(String appName, Map srcFusionObjects, def oldLinks) {
        List<FusionResponseWrapper> responses = []

        List<Map<String, Object>> dsExisting = getDataSources()
        // get map of existing links (faster lookups...?)
        srcFusionObjects['dataSources'].each { Map<String, Object> p ->
            String dsName = p.id
            def existingDS = dsExisting.find {
                it.id == dsName
            }
            if (existingDS) {
                log.info "\tSKIPPING existing datasource: $dsName"
            } else {
                log.info "CREATE datasource: $dsName "
                FusionResponseWrapper responseWrapper = createDataSource(p, appName)
                responses << responseWrapper

                if (responseWrapper.wasSuccess()) {
                    // get the relevant links for this datasource (typically 3)
                    def dsLinksImport = oldLinks.findAll { it.subject.startsWith("datasource:${dsName}") }
                    dsLinksImport.each { Map m ->
                        String subject = m.subject
//                        if (existingLinksMap[subject]) {
                        if (dsExisting[subject]) {
                            log.info "\t\tSkipping existing data source link: $m"
                        } else {
                            FusionResponseWrapper responseWrapper2 = createLink(m, appName)
                            log.info "\t\tCreate link result code: $responseWrapper2"
                        }
                    }
                    log.info "\t\tDatasource links to import (or skip): ${dsLinksImport}"
                } else {
                    log.info "\t\tSkipping datasource links since we did not create "
                }
            }
        }
        return responses
    }

    def getConfigSets() {
        String url = "$fusionBase/api/solrAdmin/default/admin/configs?action=LIST"
        // {{furl}}/api/solrAdmin/default/admin/configs?action=LIST
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            List<String> configSets = fusionResponseWrapper.parsedMap.configSets
            configSets.each {
                log.debug "get configset: $it"

            }
            log.debug "Successfully got configsets (${configSets.size()}"
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return fusionResponseWrapper.parsedMap.configSets
        // todo -- consider refactoring to return FusionResponseWrapper like other calls...
    }

    def getConfigSet() {
        String url = "$fusionBase/api/solrAdmin/default/admin/configs?action=LIST&recursive=true"
        // {{furl}}/api/solrAdmin/default/admin/configs?action=LIST
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            List<String> configSets = fusionResponseWrapper.parsedMap.configSets
            configSets.each {
                log.debug "get configset: $it"

            }
            log.debug "Successfully got configsets (${configSets.size()}"
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return fusionResponseWrapper.parsedMap.configSets
        // todo -- consider refactoring to return FusionResponseWrapper like other calls...
    }

    def getObjects(String params = '', HttpResponse.BodyHandler bodyHandler) {
        String url = "$fusionBase/api/objects/export${params}"
        // {{furl}}/api/solrAdmin/default/admin/configs?action=LIST
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request, bodyHandler)
        if (fusionResponseWrapper.wasSuccess()) {
            List<String> configSets = fusionResponseWrapper.parsedMap.configSets
            configSets.each {
                log.debug "get configset: $it"

            }
            log.debug "Successfully got configsets (${configSets.size()}"
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return fusionResponseWrapper.parsedMap.configSets
        // todo -- consider refactoring to return FusionResponseWrapper like other calls...
    }

    /**
     * get a list of solr config 'things' -- placeholders that then need to be loaded individually
     * note: {{furl}}/api/solrAdmin/default/admin/configs?action=LIST&recursive=true
     * @param collectionName
     * @param configObject
     * @param params
     * @param handler
     * @return list of entries in zookeeper (need to load each one, and possibly recursively for something like `lang`
     */
    List<Map<String, Object>> getSolrConfigList(String collectionName, Map<String, String> params = [expand: 'true', 'recursive': 'true']) {
        String url = "$fusionBase/api/collections/${collectionName}/solr-config"
        if (params) {
            url = "$url?${params.collect { "${it.key}=${it.value}" }.join('&')}"
        }

        HttpRequest request = buildGetRequest(url)
        HttpResponse.BodyHandler handler = HttpResponse.BodyHandlers.ofString()
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request, handler)
        if (fusionResponseWrapper.wasSuccess()) {
            List<Map<String, Object>> solrConfigs = fusionResponseWrapper.parsedList
            def flattened = solrConfigs.flatten()
            flattened.each {Map<String, Object> m ->
                SolrConfigThing thing = new SolrConfigThing(m)
                m.thing = thing
                if(thing.value){
                    log.debug "what now? $thing"
                }
            }
            for (int i = 0; i < solrConfigs.size(); i++) {
                Map configEntry = solrConfigs[i]
                log.debug "$i) Solr config: $configEntry"
                String entryName = configEntry.name
                log.info "\t\tProcess solr config entry: $entryName"
                if (configEntry.value) {
                    String value = configEntry.value
                    def decodedValue = value.decodeBase64()
                }

                def foo = getSolrConfigThing(collectionName, entryName)
                boolean isdir = configEntry.isDir
                log.debug "\t\tsub item: $configEntry"
            }
            log.debug "Successfully got configsets (${solrConfigs.size()}"
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return fusionResponseWrapper.parsedList

    }

    /**
     * get a specific node in the solr  config tree (zknode thing) -- this is as opposed to a directory/tree (such as `lang`
     *
     * @param collectionName
     * @param configThingPath
     * @param params
     * @return
     */
    SolrConfigThing getSolrConfigThing(String collectionName, String configThingPath, Map<String, String> params = [expand: 'true', 'recursive': 'true']) {
        String url = "$fusionBase/api/collections/${collectionName}/solr-config/${configThingPath}"
        if (params) {
            url = "$url?${params.collect { "${it.key}=${it.value}" }.join('&')}"
        }
        SolrConfigThing thing = null
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            thing = new SolrConfigThing(fusionResponseWrapper.parsedMap)
            log.debug "Successfully got thing: (${thing}"
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return thing

    }

    /**
     * use fusion passthrough api to get "combined" schema from Solr (includes overlay.json....)
     * url format: {{furl}}/api/solr/{{coll}}/schema
     * @param collectionName
     * @param params
     * @return
     */
    def getSolrSchema(String collectionName, Map<String, String> params = [action: 'LIST', wt: 'json']) {
//        String url = "$fusionBase/api/objects/export?collection.ids=${collectionName}"
        String url = "$fusionBase/api/solr/${collectionName}/schema"
        if (params) {
            url = "$url?${params.collect { "${it.key}=${it.value}" }.join('&')}"
        }
        log.info "get solr (combined) schema via fusion pass-through api: $url"
        HttpRequest request = buildGetRequest(url)
        HttpResponse.BodyHandler handler = HttpResponse.BodyHandlers.ofString()
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request, handler)
        Map schema = null
        if (fusionResponseWrapper.wasSuccess()) {
            schema = fusionResponseWrapper.parsedMap.schema
            log.debug "Successfully got schema (${schema}"
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return schema

    }

}
