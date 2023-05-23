package com.lucidworks.ps.clients

import com.lucidworks.ps.Helper
import com.lucidworks.ps.model.fusion.Application
import com.lucidworks.ps.solr.SolrConfigThing
import groovy.cli.picocli.OptionAccessor
import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.http.auth.AuthenticationException

//import org.apache.commons.io.IOUtils

import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.apache.tools.zip.ZipEntry

//import org.apache.commons.compress.utils.IOUtils

import org.apache.tools.zip.ZipOutputStream

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandler
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.Duration
import java.util.regex.Pattern

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
    static Logger log = Logger.getLogger(this.class.name)
    Application application
    String user = null
    String password = null
    String fusionBase = null
    String appName = null
    HttpClient httpClient = null
    String versionString = 'n.a.'
    int majorVersion = -1

    private String sessionCookie = null
    File objectsJsonFile = null
    File exportDirectory = null
    String objectsGroup = null
    Map introspectInfo = null
    Map<String, Object> apiInfo = null
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
    FusionClient(String baseUrl, String user, String pass, String app = null) {
        this(new URL(baseUrl), user, pass, app)
    }

    FusionClient() {
        def envVars = System.getenv()
        this.user = envVars.fuser
        this.password = envVars.fpass

        String furl = envVars.furl
        if(furl && this.user && this.password) {
            if (furl.endsWith('/')) {
                furl = furl[0..-2]
                log.info "\t\tstripping trailing slash from baseUrl: '$furl' => '${fusionBase}'"
            }
            this.fusionBase = new URL(furl)

            log.info "FusionClient constructor from System.getenv(): fusionBase:($fusionBase), user:($user), password?(${password > ''}) )..."

            if (envVars.fapp) {
                appName = envVars.fapp
                log.info "\t\tSetting app: $appName"
            }

            if(envVars.exportDir){
                String dir =  envVars.exportDir
                exportDirectory = new File(dir)
                if(exportDirectory.exists()){
                    log.debug "export dir exists: $exportDirectory"
                } else {
                    Helper.getOrMakeDirectory(exportDirectory)
                    log.info "export dir ($exportDirectory) did not exist, does it now? ${exportDirectory.exists()}"
                }

                log.info "\t\tsetting export dir from env variable (exportDir): $dir -> $exportDirectory"
            }
            httpClient = buildClient(fusionBase, user, password)
        } else {
            log.warn "Missing critical constructor information: url:(${this.fusionBase}), user:(${this.user}), password:(${password > ''}) -- likely there will be problems...?"
        }
    }


    /**
     * typical constructor with information on how to connect to a fusion (F4 or F5, possibly other versions) - proper Java URL argument version
     * @param baseUrl e.g. http://yourdemocluster.lucidworks.com:6764   (trailing slash will be removed if included)
     * @param user
     * @param pass
     */
    FusionClient(URL baseUrl, String user, String pass, String app = null) {
        this.fusionBase = baseUrl
        this.user = user
        this.password = pass
        if (app) {
            log.debug "Setting app: $app"
            appName = app
        }

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
        String fbase = options.hasOption('fusionUrl') ? options.fusionUrl : null
        if (fbase) {
            if (fbase.endsWith('/')) {
                fusionBase = fbase[0..-2]
                log.info "Remove trailing slash from options.f (fusionBase: ${fbase}) -> $fusionBase"
            } else {
                fusionBase = fbase
                log.debug "Standard fusion base: $fusionBase"
            }
        } else {
            log.info "No fusion base given in options, assuming config file will have it if needed...?"
        }
        user = options.u                            // fusion username for destination of new/migrated app
        password = options.p
        if (options.appName) {
            appName = options.appName
            log.info "given appname arg: $appName"
        }
        String srcObjectJsonPath = options.s
        if (srcObjectJsonPath && srcObjectJsonPath != 'false') {
            // todo -- add logic to read configs from live fusion (needs source: url, pass, furl, appname)
            File srcFile = new File(srcObjectJsonPath)
            if (srcFile.isFile() && srcFile.exists()) {
                objectsJsonFile = srcFile
                log.debug "found a source file: ${srcFile.absolutePath}, loading application from it...."
                application = new Application(objectsJsonFile)
                log.info "Source file exists: ${objectsJsonFile.absolutePath}"
            } else {
                log.warn "Source file: ${srcFile.absolutePath} does NOT EXIST..."
                throw new IllegalArgumentException("Missing source file, param: $srcObjectJsonPath, but not a readable file (${srcFile.absolutePath})")
            }
        } else {
            log.info "No source file arg given (not reading app export, nor objects.json)... "
        }

        // some optional label to group things by (client name, or app name)
        objectsGroup = options.g

        // if we have an export directory, make sure if exists, and save it in class prop
        if (options.exportDir) {
            exportDirectory = Helper.getOrMakeDirectory(options.exportDir)
        }

        log.info """Initialized FusionClient with command line options:
            Fusion Url:     ${fusionBase}
            App Name:       $appName
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
        log.debug "\t\t${this.class.simpleName} getClient(baseurl: $baseUrl, user:$user, password <hidden>...)   [should only need to call this once...]"
        HttpClient client = HttpClient.newBuilder()
                .cookieHandler(new CookieManager())
                .version(HttpClient.Version.HTTP_1_1)
                .build()

        // groovy string template for speed, rather than json builder
        String authJson = """{"username":"$user", "password":"$pass"}"""

        // start a session for this client to use
        // todo -- check for stale cookies, etc
        String urlSession = "${baseUrl}/api/session"
        log.debug "\t\tInitializing Fusion client session via POST to session url: $urlSession"

        HttpRequest request = buildPostRequest(urlSession, authJson)
        BodyHandler<String> handler = HttpResponse.BodyHandlers.ofString()
        HttpResponse<String> response = client.send(request, handler)
        FusionResponseWrapper fusionResponseWrapper = new FusionResponseWrapper(request, response, handler)
        responses << fusionResponseWrapper
        if (response.statusCode() < 300) {
            log.debug("\t\tResponse status: " + response.statusCode())
            sessionCookie = response.headers().firstValue("set-cookie")
            cookieMS = System.currentTimeMillis()
            Date ts = new Date(cookieMS)
            log.info("FusionClient.buildClient() successfully created session (url:$urlSession) with cookie set/refreshed at (${ts}) -- user:($user)")
            log.debug("\t\tcookie: ${this.sessionCookie} set/refreshed at timestamp: $cookieMS (${ts}) -- user:($user)")
        } else if (response.statusCode() == 401) {
            log.error "Error code 401 creating client (incorrect auth??), Status Code: ${response.statusCode()} -- body: ${response.body()}"
            throw new AuthenticationException("Could not create Fusion Client (${response.body()})")

        } else {
            log.error "Bad status code creating client -- Status Code: ${response.statusCode()} -- body: ${response.body()}"
            throw new IllegalArgumentException("Could not create Fusion Client -- response code:(${response.statusCode()}) -- body:(${response.body()})")
        }

        this.httpClient = client
        Map info = getFusionInformation()
        log.debug "Fusion information: $info"

        // todo -- revisit returning the client, versus setting it above; doing both because I feel getting fusion info/version is part of building the client and general awareness (different urls between different fusions
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
     * @param payload
     * @return
     */
    HttpRequest buildPutRequest(String url, def payload) {
        String json = null
        HttpRequest request
        if (payload instanceof String) {
            log.debug "Got a string, no json builder needed"
            json = payload
            request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .PUT(HttpRequest.BodyPublishers.ofString(json))
                    .build()
        } else if (payload instanceof File || payload instanceof Path) {
            Path uploadPath = payload
            request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
//                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .PUT(HttpRequest.BodyPublishers.ofFile(uploadPath))
                    .build()
            log.info "Upload file: ${uploadPath.toAbsolutePath()}"

        } else if (payload instanceof Map || payload instanceof Collection) {
            JsonBuilder jsonBuilder = new JsonBuilder(payload)
            json = jsonBuilder.toString()
            log.debug "converted object($payload) to Json string (${json}) for indexing"
            request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .PUT(HttpRequest.BodyPublishers.ofString(json))
                    .build()
        }
        return request
    }

    /**
     * encapsulate POST (create) requests
     * @param url
     * @param payload
     * @return
     */
    HttpRequest buildPostRequest(String url, def payload) {
        String json = null
        HttpRequest request
        if (payload instanceof Collection || payload instanceof Map) {
            JsonBuilder jsonBuilder = new JsonBuilder(map)
            json = jsonBuilder.toString()
            request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(3))
                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build()
            log.debug "converted object($payload) to Json string (${json}) for indexing"
        } else if (payload instanceof Path) {
            Path uploadPath = payload
            log.info "Upload file: ${uploadPath.toAbsolutePath()}"
            request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(3))
//                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .PUT(HttpRequest.BodyPublishers.ofFile(uploadPath))
                    .build()

        } else {
            json = payload
            request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMinutes(3))
                    .header("Content-Type", "application/json")
                    .setHeader("User-Agent", "Java 11+ HttpClient Bot") // add request header
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build()
        }

        return request
    }


    /**
     * Get json "list" of applications defined in the cluster. See also: export
     * @return
     */
    List<Map<String, Object>> getApplications() {
        HttpResponse response = null
        String url = "$fusionBase/api/apps"
        log.info "\t\tGet Fusion applications summary details via url: $url"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        List<Map<String, Object>> applications = fusionResponseWrapper.parsedInfo
        return applications
    }

    def getApplication(String appId) {
        HttpResponse response = null
        String url = "$fusionBase/api/apps/${appId}"
        log.info "\t\tGet Fusion application ($appId) details via url: $url"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        Map<String, Object> application = fusionResponseWrapper.parsedInfo
        return application
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
     * @return map of string and values for Fusion info
     */
    Map<String, Object> getFusionInformation() {
        String url = "$fusionBase/api"
        JsonSlurper jsonSlurper = new JsonSlurper()
        HttpRequest request = buildGetRequest(url)

        // todo -- refactor to call client.send()
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        int rc = response.statusCode()
        if (rc >= 200 && rc < 300) {
            log.debug("Response from getFusionInformation() ${response.statusCode()}")
            apiInfo = jsonSlurper.parseText(response.body())
            log.debug "Fusion API info: $apiInfo"
            versionString = apiInfo.version
            if (versionString) {
                if (versionString == 'local') {
                    log.warn "\t\tVersion (${versionString}) unclear, assuming 4.x..."
                    // todo -- refactor...? leaving as a process, rather than a more functional programming (more proper) style...
                    getApolloConfigurations()
                    log.debug "Major version: ${majorVersion}"
//                    majorVersion = 4
                } else if (versionString.startsWith('5')) {
                    majorVersion = 5
                    log.info "\t\tFound version string: '${versionString}' which looks like major version (${majorVersion})"
                }
            } else {
                log.warn "No version entry found from API call... WTH...? what version is this...? API info returned: \n$apiInfo"
            }
        } else {
            log.warn "Could not get valid API information: ${this.fusionBase}"
        }

        // todo refactor to separate call
        URI uri = (URI.create("${this.fusionBase}/api/introspect"))
        var reqIntro = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .timeout(Duration.ofMinutes(2))
                .setHeader("User-Agent", "Java 11+ HttpClient FusionPS Bot") // add request header
//                .header("Content-Type", "application/json")
                .build()

        HttpResponse<String> respIntro = httpClient.send(reqIntro, HttpResponse.BodyHandlers.ofString())
        rc = respIntro.statusCode()
        if (rc >= 200 && rc < 300) {
            log.info("\t\tResponse introspection result code: ${respIntro.statusCode()}")
            introspectInfo = jsonSlurper.parseText(respIntro.body())
            log.debug "\t\tFusion Introspection info (keys only): ${introspectInfo.keySet()}"
        } else {
            log.warn "Could not get valid Introspection information: ${this.fusionBase}"
        }
        return apiInfo
    }


    /**
     * fallback for Fusion 4 (localhost?) call to get version info...?
     */
    def getApolloConfigurations() {
        String urlBase = 'api/apollo/configurations'
        String url = "$fusionBase/${urlBase}"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        HttpResponse response = fusionResponseWrapper.httpResponse
        if (fusionResponseWrapper.wasSuccess()) {
            Map info = fusionResponseWrapper.parsedInfo
            log.debug "Fall-back call to url base: ${urlBase} to get version information:${info.keySet()}"
            // todo -- find something better than info.keySet() for debug message
            versionString = info['app.version']
            majorVersion = getFusionMajorVersion(versionString)
        }
    }

    /**
     * get fusion version, likely part of setting the rest api structure in future calls
     */
/*    def getFusionVersion() {
        // todo :: figure out what is a good way to get relevant version info, the calls in getFusionVersion don't have what I expect (version is 'local' for localhost 4.2.6)
        return "more to come RSN..."
    }*/

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
    Map<String, Object> query(String app, String collection, String queryPipeline, Map<String, Object> qparams = [:], String reqHandler = 'select') {

        URIBuilder ub = new URIBuilder("${this.fusionBase}/api/apps/${app}/query-pipelines/${queryPipeline}/collections/${collection}/${reqHandler}")
//        URI uri = URI.create("${this.fusionBase}/api/apps/${app}/query-pipelines/${queryPipeline}/collections/${collection}/${reqHandler}")
        qparams.each { String name, def val ->
            log.debug "\t\t Adding param: $name => $val"
            ub.addParameter(name, "${val}")         // needs to be a string?
        }
//        ub.addParameters(qparams)
        URI uri = URI.create(ub.toString())
        log.info "\t\tprepared uri: $uri"

        // todo -- refactor buildGetRequest to accept uri?
        HttpRequest request = buildGetRequest(uri.toString())
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            def info = fusionResponseWrapper.parsedInfo
            log.debug "\t\tNum found: ${info.httpResponse?.numFound}"       // remove me???
        }
        return fusionResponseWrapper.parsedMap
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
     * @param bodyHandler how to retrieve response body -- typically stream hanlder
     * @return FRW with parsedInfo holding the exported 'stuff' -- currently a parsed ZipFile, but this should be refactored?
     */
    FusionResponseWrapper exportFusionObjects(String exportParams, def bodyHandler = HttpResponse.BodyHandlers.ofInputStream()) {
//    HttpResponse<Path> exportFusionObjects(String exportParams, Path outputPath) {
        String url = "$fusionBase/api/objects/export?${exportParams}"
        log.info "\t\tExport Fusion objects url: $url"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request, bodyHandler)

        return fusionResponseWrapper
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
            log.info "Created app? $jsonToIndex"
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
        FusionResponseWrapper fusionResponse = new FusionResponseWrapper(request, response, bodyHandler)

        responses << fusionResponse // add this response to the client's collection of responses
        if (fusionResponse.wasSuccess()) {
            log.debug "successful request/response: $fusionResponse"
            if (exportDirectory) {
                log.info "\t\tExport (sucessful) request/response to folder: $exportDirectory..."
            }
        } else {
            log.warn "UNSUCCESSFUL request/response: $fusionResponse"
            String body = response.body()
            if (Helper.isJson(body)) {
                def json = new JsonSlurper().parseText(body)
                if (fusionResponse.statusMessage) {
                    fusionResponse.statusMessage += "\n" + json
                } else {
                    if (json.type == 'RESTError') {
                        fusionResponse.statusMessage = "${json.type} (${json.httpStatusCode}): ${json.message} :: ${json.details}"

                    } else {
                        fusionResponse.statusMessage = json.toString()
                    }
                }
                if (exportDirectory) {
                    log.info "\t\tExport (failed) request/response to folder: $exportDirectory..."
                }

            } else {
                log.warn "Response body was not Json!! + $body"
            }
        }
        return fusionResponse
    }

    /**
     * Get a list of existing collection definitions (Json Maps) within an optional application
     * @param appId optional filter to collections in a given app
     * @param idFilter optional String or Pattern filter
     * @return
     */
    List<Map<String, Object>> getCollectionDefinitions(String appId = null, def idFilter = null) {
        String url = appId ? "$fusionBase/api/apps/${appId}/collections" : "$fusionBase/api/collections"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
        List collections = responseWrapper.parsedList
        if (idFilter) {
            log.debug "Filtering returned Collections by: ($idFilter)..."
            collections = responseWrapper.parsedList.findAll {
                if (idFilter instanceof Pattern) {
                    it.id ==~ idFilter
                } else {
                    it.id == idFilter
                }
            }

        }
        def collIdList = collections.collect { it.id }
        log.info "Found (${collections.size()}) collections for (appId: $appId)  ::  (idFilter: $idFilter) --> url: $url "
        log.debug "Found collection ids: $collIdList"
        return collections
    }

    /**
     * get the api collections output (as opposed to the objects export configsets, called the same thing...)
     * @param appId optional filter of app id (AppName is often the same, but not always, we use the id here)
     * @param idFilter optional id String (exact match) or id @Pattern which will do a regex match
     * @return the Json Map of the collection definition
     * @throws IllegalArgumentException
     */
    Map<String, Object> getCollectionDefinition(String appId, def idFilter) throws IllegalArgumentException {
        log.info "List collection:(idFilter: $idFilter) for app $appName  (call getCollections)... "
        List collections = getCollectionDefinitions(appId, idFilter)
        if (collections) {
            Map collection = collections[0]
            if (collections.size() == 1) {
                log.info "\t\tFound a single collection (as expected), return it (as a map) rather than the single-element list... (${collection.id}}"
            } else {
                String msg = "More than one collection returned (${collections.size()} :: ${collections.collect { it.id }})"
                log.error(msg)
                throw new IllegalArgumentException(msg)

            }
            return collection
        } else {
            log.warn "Could not find collection ($idFilter) in App ($appId), returning null..."
            return null
        }
    }


    /**
     * convenience wrapper to get configset(s?) zip file
     * @param configsetId
     * @param bodyHandler
     */
    def exportSolrConfigset(String configsetId, bodyHandler) {
        def configsetZip = exportFusionObjects("collection.ids=$configsetId", bodyHandler)
    }


    /**
     * Create a collection in the given fusion application.
     * Note: the defaultFeatures controls things like signals sidecar collections, query rewrite, etc.
     * @param collectionMap
     * @param appName
     * @param defaultFeatures (set to false, since user is likely to create an app, which would have primary coll with default features) --here we assume supporting collections
     * @return
     */
    FusionResponseWrapper createCollection(String collectionName, Map<String, Object> collectionMap, String appName, boolean defaultFeatures = false) {
        FusionResponseWrapper responseWrapper = null
        String collName = collectionMap.id
        JsonBuilder jsonBuilder = new JsonBuilder(collectionMap)
        String jsonToIndex = jsonBuilder.toString()
        log.debug "Revisit collection creation process, this call is doing a naive/vanilla collection creation: $jsonToIndex"
        try {
            String url = "$fusionBase/api/apps/${appName}/collections"      // todo: add defaultFeatures?
//            String url = "$fusionBase/api/apps/${appName}/collections/${collectionName}"      // todo: add defaultFeatures?
            log.info "\t Create COLLECTION ($collName) url: $url -- Json text::\t ${jsonToIndex}"
//            HttpRequest request = buildPutRequest(url, jsonToIndex)
            HttpRequest request = buildPostRequest(url, jsonToIndex)
            responseWrapper = sendFusionRequest(request)
            log.debug "\t\tcreate collection respose: $responseWrapper"
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
    Object getIndexPipelines(String app = null, def idFilter = null) {
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
        List<Map<String, Object>> pipelines = responseWrapper.parsedList
        if (idFilter) {
            List filteredList = pipelines.findAll { it.id ==~ idFilter }
            return filteredList
        }

        return responseWrapper.parsedInfo
    }

    /**
     * get single pipeline definition
     * @param pipelineId
     * @param app
     * @return
     */
    Map<String, Object> getIndexPipeline(String pipelineId, String app = null) {
        HttpResponse<String> response = null
        String url = null
        if (app) {
            url = "$fusionBase/api/apps/${app}/index-pipelines/$pipelineId"
            log.info "${app} app given, getting query pipeline: $pipelineId"

        } else {
            url = "$fusionBase/api/index-pipelines/$pipelineId"
            log.info "no app given, getting query 'any' pipeline with id: $pipelineId"
        }
        log.info "\t get index pipeline ($pipelineId) for url: $url "
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
        Map idxpDef = responseWrapper.parsedMap
        return idxpDef
    }


    /**
     * get a list index pipelines already defined/existing in the (destination/F5) Fusion deployment
     * @param app
     * @return
     */
    List<Map<String, Object>> getQueryPipelines(String app = null, def idFilter = null) {
        HttpResponse<String> response = null
        String url = null
        if (app) {
            url = "$fusionBase/api/apps/${app}/query-pipelines"
        } else {
            url = "$fusionBase/api/query-pipelines"
            log.info "No app given, getting all query pipelines..."
        }
        log.info "\t list query pipelines for url: $url "
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
        List<Map<String, Object>> qrypList = responseWrapper.parsedList
        if (idFilter) {
            List<Map<String, Object>> filteredList = qrypList.findAll { it.id ==~ idFilter }
            log.info "return filtered ($idFilter) list (${filteredList.size()}) down from total (${qrypList.size()})"
            return filteredList
        } else {
            return qrypList
        }
    }

    Map<String, Object> getQueryPipeline(String qryPipelineId, String app = null) {
        HttpResponse<String> response = null
        String url = null
        if (app) {
            url = "$fusionBase/api/apps/${app}/query-pipelines/$qryPipelineId"
        } else {
            url = "$fusionBase/api/query-pipelines/$qryPipelineId"
            log.debug "No app given, getting query pipeline without app context (not a problem, just FYI)"
        }
        log.info "\t list query pipelines for url: $url "
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
        Map<String, Object> pipeline = responseWrapper.parsedInfo
        return pipeline
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

            response = responseWrapper.httpResponse

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating INDEX PIPELINE($name): $iae"
        }

        return response
    }

    /**
     * create a query pipeline by converting map object to json payload including destination application to connect it to
     * @param map
     * @param app
     * @return
     */
    def createQueryPipeline(Map<String, Object> map, String qrypJson) {
        HttpResponse<String> response = null
        JsonBuilder jsonBuilder = new JsonBuilder(map)
        String name = map.id
        String jsonToQuery = jsonBuilder.toString()
        try {
            String url = "$fusionBase/api/apps/${app}/query-pipelines"
            log.info "\t Create QUERY PIPELINE ($name) url: $url -- Json text size::\t ${jsonToQuery.size()}"
            HttpRequest request = buildPostRequest(url, jsonToQuery)

            FusionResponseWrapper responseWrapper = sendFusionRequest(request)

            response = responseWrapper.httpResponse

        } catch (IllegalArgumentException iae) {
            log.error "IllegalArgumentException creating QUERY PIPELINE($name): $iae"
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
        log.debug "getting all ...available... connectors"
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
        log.debug "getting all installed connectors"
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


//    /**
//     * get a list of datasources defined in the fusion cluster
//     * @return
//     */
//    List<Map<String, Object>> getDataSources() {
//        HttpResponse<String> response = null
//        String url = "$fusionBase/api/connectors/datasources"
//        log.info "No app given, getting all datasources"
//        HttpRequest request = buildGetRequest(url)
//        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
//
//        return responseWrapper.parsedInfo
//    }


    /**
     * get list of datasources with optional app name filter,
     * @param appName
     * @return
     */
    List<Map<String, Object>> getDataSources(String appName = null, def idFilter = null) {
        HttpResponse<String> response = null
        String url = null
        if (appName) {
            url = "$fusionBase/api/apps/${appName}/connectors/datasources"
            log.info "app (${appName}) given, getting those datasources, url: $url"
        } else {
            url = "$fusionBase/api/connectors/datasources"
            log.info "NO APP given, getting all datasources, url: $url"
        }
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper responseWrapper = sendFusionRequest(request)

        List dsDefs = null
        if (idFilter) {
            dsDefs = responseWrapper.parsedList.findAll {
                if (idFilter instanceof Pattern) {
                    it.id ==~ idFilter
                } else {
                    it.id == idFilter
                }
            }
        } else {
            dsDefs = responseWrapper.parsedList
        }
        return dsDefs
    }

//    Map<String, Object> getDataSource(String datasourceId) {
//        HttpResponse<String> response = null
//        String url = "$fusionBase/api/apps/${appName}/connectors/datasources/$datasourceId"
//        log.info "getting datasource (${datasourceId}), url: $url"
//        HttpRequest request = buildGetRequest(url)
//        FusionResponseWrapper responseWrapper = sendFusionRequest(request)
//        return responseWrapper.parsedMap
//    }

    /**
     * create fusion datasource: $fusionBase/api/apps/${app}/connectors/datasources
     * @param map
     * @param app
     * @return
     */
    FusionResponseWrapper createDataSource(Map<String, Object> map, String app) {
        String jsonToIndex = new JsonBuilder(map).toString()
        String url = "$fusionBase/api/apps/${app}/connectors/datasources"
        log.debug "\t\t Create DATASOURCES (${map.id}) type(${map.connector}) url: $url -- Json text size::\t ${jsonToIndex.size()}"
        HttpRequest request = buildPostRequest(url, jsonToIndex)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
    }

    /**
     * Get existing links in Fusion deployment (api call)
     * "$fusionBase/api/links"
     * @return
     */
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

    /**
     * Get existing links in Fusion deployment (api call)
     * "$fusionBase/api/links"
     * @return
     */
    List<Map<String, String>> getJobs(String app = '', String jobType = '') {
        def result = null
        String url = "$fusionBase/api/"
        if (app) url += "apps/${app}"
        if (jobType) {
            url += "/jobs?type=${jobType}"
        } else {
            url += "/jobs"
        }
        log.info "\t\tget jobs url (with app/jobType??): $url"

        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            result = fusionResponseWrapper.parsedInfo
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return result     // todo -- consider refactoring to return FusionResponseWrapper like other calls...
    }

    /**
     * get the ?map? of job details
     * (probably not a common call...?)
     * @param resource
     * @return parsed ?map?
     */
    Map<String, Object> getJob(String resource) {
        def result = null
        String url = "$fusionBase/api/jobs/$resource"
        log.info "\t\tget job ($resource) with url : $url"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            result = fusionResponseWrapper.parsedInfo
        } else {
            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
        }
        return result     // todo -- consider refactoring to return FusionResponseWrapper like other calls...
    }


    /**
     * find the subset of jobs with 'nextStartTime', and then get get the schedule information
     * @note this ia a bit of a hack, as Fusion does not seem to have a schedules list
     * @param jobs
     * @return
     */
    def getJobSchedules(List jobs = null) {
        List result = []
        if (!jobs) {
            log.info "No jobs given as param to getJobSchedules, so we will all the jobs..."
            jobs = getJobs()
        }
        def jobsWithSchedules = jobs.findAll { it.nextStartTime }
        // assume if the job api returns a nextStartTime in the props, this has a schedule (and we need to grab details for each)
        jobsWithSchedules.each {
            log.debug "get schedule info for job: ${it.resource}"
            def schedule = getJobSchedule(it.resource)
            result << schedule
        }
//        HttpRequest request = buildGetRequest(url)
//        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
//        if (fusionResponseWrapper.wasSuccess()) {
//            result = fusionResponseWrapper.parsedInfo
//        } else {
//            log.warn "Faled to get Object links!!?! Response wrapper: $fusionResponseWrapper"
//        }
        return result
    }

    def getJobSchedule(String jobResourceName) {
        String url = "$fusionBase/api/jobs/$jobResourceName/schedule"
        def result
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        if (fusionResponseWrapper.wasSuccess()) {
            result = fusionResponseWrapper.parsedInfo
            if (result) {
                log.debug "job schedule exists?? $result"
            } else {
                log.info "no job schedule exists?? $jobResourceName at api url: $url"
            }
        } else {
            log.warn "Failed to get Job ($jobResourceName) schedule! Response wrapper: $fusionResponseWrapper"
        }
        return result     // todo -- consider refactoring to return FusionResponseWrapper like other calls...

    }


    /**
     * Create fusion app object link
     * @param map
     * @param app
     * @return
     */
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
        log.info "Found ()${connectorsInstalled.size()}) Installed Connectors"
        Map connectorStatus = [used: [], installed: [], repository: [], ready: [], added: [], missing: [], remapped: []]
        List<Map<String, String>> connectorsInstalled = getConnectorsInstalled()
        log.debug "\t\t Found connectors installed: $connectorsInstalled"
        connectorStatus.installed = connectorsInstalled

        List<Map<String, String>> connectorsRepository = getConnectorsRepository()
        connectorStatus.repository = connectorsRepository
        log.info "Found Repository Connectors: ${connectorsRepository.size()}"

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
            List<Map<String, String>> installedMatches = connectorsInstalled.findAll { it.type.containsIgnoreCase(type) }

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
                        log.debug "\t\tFound ready connector:$installedMatches -- nothing more to do"
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
        List<Map<String, Object>> existingCollections = getCollectionDefinitions(appName)


        // --------------- Create Collections ------------------
        def collectionsToCreate = srcFusionObjects['collections'].findAll { it.type == 'DATA' }
        collectionsToCreate.each { Map<String, Object> coll ->
            String newCollname = coll.id
            def existingColl = existingCollections.find { it.id == newCollname }
            if (existingColl) {
                log.info "\t\tSkipping existing collection ($newCollname)"
                log.debug "\t\tSkipped existing collection ($newCollname): $existingColl"
            } else {
                boolean defaultFeatures = false
                FusionResponseWrapper responseWrapper = createCollection(newCollname, coll, appName, defaultFeatures)
                if (responseWrapper.wasSuccess()) {
                    log.info "\t\tCreated Collection: ($coll)"
                } else {
                    log.warn "Had a problem creating collection: $coll??? Response wrapper: $responseWrapper"
                }
                responses << responseWrapper
            }
        }
        return responses
    }

/*
    List<FusionResponseWrapper> addJobsIfMissing(String appName, Map srcFusionObjects) {
        List<FusionResponseWrapper> responses = []
        List<Map<String, Object>> existingJobs = getJobs(appName)

        // --------------- Create Collections ------------------
        srcFusionObjects['collections'].each { Map<String, Object> coll ->
            String newCollname = coll.id
            def existingColl = existingJobs.find { it.id == newCollname }
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
*/

    List<FusionResponseWrapper> addQueryPipelinesIfMissing(String appName, Map srcFusionObjects) {
        List<FusionResponseWrapper> responses = []
        List<Map<String, Object>> existingQryPs = getQueryPipelines(appName)

        // --------------- Create Query Pipelines ------------------
        srcFusionObjects['queryPipelines'].each { Map<String, Object> qryp ->
            String qrypId = qryp.id
            def existingQryp = existingQryPs.find { it.id == qrypId }
            if (existingQryp) {
                log.info "\t\tSkipping existing query pipeline ($qrypId)"
            } else {
                FusionResponseWrapper responseWrapper = createQueryPipeline(qryp, appName)
                if (responseWrapper.wasSuccess()) {
                    log.info "Created Collection: ($qryp)"
                } else {
                    log.warn "Had a problem creating collection: $qryp??? Response wrapper: $responseWrapper"
                }
                responses << responseWrapper
            }
        }
        return responses
    }

    /**
     * Read existing job schedules, and add anything "missing" from source (exported objects only at the moment)
     * @param appName to help with adding links (using app name in api call??) -- todo -- consider moving to explicit link management -- this is likely not working properly at the moment
     * @param srcJobsWithSchedules
     * @param overwrite flag to indicate if we should overwrite existing
     * @return list of fusion responses (for later analysis)
     */
    List<FusionResponseWrapper> addJobSchedulesIfMissing(String appName = '', List<Map<String, Object>> srcJobsWithSchedules, boolean overwrite = false) {
        log.info "addJobSchedulesIfMissing(app:$appName, srcJobsWithSchedules(${srcJobsWithSchedules.size()}):${srcJobsWithSchedules.collect { it.resource }}"
        List<FusionResponseWrapper> responses = []
        List<Map<String, Object>> existingJobs = getJobs(appName)

        // --------------- Create Schedules ------------------
        srcJobsWithSchedules.each { Map<String, Object> map ->
            String srcResourceName = map.resource
            def existingJob = existingJobs.find { Map<String, Object> existing -> existing.resource == srcResourceName }
            log.debug "Found matching jobs, src: $srcResourceName -> dest:$existingJob"
            if (existingJob) {
                def schedule = getJobSchedule(srcResourceName)
                if (schedule && schedule.triggers && !overwrite) {
                    log.info "\t\tJob schedule ($schedule) from source exists in destination Fusion app, nothing needed to add..."
                } else {
                    log.info "\t\tAdding Schedule ($map) from source to destination Fusion..."
                    def response = createJobSchedule(map, appName)
                    log.info "\t\tAdded Schedule ($map), result: $response"
                    responses << response
                }

            } else {
                log.warn "Missing source Job (${map.resource}) with triggers: ${map.triggers} in Destination Fusion ($fusionBase)"
            }
        }
        return responses
    }


    FusionResponseWrapper createJobSchedule(Map json, String app) {
        String url = null
        String resource = json.resource
//        if (app) {
//            url = "$fusionBase/api/apps/${app}/${resource}/schedule"
//        } else {
        url = "$fusionBase/api/jobs/$resource/schedule"
//        }
        log.info "\t\tCreate Job Schedule, url: $url -- json: $json"
        /*
        {"resource":"datasource:test-names","enabled":true,
        "triggers":[{"type":"interval","enabled":true,"interval":1,"timeUnit":"month","startTime":"2022-05-06T04:00:00.000Z"},
        {"type":"job_completion","enabled":true,"triggerType":"on_success","otherJob":"datasource:test_csv-test"},
        {"type":"interval","startTime":"2022-05-07T04:00:00.000Z","interval":1,"timeUnit":"month"}],
        "default":false}
         */
        HttpRequest request = buildPutRequest(url, json)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)

        return fusionResponseWrapper
    }


    /**
     * Add any missing datasources to this client's fusion instance
     * @param appName
     * @param srcFusionObjects
     * @param oldLinks
     * @return List<FusionResponseWrapper> list of responses (for review...) of any calls to add missing datasources
     * @todo consider refactoring: do an "informed diff" and then add/update any that need updating
     */
    List<FusionResponseWrapper> addDatasourcesIfMissing(String appName, Map srcFusionObjects, def oldLinks) {
        List<FusionResponseWrapper> responses = []

        List<Map<String, Object>> dsExisting = getDataSources()
        // get map of existing links (faster lookups...?)
        srcFusionObjects['dataSources'].each { Map<String, Object> p ->
            String dsName = p.id
            def existingDS = dsExisting.find { it.id == dsName }

            if (existingDS) {
                log.info "\t\tSKIPPING existing datasource: $dsName"
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

    /**
     * Get the 'tree' from solr/zk for things like solrconfig, stopwords, managed-schema
     * @return
     */
/*
    def getConfigSets(List<String> configsetsToGet = []) {
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
*/

    /**
     * get a specific solr configset
     * todo -- implemeent call to get the one
     * @return
     */
/*
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
        return fusionResponseWrapper
    }
*/


    /**
     * get objects from REST api call -- can get string, or zip file...
     * todo -- review, getting 500 error on my most recent check...
     * @param params
     * @param bodyHandler
     * @return
     */
    def getObjects(String params = '', HttpResponse.BodyHandler bodyHandler) {
        String url = "$fusionBase/api/objects/export${params}"
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
        // todo -- convert to proper URL with encoding params
        String url = "$fusionBase/api/collections/${collectionName}/solr-config"
        if (params) {
            url = "$url?${params.collect { "${it.key}=${it.value}" }.join('&')}"
        }

        HttpRequest request = buildGetRequest(url)
        HttpResponse.BodyHandler handler = HttpResponse.BodyHandlers.ofString()
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request, handler)
        if (fusionResponseWrapper.wasSuccess()) {
            List<Map<String, Object>> solrConfigs = fusionResponseWrapper.parsedList
//            def flattened = solrConfigs.flatten()
//            flattened.each {Map<String, Object> m ->
            solrConfigs.each { Map<String, Object> m ->
                SolrConfigThing thing = new SolrConfigThing(m)
                m.thing = thing
                if (thing.value) {
                    log.debug "what now? $thing"
                }
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
     * @param params , e.g. action: LISt and
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

    def blobUpload(String app, String path, String resourceType, def blob) {
        // {{furl}}//api/apps/{{app}}/blobs?resourceType=file
        //http://lucy:6764/api/apps/test/blobs/lib/test.js/FusionServiceLib.js?resourceType=file
        String url = "$fusionBase/api/apps/${app}/blobs/${path}?resourceType=${resourceType}"
        log.info "\t\t Blob UPLOAD -- path:${path}) type(${resourceType}) blob class name: ${blob.getClass().simpleName}"
        HttpRequest request = buildPutRequest(url, blob)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        return fusionResponseWrapper
    }

    /**
     * call to get blob definitions (not download the actual objects)
     * @param app
     * @param blobFilter
     * @param resourceType
     * @return list of blob definitions
     */
    List<Map<String, Object>> getBlobDefinitions(String app = '', String resourceType = '', def blobIdFilter = null) {
        String url
        if (app) {
            url = "${fusionBase}/api/apps/${app}/blobs"
        } else {
            url = "${fusionBase}/api/blobs"
        }
        if (resourceType) {
            url += "?resourceType=${resourceType}"
        }
//        if(blobIdFilter){
//            if(blobIdFilter instanceof String){
//                log.info ""
//            }
//        }
        log.info "\t\t Get blobs -- app:${app}  -- type(${resourceType})"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request)
        List blobDefs = null
        if (blobIdFilter) {
            blobDefs = fusionResponseWrapper.parsedList.findAll { it.id ==~ blobIdFilter }
        } else {
            blobDefs = fusionResponseWrapper.parsedList
        }
        return blobDefs
    }

    /**
     * call to get an unspecified bloc
     * todo -- improve binary blobs, currently this focuses on text, or zipfiles, other blobs will likely fail...
     * @param blobId
     * @param bodyHandler
     * @return an object as returned by FusionResponseWrapper
     */
    def getBlob(String blobId, BodyHandler bodyHandler = HttpResponse.BodyHandlers.ofString()) {
        String url = "${fusionBase}/api/blobs/${blobId}"
        log.info "\t\t Get blob thing with id: $blobId -- body handler: $bodyHandler"
        HttpRequest request = buildGetRequest(url)
        FusionResponseWrapper fusionResponseWrapper = sendFusionRequest(request, bodyHandler)
        def blob = fusionResponseWrapper.parsedInfo
        return blob
    }

    String getFusionVersion() {
        if (versionString) {
            return versionString
        } else {
            log.warn "No version string found..."
        }
    }

    int getFusionMajorVersion(String ver) {
        Integer majorVersion
        if (ver) {
            String firstChar = ver[0]
            if (firstChar.isInteger()) {
                majorVersion = new Integer(firstChar)
            } else {
                log.warn "Could not determine fusion major version from version string: '$ver'... setting to '-1'"
                majorVersion = -1
            }
            return majorVersion
        }
    }


    /**
     *  accept inputs to create a zip file archive that can be imported into Fusion
     *  typically need:
     *      objects.json (with metadata and Fusion version),
     *      (optional) blob things (often a file folder with objects to upload)
     *      (optional) configsets for solr collections
     * @param objectsJson
     * @param blobThings
     * @param configsetThings
     */
    def createImportableZipArchive(OutputStream outputStream, Map objectsJson, Map<String, Object> blobThings = [:], Map<String, Object> configsetThings = [:]) {
        ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream)
        def metadata = objectsJson.metadata
        String json = JsonOutput.toJson(objectsJson)
        String prettyJson = JsonOutput.prettyPrint(json)
        log.info "Add objects.json with keys: ${objectsJson.keySet()}"
        ZipEntry objectsJsonEntry = new ZipEntry('objects.json')
        zipOutputStream.putNextEntry(objectsJsonEntry)
        InputStream iois = IOUtils.toInputStream(prettyJson, StandardCharsets.UTF_8)
        // NOTE: IO utils has this method, but the one from Compress does not -- beware!
        IOUtils.copy(iois, zipOutputStream)

        blobThings.each { String namePath, Object blobItem ->
            String s = "/blobs/" + namePath
            log.info "Add blob: (${s} --> $blobItem"
            ZipEntry e = new ZipEntry(s)
            zipOutputStream.putNextEntry(e)

            InputStream inputStream = getInputStream(blobItem)
            IOUtils.copy(inputStream, zipOutputStream)
//            byte[] data = blobItem.getBytes()
//            zipOutputStream.write(data, 0, data.length)
        }

        configsetThings.each { namePath, thing ->
            String s = "/configset/" + namePath
            log.info "Add config thing: (${s}) --> $thing"
            ZipEntry e = new ZipEntry(namePath)
            zipOutputStream.putNextEntry(e)

            InputStream inputStream = getInputStream(thing)
            IOUtils.copy(inputStream, zipOutputStream)
        }
        log.info "\t\tClose ZipOutputStream: $zipOutputStream"
        zipOutputStream.close()

        return zipOutputStream
    }


    /**
     * helper method to populate a map with fusion export zip metadata (the fusionVersion element is the most important)
     * @param metadata
     * @param fusionVersion
     * @param fusionGuid
     * @param exportedBy
     * @return map with expected metadata values
     */
    Map<String, Object> createtObjectsMetadata(String fusionVersion, String fusionGuid, String exportedBy = 'FusionJClient') {
        Map metadata = [:]
        metadata.exportedBy = exportedBy
        metadata.formatVersion = "1"
        metadata.exportedDate = new Date().toInstant().toString()
        metadata.fusionVersion = fusionVersion
        metadata.fusionGuid = fusionGuid
        return metadata
    }


    InputStream getInputStream(Object o) {
        InputStream inputStream = null
        if (o instanceof URL) {
            // https://stackoverflow.com/questions/6932369/inputstream-from-a-url
            inputStream = ((URL) o).openStream()
        } else if (o instanceof File) {
            inputStream = ((File) o).newInputStream()
        } else if (o instanceof String) {
            inputStream = IOUtils.toInputStream(o, StandardCharsets.UTF_8)
//        } else if(o instanceof File){
//            inputStream = ((File)o).newInputStream()
        } else {
            log.warn "Unknown blob object type: ${o.class.name}"
        }
        return inputStream
    }

}
