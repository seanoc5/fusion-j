package com.lucidworks.test

import com.lucidworks.ps.clients.FusionClient
import groovy.json.JsonSlurper
import org.apache.log4j.Logger

import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipEntry
import java.util.zip.ZipFile


final Logger log = Logger.getLogger(this.class.name);
log.info "Starting ${this.class.name}..."

URL baseUrl = new URL('http://newmac:8764')
String user = 'sean'                            // fusion username for saving content back to fusi
String pass = 'pass1234'
String app = 'test'
String coll = 'test'
String qryp = 'test'

String group = 'Sean newmac 4.2.6'
boolean commit = true
Date now = new Date()
String exportDir = "C:\\work\\Lucidworks\\test"

FusionClient fusionClient = new FusionClient(baseUrl, user, pass)
List<Map> apps = fusionClient.getApplications()
log.info "Apps: $apps"
apps.each { Map appMap ->
    String appId = appMap.id
    Path outputPath = Paths.get("${exportDir}/app-${appId}-export.zip")
    HttpResponse<Path> response = fusionClient.exportFusionObjects("app.ids=$appId",outputPath)

    ZipFile zipFile = new ZipFile(outputPath.toAbsolutePath().toString())
    ZipEntry objEntry = zipFile.getEntry("objects.json");
    InputStream inputStream = zipFile.getInputStream(objEntry)
    def json = new JsonSlurper().parse(inputStream)

    log.info "success??"
}


String q = 'tag_ss:deleteme'
log.info "Delete by query(commit:$commit): '$q'..."
def r = fusionClient.deleteByQuery(coll, q, commit)
log.info "Delete response: $r"

HttpResponse response = fusionClient.query(app, coll, qryp, [q: q, fl: '*,score', rows: 55])
log.info "What came back for query($q)? ==> $response"
def respdocs = fusionClient.getQueryResponseDocs(response)
log.info "Response docs (0 docs??): $respdocs"

List<Map<String, Object>> docs = [
        [id: 'testA', title_t: "A Title", tag_ss: ['test', 'A', 'deleteme'], timestamp_tdt: now],
        [id: 'testB', title_t: "Be Title", tag_ss: ['test', 'B', 'deleteme'], timestamp_tdt: now],
]

HttpResponse idxRes = fusionClient.indexContentByCollectionPipeline('test', 'test', docs, commit)
log.info "Index repsonse: $idxRes"

response = fusionClient.query(app, coll, qryp, [q: q, fl: '*,score', rows: 555])
log.info "What came back for query($q)? ==> $response"
respdocs = fusionClient.getQueryResponseDocs(response)
log.info "Response docs (2??): $respdocs"

log.info "Done...?"





