package com.lucidworks.test

import com.lucidworks.ps.clients.FusionClient
import groovy.cli.picocli.CliBuilder
import groovy.json.JsonSlurper
import org.apache.log4j.Logger

import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipEntry
import java.util.zip.ZipFile


final Logger log = Logger.getLogger(this.class.name);
log.info "Starting ${this.class.name}..."
def args = this.args

def cli = new CliBuilder(usage: "${this.class.name}.groovy [-a] -c")
cli.with {
    h longOpt: 'help', 'Show usage information'
    a longOpt: 'app', args: 1, defaultValue: 'test', 'fusion application'
    c longOpt: 'collection', args: 1, defaultValue: 'test', 'collection to focus on (if any)'
    d longOpt: 'directory for export', args:1, defaultValue: '.', 'Export directory'
    f longOpt: 'fusion', args: 1, defaultValue: 'http://newmac:8764', 'Fusion url with protocol, host, and port (if any)'
    g longOpt: 'group', args: 1, defaultValue: 'SeanTest', 'Group/label for these objects'
    p longOpt: 'password', args: 1, defaultValue: 'pass1234', 'password for auth'
    u longOpt: 'user', args: 1, argName: 'user', defaultValue: 'sean', 'the fusion user to authenticate with'
}
def options = cli.parse(args)
String url = options.f
URL baseUrl = new URL(url)
String user = options.u                            // fusion username for saving content back to fusi
String pass = options.p
String app = options.a
String coll = options.c

group = 'Sean newmac 4.2.6'
boolean commit = true
Date now = new Date()
String exportDir = options.d

FusionClient fusionClient = new FusionClient(baseUrl, user, pass)
List<Map> apps = fusionClient.getApplications()
log.info "Apps: $apps"
apps.each { Map appMap ->
    String appId = appMap.id
    Path outputPath = Paths.get("${exportDir}/app-${appId}-export.zip")
    HttpResponse<Path> response = fusionClient.exportFusionObjects("app.ids=$appId", outputPath)

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





