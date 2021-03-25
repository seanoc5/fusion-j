package com.lucidworks.test

import com.lucidworks.ps.clients.FusionClient
import org.apache.log4j.Logger

import java.net.http.HttpResponse

final Logger log = Logger.getLogger(this.class.name);
log.info "Starting ${this.class.name}..."
Map envMap = [
        local: [host: 'http://newmac', port: 8764, user: 'sean', pass: 'pass1234', application: 'lucy', collection: 'lucy', indexPipeline:'lucy', queryPipeline:'lucy'],
        radar: [host: 'http://radar.lucidworks.com', port: 0, user: 'sean', pass: 'FusionR0cks', application: 'Lucy', collection: 'Lucy', indexPipeline:'lucy', queryPipeline:'lucy'],
]
Map env = envMap['local']

boolean commit = true

String queryPipeline = 'Lucy'
String indexPipeline = 'lucy-slack-pipe'
Date now = new Date()

//FusionClient fusionClient = new FusionClient(fusionBase, port, user, pass, application, collection)
FusionClient fusionClient = new FusionClient(env)

String q = 'tag_ss:*'
log.info "Delete by query(commit:$commit): '$q'..."
def r = fusionClient.deleteByQuery(q, commit)
log.info "Delete response: $r"

List<Map<String, Object>> docs = [
        [id:'testA', title_t:"A Title", tag_ss:['test', 'A', 'zzz'], timestamp_tdt:now],
        [id:'testB', title_t:"Be Title", tag_ss:['test', 'B', 'zzz'], timestamp_tdt:now],
]

//String jsonToIndex = '[{ "id":"test1", "title_t":"my test json document zzz", "tag_ss":["test", "json"], "dateCreated_tdt":"${now}" }]'
//def idxRes = fusionClient.indexContent(jsonToIndex, 'lucy', 'lucy', commit)

HttpResponse idxRes = fusionClient.indexContent(docs, env.collection, indexPipeline, commit)
log.info "Index repsonse: $idxRes"

HttpResponse response = fusionClient.query(env['queryPipeline'], [q:q, fl:'id,title_t,timestamp_tdt,tag_ss,score', rows:555])
log.info "What came back for query($q)? ==> $response"
Map qryResponseMap = fusionClient.getQueryResponseDocs(response)
log.info "Response json: $qryResponseMap"
docs = qryResponseMap.response?.docs
if(docs){
    log.info "Response docs: $docs"
} else {
    log.warn "no response docs...?"
}

log.info "Done...?"





