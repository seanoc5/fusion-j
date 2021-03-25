import com.lucidworks.ps.clients.FusionClient
import groovy.json.JsonSlurper
import org.apache.log4j.Logger

import java.net.http.HttpResponse
import java.util.concurrent.atomic.AtomicInteger

import static groovyx.gpars.GParsPool.withPool
//import com.opencsv.CSVWriter

final Logger log = Logger.getLogger(this.class.name);
log.info "Starting ${this.class.name}..."
String env = 'stg'
String fhost = "https://build-com-${env}.b.lucidworks.cloud/"
Integer port = null
String user = 'seanoconnor'
String pass = '2RK%RtcxJ4fzH13j*'
String app = 'build'
String collSuggestions = 'build_query_suggestions'
String qplSuggestions = '_system'
String qSuggestionField = 'query_s'

String qplMain = 'build_search_QPL'       //'build_search_QPL'    //'build_typeahead_query_QPL'
String collMain = 'build'

FusionClient fusionSuggestionsClient = new FusionClient(fhost, port, user, pass, app, collSuggestions)
Map params = [q:'*:*', rows:100000, fl:"id,${qSuggestionField},weight_d"]
log.info "Preparing query to get suggestions using params: $params"
HttpResponse response = fusionSuggestionsClient.query(qplSuggestions, params)
JsonSlurper jsonSlurper = new JsonSlurper()
def json = jsonSlurper.parseText(response.body())
List<Map> docs = json.response?.docs
AtomicInteger atomicInteger = new AtomicInteger(0)

FusionClient fusionMainClient = new FusionClient(fhost, port, user, pass, app, collMain)

File outputCsv = new File("../../build_query_suggestions.${env}.checked.csv")      // TODO
char seperator = ',' as char
//CSVWriter csvWriter = new CSVWriter(new FileWriter(outputCsv));     // TODO -- problem with groovy magic and getting an actual char for params, just go with defaults...

boolean turboCharge = true // parallel processing or no

if(docs) {

    if(turboCharge) {
        withPool() {
            docs.eachParallel { Map docMap ->
                int cnt = atomicInteger.incrementAndGet()
                String q = docMap[qSuggestionField]
                log.debug "\t\t$cnt)  query: '$q'"

                Map mainParams = [q: q, rows: 1, fl: 'id']
                HttpResponse testResponse = fusionMainClient.query(qplMain, mainParams)
                Map jsonTest = jsonSlurper.parseText(testResponse.body())
                Map rsp = jsonTest.response
                if (rsp && rsp.keySet() ) {
                    Integer numFound = rsp.numFound
                    BigDecimal maxScore = rsp.maxScore
                    String[] row = [q, numFound, maxScore.round(2)]
                    if(numFound==0){
                        log.warn "$cnt) No results?!? Query: '${q}'"
                    } else {
                        log.debug "$cnt) Found something, row: $row"
                    }
                    if (cnt % 50 == 0) {
                        log.info "$cnt) processing row: $row"
                    }
//                    csvWriter.writeNext(row)
                } else {
                    log.warn "Response was empty or invalid: $jsonTest"
                }
            }
        }

    } else {
        int serialCounter = 0
        docs.each { Map docMap ->
            serialCounter++
            String q = docMap[qSuggestionField]
            log.debug "\t\t$serialCounter)  query: '$q'"

            Map mainParams = [q: q, rows: 1, fl: 'id']
            HttpResponse testResponse = fusionMainClient.query(qplMain, mainParams)
            Map jsonTest = jsonSlurper.parseText(testResponse.body())
            Map rsp = jsonTest.response
            if (rsp && rsp.keySet() ) {
                Integer numFound = rsp.numFound
                BigDecimal maxScore = rsp.maxScore
                if(numFound==0){
                    log.warn "No results?!? Query: '${q}'"
                }
//                log.debug "\t $serialCounter) $q,$numFound,${maxScore.round(2)}"
                String[] row = [q, numFound, maxScore.round(2)]
                if (serialCounter % 50 == 0) {
                    log.info "$serialCounter) processing row: $row"
                } else {
                    log.info "\t\t$serialCounter) processing row: $row"
                }

//                csvWriter.writeNext(row)
            } else {
                log.warn "Response was empty or invalid: $jsonTest"
            }

        }
    }

} else {
    log.warn "No docs found in json: $json"
}

//csvWriter.close()
//log.info "Wrote csv file: ${outputCsv.absoluteFile}"

log.info "Done...?"

