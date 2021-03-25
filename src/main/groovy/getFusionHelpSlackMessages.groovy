import com.lucidworks.ps.clients.FusionClient
import groovy.transform.Field
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.log4j.Logger

import java.net.http.HttpResponse

@Field
Logger log = Logger.getLogger(this.class.name);
log.info "Starting script ${this.class.name}"

String app = 'lucy'
String coll = app
String qryp = 'terms-slack'


FusionClient fusionLucy = new FusionClient('http://oldmac', 8764, 'sean', 'pass1234', app, coll)

// get terms from the collection as a whole...
String termsField = 'text_t'

Map<String, Integer> termsMap = getTerms(fusionLucy, termsField, 10000, '[a-z]+', './cache')

log.info "Output file: ${outCSV.absolutePath} -- ${outCSV.size()}"


//Map params = [q: '*:*', fq: 'channel_s:fusion-help text_t:*', fl: 'id,text_t', debug: true, echoParams: 'all']
//log.info "Query fusion client with params: ${params}"
//
//HttpResponse rsp = fusionLucy.query(qryp, params)
//List<Map> docs = fusionLucy.getQueryResponseDocs(rsp)


FusionClient fusionCapam = new FusionClient('http://cap426', 8764, 'sean', '2021Rocks', 'web2021', 'web2021')
// get terms from the collection as a whole...
termsField = 'body'
HttpResponse capamRsp = fusionCapam.getTermsResponse(termsField, 10000)
//Map termsResponseMap = fusionClient.parseResponse(rsp)
def capamTermsMap = fc.parseTermsResponse(capamRsp)
outPath = outCSV.parent + '/capam-terms.csv'
log.info "Second output (capam terms): ${outPath}"
outCSV = exportToCSV(termsMap, outPath)
log.info "Output file: $outCSV"


log.info "done...?"



// ------------------------------ functions --------------------------------------
public File exportToCSV(Map<String, Number> termsMap, def outputDestination) {
    File outputCsv = null
    if(outputDestination instanceof String){
        outputCsv =  new File(outputDestination)
/*
        //  todo -- proper test for writability...? returns false, presumably because the file does not exist...?
        if(!outputCsv.canWrite()){
            throw new RuntimeException("Cannot write to file name/path: $outputDestination")
        } else {
            log.info "Using file path: $outputDestination to write to file: ${outputCsv.absolutePath}"
        }
*/
    } else if(outputDestination instanceof File){
        outputCsv = outputDestination
        log.info "Using file param as passed for output to: ${outputCsv.absolutePath}"
    } else {
        log.warn "Unknown param outFile (class: ${outputDestination.class.name}), hoping for the best..."
        outputCsv = outputDestination
    }

    outputCsv.withPrintWriter { PrintWriter pw ->
        CSVPrinter csvPrinter = new CSVPrinter(pw, CSVFormat.DEFAULT.withHeader("Index", "Term", "Count"));
        int cnt = 0
        termsMap.each { String term, def frequency ->
            cnt++
            csvPrinter.printRecord(cnt, term, frequency)
            if (cnt % 1000 == 0) {
                log.info "\t $cnt) wrting record: $term -> $frequency"
            } else {
                log.debug "\t $cnt) wrting record: $term -> $frequency"
            }
        }
        csvPrinter.flush();
    }
    log.info "\t$cnt) Done exporting To CSV: ${outputCsv.absolutePath} -- ${outputCsv.size()}"
    return outputCsv
}


public Map<String, Number> getTerms(FusionClient fc, String termsField, Integer limitCount = 10000, String regex = '[a-z]+', String cachePath = './cache') {
    log.info "Get Terms: $fc == $termsField -- limit: $limitCount -- regex: $regex -- cache: $cachePath"
    HttpResponse rsp = fc.getTermsResponse(termsField, limitCount, regex)
    Map<String, Integer> termsMap = fc.parseTermsResponse(rsp)

    if(cachePath){
        File cacheDir = null
        File outCSV = null
        cacheDir = new File(cachePath)
        log.info "Using cache dir: ${cacheDir.absolutePath}"
        String outfilename = 'terms.' + fc.getApplication() + '.' + fc.getCollection() + ".field-$termsField.csv"
        outCSV = new File(cacheDir, outfilename)
        File writtenCsv = exportToCSV(termsMap, outCSV.path)
        log.info "Wrote cache file: ${writtenCsv.absolutePath} -- file size: ${writtenCsv.size()}"
    } else {
        log.info "No cache path given, skipping any terms caching (loading or saving) ..."
    }
    return termsMap
}

