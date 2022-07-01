import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.clients.FusionClientArgParser
import groovy.cli.picocli.OptionAccessor
import org.apache.log4j.Logger
/**
 * exploratory script, pulling together several Fusion-j calls to get an overview of a Fusion install
 */
Logger log = Logger.getLogger(this.class.name);

OptionAccessor options = FusionClientArgParser.parse(this.class.name, args)

log.info "start script ${this.class.name}..."

FusionClient fusionClient = new FusionClient(options)// todo -- revisit where/how to parse the source json (file, zip, or fusionSourceCluster...?), currently mixing approaches, need to clean up

File srcJson = fusionClient.objectsJsonFile
Map sourceFusionOjectsMap = parsedMap.objects
log.info "\t\tSource Fusion Objects count: ${sourceFusionOjectsMap.size()} \n\t${sourceFusionOjectsMap.collect { "${it.key}(${it.value.size()})" }.join('\n\t')}"


log.info "done...?"
