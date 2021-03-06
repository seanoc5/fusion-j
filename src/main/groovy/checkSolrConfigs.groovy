import com.lucidworks.ps.clients.FusionClient
import groovy.cli.picocli.CliBuilder
import groovy.cli.picocli.OptionAccessor
import org.apache.log4j.Logger

import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths

/**
 * outdated test script? has some useful bits, but needs to be revised or remmoved
 *
 */
Logger log = Logger.getLogger(this.class.name);

log.info "Starting ${this.class.name}..."
String toolName = this.class.name

// todo switch to stanardized opts wrapper?
CliBuilder cli = new CliBuilder(usage: "${toolName}.groovy -fhttp://myFusion5addr:6764 -uadmin -psecret123 -s~/data/MyApp.objects.json -m ~/Fusion/migration/F4/mappingFolder", width: 160)
cli.with {
    h longOpt: 'help', 'Show usage information'
    f longOpt: 'fusionUrl', args: 1, required: true, 'Fusion url with protocol, host, and port (if any)--for new/migrated app'
    g longOpt: 'groupLabel', args: 1, required: false, defaultValue: 'TestGroup', 'Label for archiving/grouping objects; app name, environment, project,... freeform and optional'
    m longOpt: 'mappingDir', args: 1, required: false, 'Folder containing object mapping instructions (subfolders grouped by object type)'
    p longOpt: 'password', args: 1, required: true, 'password for authentication in fusion cluster (assuming basicAuth for now...)'
    s longOpt: 'source', args: 1, required: false, 'Source (objects.json or appexport.zip) to read application objects from (old app to be migrated)'
    t longOpt: 'test', 'Test connection to Fusion (fusionUrl, user, and password)'
    u longOpt: 'user', args: 1, argName: 'user', required: true, 'the fusion user to authenticate with'
    x longOpt: 'exportDir', args: 1, required: false, 'Export directory'
}

OptionAccessor options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(-1)
}
if (options.help) {
    cli.usage()
    System.exit(0)
}
log.info "Setup fusion client..."
FusionClient fusionClient = new FusionClient(options)

log.info "\t\tcheck loading Solr Schema..."
def solrSchema = fusionClient.getSolrSchema("test")
def schemaOverview = solrSchema.collect {"${it.key}(${it.value instanceof Collection ? it.value.size() : it.value})"}
log.info "\t\tgot schema with keys (and size): ${schemaOverview}"

def solrConfigs = fusionClient.getSolrConfigList("test")

log.info "Solr configs, size:${solrConfigs.size()}"
Path tempPath = Paths.get("tempfile.zip")
HttpResponse.BodyHandler bodyHandler = HttpResponse.BodyHandlers.ofFile(tempPath)
log.info "\t\tusing temp file: ${tempPath.toAbsolutePath()}"

def objects = fusionClient.getObjects("", bodyHandler)
def configSets = fusionClient.getConfigSets()

log.info "ConfigSets: $configSets"
