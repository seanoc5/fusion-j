package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger

/**
 * placeholder component app for Datasources (when/if necessary)
 * todo -- remove me, or make me useful, I am space at the moment....
 */
class Javascript extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);

    String label
    String script
    List<String> lines = []
    Map<String, List<String>> groupedLines = [:]

    Javascript(String label, String script) {
        this.label = label
        this.script = script
        lines = script.split('\n')
        srcItems = lines
    }

    @Override
    def export(File exportFolder) {
        log.info "export to folder: ${exportFolder.absolutePath}"
        String fileName = "${label}.js"
        File jsOut = new File(exportFolder, fileName)
        jsOut.text = script
        return null
    }

    def groupLines() {
        groupedLines = lines.groupBy { String line ->
            if (line.contains('//')) {
                List<String> parts = line.split(/\/\//)
                log.debug "Stripping comment from line:[$line] -> [${parts[0]}]"
                line = parts[0]
            }

            String type = 'n.a.'
            line = line.trim()

            switch (line) {
                case ~/(var|let).*=.*/:
                    type = 'assignment'
                    break
                case ~/(console|log).*/:
                    type = 'logging'
                    break
                default:
                    type = 'unknown'
            }
            log.debug "Line type: $type :: $line"
        }
        log.info "\t\tGrouped lines: " + groupedLines.collect {
            it.key + ":" + it.value.size()
        }
    }



@Override
def export(FusionClient fusionClient) {
    throw new RuntimeException("Export to live fusion client not implemented yet...")
    return null
}


String getItemName() {
    return label
}

String toString() {
    String s = "$label with (${lines.size()}) lines and (${script.size()} characters)"
}

}
