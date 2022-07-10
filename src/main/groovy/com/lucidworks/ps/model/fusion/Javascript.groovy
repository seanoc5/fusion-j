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

    Javascript(String label, String script) {
        this.label = label
        this.script = script
        lines = script.split('\n')
    }

    @Override
    def export(File exportFolder) {
        log.info "export to folder: ${exportFolder.absolutePath}"
        String fileName = getItemName() + '.js'
        File jsOut = new File(exportFolder, fileName)
        jsOut.text = script
        return null
    }

    def groupLines() {
        def grouped = lines.groupBy { String line ->
            line = line.trim()
//            if(line.contains('//')

            String type = 'n.a.'

            if (line ==~ /\s*(var|let) */) {
                type = 'assignment'
            } else if (line ==~ /\s*(var|let) */) {
                switch (line.trim()) {
                    case ~/(var|let).*=.*/:
                        type = 'assignment'
                        break
                }
            }
        }
    }


    @Override
    def export(FusionClient fusionClient) {
        throw new RuntimeException("Export to live fusion client not implemented yet...")
        return null
    }


    String getItemName(){
        return label
    }
}
