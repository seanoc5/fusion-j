package com.lucidworks.ps.model.fusion

import com.lucidworks.ps.clients.FusionClient
import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger
import org.apache.solr.common.SolrInputDocument

/**
 * placeholder component app for Datasources (when/if necessary)
 * todo -- remove me, or make me useful, I am space at the moment....
 */
class Javascript extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);

    String label
    String script
    String sourceType
    List<String> lines = []
    Map<String, List<String>> groupedLines = [:]

    Javascript(String label, String script, String sourceType = null) {
        this.label = label
        this.script = script
        this.sourceType = sourceType
        lines = script.split('\n')
        srcItems = lines
    }

    @Override
    def export(File exportFolder) {
        log.info "export to folder: ${exportFolder.absolutePath}"
        String fileName = "${label}.js"
        File jsOut = new File(exportFolder, fileName)
        jsOut.text = script
        return jsOut
    }

    Map<String, List<String>> groupLines() {
        log.debug "Group lines: ${this.label}"
        Map<String, List<String>> groupedLines = [:].withDefault { [] }

        lines.each { String line ->
            String type = 'n.a.'
            String lineTrimmed = line.trim()
            if (lineTrimmed) {
                if (lineTrimmed.contains('//')) {
                    List<String> parts = lineTrimmed.split(/\/\//, 2)
                    log.debug "Stripping comment from line:[$lineTrimmed] -> [${parts[0]}]"
                    lineTrimmed = parts[0].trim()
                    String comment = '//'+parts[1]
                    groupedLines.comments << comment
                } else {
                    log.debug "regular "
                }


                switch (lineTrimmed) {
                    case '':
                        type= 'blank'
                        break

                    case ~/(var|let) .* *= *\w+\..*/:
                        type = 'java package import'
                        break
                    case ~/(var|let) .*=.* new (.*)/:
                        type = 'java variable init'
                        break
                    case ~/(Packages|java)\..*/:
                        type = 'java library call?'
                        break

                    case ~/(var|let).*=.*/:
                        type = 'variable init'
                        break
                    case ~/(.*= *)?\w+\.\w+\(.*\).*/:           // overlap with control statements,but we are most concerned with function calls for evaluation
                        if(itemName =~ /doc(ument)?\.(add|set|remove)/){
                            type = 'doc adjustment'
                        } else {
                            type = 'method call'
                        }
                        break
                    case ~/(\w+) *[+-]?=.*\w+.*/:
                        type = 'variable assignment'
                        break
                    case ~/function *(\w+)?\(.*\) *\{?/:
                        type = 'function declaration'
                        break
                    case ~/(if *\(.*\)|[} ]*else\b[{ (]*).*/:
                        type = 'if-conditional'
                        break

                    case ~/(console|log).*/:
                        type = 'logging'
                        break

                    case ~/try\b *\{?/:
                        type = 'try block '
                        break
                    case ~/[} ]*catch\b.*/:
                        type='catch block'
                        break
                    case ~/.*\bthrow\b.*/:
                        type='throw block'
                        break

                    case ~/while\b.*/:
                        type='while block'
                        break

                    case ~/for\b.*/:
                        type='for block'
                        break

                    case ~/[)(}{]+/:
                        type = 'syntax'
                        break

                    case ~/(return\b.*|doc;?|document;?)/:
                        type = 'return'
                        break

                    default:
                        type = 'unknown'
                }
                groupedLines[type] << lineTrimmed
                log.debug "Line type: $type :: $lineTrimmed"
            } else {
                log.debug "Skipping blank line: '$line"
            }
        }

        log.info "\t\tGrouped lines: " + groupedLines.collect {
            it.key + ":" + it.value.size()
        }

        return groupedLines
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

    SolrInputDocument toSolrInputDocument() {
        SolrInputDocument sid = new SolrInputDocument()
        sid.setField('id', this.label)
        sid.addField('label_s', label)
        sid.addField('type_s', itemType)
//        sid.addField('_lw_data_source_s', 'javascript stage extrac')
        sid.addField('script_txt', script)
        sid.addField('sourceType_s', sourceType)
        sid.addField('jslines_txt', lines)
        sid.addField('lineCount_i', lines.size())

        if (!groupedLines) {
            groupedLines = groupLines()
            log.debug "Grouped lines: $groupedLines"
        }
        groupedLines.each { String group, List<String> groupLines ->
            log.debug "\t\t add groupded lines ($group) with ${groupLines.size()} lines"
            sid.addField("jsTag-${group}_txt", groupLines)
            sid.addField("count-${group}_i", groupLines.size())
        }
        sid.addField('tag_ss', groupedLines.keySet())
        return sid
    }

}
