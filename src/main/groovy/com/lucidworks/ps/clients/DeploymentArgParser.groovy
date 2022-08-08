package com.lucidworks.ps.clients

import com.lucidworks.ps.Helper
import groovy.cli.picocli.CliBuilder
import groovy.cli.picocli.OptionAccessor
import org.apache.log4j.Logger

/**
 * Helper class to standardize argument syntax for FusionClient programs and driver-scripts
 * TODO -- should change to 'TransformArgParser'....
 */
class DeploymentArgParser {
    static Logger log = Logger.getLogger(this.class.name);

    public static OptionAccessor parse(String toolName, String[] args) {
        CliBuilder cli = new CliBuilder(usage: "${toolName}.groovy -s/Users/sean/data/MyExportedApp.zip", width: 160)
        cli.with {
            h longOpt: 'help', 'Show usage information'
            c longOpt: "config", args:1, required: true,  argName: 'configFile', 'Configuration file to load with GRoovy ConfigSlurper: http://docs.groovy-lang.org/next/html/gapi/groovy/util/ConfigSlurper.html'
            r longOpt: "replace", 'boolean option for replace destination if a file exists (folders with existing files are a special case)'
            s longOpt: 'source', args: 1, required: true, argName: 'sourceFile', 'Source (objects.json or appexport.zip) to read application objects from (old app to be migrated)'
            x longOpt: 'exportDir', args: 1, required: true, argName: 'dir', 'Export directory'
        }


        OptionAccessor options = cli.parse(args)
        if (!options) {
            System.exit(-1)
        } else if (options.help) {
            cli.usage()
            System.exit(0)
        } else {
            log.info "Args parsed to find Source: ${options.source}"
            if (options.exportDir) {
                def expdir = Helper.getOrMakeDirectory(options.exportDir)
                log.info "using export folder: ${expdir.absolutePath}"
            }
            options
        }

    }
}
