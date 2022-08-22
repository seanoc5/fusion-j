package com.lucidworks.ps.clients

import com.lucidworks.ps.Helper
import groovy.cli.picocli.CliBuilder
import groovy.cli.picocli.OptionAccessor
import org.apache.log4j.Logger

/**
 * Helper class to standardize argument syntax for FusionClient programs and driver-scripts
 * TODO -- should change to 'TransformArgParser'....
 */
class DeployArgParser {
    static Logger log = Logger.getLogger(this.class.name);

    public static OptionAccessor parse(String toolName, String[] args) {
        CliBuilder cli = new CliBuilder(usage: "${toolName}.groovy -s/Users/sean/data/MyExportedApp.zip", width: 160)
        cli.with {
            h longOpt: 'help', 'Show usage information'
            a longOpt: 'appName', args: 1, required: false, argName: 'AppName', 'Application name: optional to override template'
            c longOpt: 'config', args: 1, required: true, argName: 'ConfigFile', "Configuration file (for ConfigSlurper)"
            f longOpt: 'fusionUrl', args: 1, required: true, argName: 'url', 'MAIN/Destination Fusion url with protocol, host, and port (if any)--for new/migrated app'
            p longOpt: 'password', args: 1, required: true, argName: 'passwrd', 'password for authentication in fusion cluster (assuming basicAuth for now...) for MAIN/dest fusion'
            s longOpt: 'source', args: 1, required: false, argName: 'sourceFile', 'Source (objects.json or appexport.zip) to read application objects from (old app to be migrated)'
            n longOpt: 'feartureName', args:1, required: false, argName: 'featureName', 'Name of Feature: optional to override template'
            u longOpt: 'user', args: 1, argName: 'user', required: true, 'the fusion user to authenticate with for MAIN/dest fusion'
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
