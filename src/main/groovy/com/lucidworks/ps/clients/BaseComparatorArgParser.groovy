package com.lucidworks.ps.clients

import groovy.cli.picocli.CliBuilder
import groovy.cli.picocli.OptionAccessor

/**
 * General helper class trying to standardize args specific to solr operations
 * probably can be deprecated or removed, but leaving for now
 */
class BaseComparatorArgParser {

    public static OptionAccessor parse(String toolName, String[] args) {
        CliBuilder cli = new CliBuilder(usage: "${toolName}.groovy -fhttp://myFusion5addr:6764 -uadmin -psecret123 -s~/data/MyApp.objects.json -m ~/Fusion/migration/F4/mappingFolder", width: 160)
        cli.with {
            h longOpt: 'help', 'Show usage information'
            l longOpt: 'left', args:1, required: true, argName: 'left', "Left Source file (source?)"
            r longOpt: 'right', args:1, required: true, argName: 'right', "Right Source file (destination?)"
            x longOpt: 'exportDir', args: 1, required: false, argName: 'dir', 'Export directory'
        }


        OptionAccessor options = cli.parse(args)
        if (!options) {
//            cli.usage()
            System.exit(-1)
        }
        if (options.help) {
            cli.usage()
            System.exit(0)
        }
        options
    }

}
