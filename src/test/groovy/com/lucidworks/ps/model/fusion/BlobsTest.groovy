package com.lucidworks.ps.model.fusion

import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author :    sean
 * @mailto :    seanoc5@gmail.com
 * @created :   7/27/22, Wednesday
 * @description:
 */

class BlobsTest extends Specification {
    def "constructor should load expected items from test folder"() {
        given:
        Path testFolder = Paths.get(getClass().getResource('/blobs/complex/').toURI())

        when:
        Blobs blobs = new Blobs('spock-test', testFolder)

        then:
        blobs.srcItems.size() == 15
    }

    def "Export"() {
        given:
        Path testFolder = Paths.get(getClass().getResource('/blobs/complex/').toURI())
        Path tempFolder = Files.createTempDirectory(testFolder.parent, 'blobs-output-temp')

        when:
        Blobs blobs = new Blobs('spock-test', testFolder)
        def rc = blobs.export(tempFolder.toFile())

        then:
        blobs.srcItems.size() == 15

    }

//    def "LoadBlobs"() {
//    }
}
