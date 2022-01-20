package com.lucidworks.test

import groovy.transform.Field
import org.apache.log4j.Logger

import java.util.zip.ZipEntry
import java.util.zip.ZipFile

@Field
Logger log = Logger.getLogger(this.class.name);

log.info "Starting script: ${this.class.name}"

ZipFile zipFile = new ZipFile('C:\\work\\Lucidworks\\fusion-j\\src\\main\\groovy\\com\\lucidworks\\test\\app-test-export.zip')
//Iterator<? extends ZipEntry> zipEntries = Iterators.forEnumeration(new ZipFile(zip).entries());
Enumeration<? extends ZipEntry> entries = zipFile.entries()
def objectsJson = null
entries.each {ZipEntry zipEntry ->
    if(zipEntry.name.contains('objects.json')){
        objectsJson = zipEntry
    }
    log.info "ZipEntry: $zipEntry"
}

def objectsJson2 = entries.find {it.name.contains('objects.json')}
log.info "done...?"

