package com.lucidworks.ps

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger

import java.text.DateFormat
import java.text.SimpleDateFormat

//import java.security.InvalidParameterException

/**
 * General helper class for Fusion-j client (another similar object for UpVal??)
 */
class Helper {
    static Logger log = Logger.getLogger(this.class.name);

    /**
     * util function to get an (output) folder (for exporting), create if necessary
     * @param dirPath where the (new) directory/folder should be
     * @return created directory
     */
    static File getOrMakeDirectory(String dirPath) {
        File folder = new File(dirPath)
        getOrMakeDirectory(folder)
    }

    static File getOrMakeDirectory(File parentFolder, String subFolder) {
        File folder = new File(parentFolder, subFolder)
        getOrMakeDirectory(folder)
    }

    static File getOrMakeDirectory(File folder) {
        if (folder.exists()) {
            if (folder.isDirectory()) {
                log.debug "Folder (${folder.absolutePath} exists, which is good"
            } else {
                log.warn "Folder (${folder.absolutePath}) exists, but is not a folder, which is bad"
                throw new IllegalAccessException("Job Folder (${folder.absolutePath}) exists, but is not a folder, which is bad, aborting")
            }
        } else {
            def success = folder.mkdirs()
            if (success) {
                log.info "\t\tCreated folder: ${folder.absolutePath}"
            } else {
                log.warn "Folder (${folder.absolutePath}) could not be created, which is bad"
                throw new IllegalAccessException("Folder (${folder.absolutePath}) exists, could not be created which is bad, aborting")
            }
        }
        folder
    }

    /**
     * placeholder for getting a psuedo source-control folder name for exports (and potentially imports / restore)
     * @param date
     * @param dateFormat --
     * @return a "sort friendly" datestamp with hour & minute to allow multiple snapshots per day (or per hour)...
     */
    static String getVersionName(Date date = new Date(), DateFormat dateFormat = new SimpleDateFormat('yyyy-MM-dd.hh.mm')) {
        String s = dateFormat.format(date)
    }

    static String sanitizeFilename(String name, String substitute = '_', String regex = "[^a-zA-Z0-9\\.\\-]") {
        String sanitized = name.replaceAll(regex, substitute);
    }

    static boolean isJson(def body) {
        if(body instanceof String){
            if(body.trim().startsWith('{') || body.trim().startsWith('[')){
                return true
            } else {
                log.info "Body did not start with curly or square bracket, assuming not json. First few characters: " + body[0..(Math.min(10, body.size()))]
            }
        } else {
            log.info "Body was not a string, but rather class: ${body.getClass().name} -- not json"
        }
        false
    }


    /** experimenting with in-memory stream->zip file (move me??)
     *
     * @param stream from something like a call to objects/export api with a @BodyHandlers.ofInputStream
     * @return something... (currently an in-memory zip file from Apache commons compress...)
     */
    static def processStream(InputStream stream) throws IOException{
        ZipFile zipFile
        try {
            SeekableInMemoryByteChannel channel = new SeekableInMemoryByteChannel(IOUtils.toByteArray(stream));
            zipFile = new ZipFile(channel)
            Enumeration<ZipArchiveEntry> entries = zipFile.getEntries()
            log.info "Converted Stream (objects/export API call/download?) to ZipArchiveEntry enum: ${entries}"
        } catch (IOException ioe){
            log.warn "Had problem converting Stream (export api call???) to ZipFile (possibly not a zip file we are getting??): $ioe"
            throw ioe
        } finally {
            if (zipFile) {
                log.debug "Closing zipfile..."
                zipFile.close()
            }
        }
        return zipFile
    }


    /**
     *
     * @param objectsMap - either a basic JsonSlurper object, or the very similar JsonObject wrapper of that
     * @param blobsFolder - a structure mimicking the filesystem structure created when unziping an exported Fusion4+ app for blobs (these are the actual blobs, rather than the blob definitions in the objectsMap/objects.json)
     * @param configsetsFolder - a structure mimicking the filesystem structure created when unziping an exported Fusion4+ app -- these are Solr configsets which should match a zk downconfig for each solr collection
     * @return
     */
    ZipFile buildImportableZip(Map objectsMap, def blobsFolder, def configsetsFolder){
        ZipFile importableZip = null
        try {
            InputStream stream = new BufferedInputStream(objectsMap)
            SeekableInMemoryByteChannel channel = new SeekableInMemoryByteChannel(IOUtils.toByteArray(stream));
            importableZip = new ZipFile(channel)
            Enumeration<ZipArchiveEntry> entries = importableZip.getEntries()
        } finally {
            if (importableZip) {
                importableZip.close()
            }
        }

        return importableZip
    }
}

