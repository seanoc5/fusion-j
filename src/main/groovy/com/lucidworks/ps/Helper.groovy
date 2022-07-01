package com.lucidworks.ps

import org.apache.log4j.Logger
//import java.security.InvalidParameterException

/**
 * General helper class for Fusion-j client (another similar object for UpVal??)
 * todo consider refactoring flattening operations to a more obvious classname (low-priority as it is a low-level op, not big picture processing)
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
}

