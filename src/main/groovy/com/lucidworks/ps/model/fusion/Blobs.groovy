package com.lucidworks.ps.model.fusion


import com.lucidworks.ps.model.BaseObject
import org.apache.log4j.Logger

import java.nio.file.Files
import java.nio.file.Path

/**
 * placeholder component app for Datasources (when/if necessary)
 * todo -- remove me, or make me useful, I am space at the moment....
 */
class Blobs extends BaseObject {
    Logger log = Logger.getLogger(this.class.name);

    Blobs(String applicationName, List<Map<String, Object>> items) {
        super(applicationName, items)
    }

    Blobs(String appName, Path blobsFolder){
        log.info "app($appName), path($blobsFolder) construnctor...."
        this.appName = appName
        srcItems = [:]
        blobsFolder.eachFileRecurse {Path itemPath ->
            String relativePath = blobsFolder.relativize(itemPath)
            if(Files.isDirectory(itemPath)){
                log.info "\t\tprocessing folder: $itemPath..."
            } else {
                log.info "\t\t\t\tPath item: $itemPath -- relativePath:$relativePath"
            }
            srcItems.put(relativePath, itemPath)
        }
    }

    @Override
    def export(File exportFolder) {
        log.info "Todo: more code to handle exporting  App($appName)  blobs..."
        return super.export(exportFolder)
    }

    @Override
    Map<String, Object> assessItem(def item) {
        Map assessItem = super.assessItem(item)
        String contentType = item.contentType
        if(contentType ==~ /(text|application.(json|zip)).*/){
            String msg = "This content type($contentType) is easy, no complexity... "
            assessItem.items << msg
            log.debug msg
        } else {
            log.info "More stuff here for Blob with contentType: $contentType"
            int cmplx = 1
            String msg = "Unknown blob contenttype($contentType), defaulting to complexity: $cmplx"
            assessItem.complexity = cmplx
            assessItem.items << msg
        }

        return assessItem
    }

  /*  def loadBlobs(){
        throw new IllegalArgumentException("Incomplete code -- add logic for loading blobs from... somewhere...?")
    }*/
}
