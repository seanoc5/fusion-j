package com.lucidworks.ps.model

import groovy.json.JsonSlurper
import org.apache.commons.text.StringEscapeUtils
import org.apache.log4j.Logger
/**
 * @author :    sean
 * @mailto :    seanoc5@gmail.com
 * @created :   6/26/22, Sunday
 * @description:
 */

/**
 * Wrapper-helper object for navigating and manipulating JsonSlurped objects.
 * <p>These are typically Maps and Lists with leaf-nodes having primative values
 * <p>Note: this differs from XMLObject; JsonObjects have hierarchy and then values at leaf-nodes, XMLObjects have attributes in the hierarchy, which means a different set of assumptions.
 * GPath works well for XML parsed objects, because the paths can/should allow access to attributes "along the way" in the hierarchy
 */
class JsonObject {
    static Logger log = Logger.getLogger(this.class.name);
    static final String DEFAULT_SEPARATOR = '/'
    String separator = DEFAULT_SEPARATOR
    Object slurpedItems            //   items from JsonSlurper
    Map<String, Object> flatPathMap

    JsonObject(String source, String separator = DEFAULT_SEPARATOR) {
        JsonSlurper slurper = new JsonSlurper()
        int srcSize = source.size()
        if (srcSize > 500 || source.contains('{')) {
            log.info "We seem to have json text, so use slurper.parseText()... "
            slurpedItems = slurper.parseText(source)
        } else {
            log.info "We seem to have constructor with source arg (string) pointing to source ($source), so use slurper.parse()... "
            slurpedItems = slurper.parse(source)
        }
        this.separator = separator
        flatPathMap = flattenWithLeafObject(slurpedItems, 0, separator)
    }

    JsonObject(Map sourceMap, String separator = DEFAULT_SEPARATOR) {
//            log.info "We seem to have constructor with source arg (${source.getClass().simpleName}) pointing to source ($source), so use slurper.parse()... "
        slurpedItems = sourceMap
//        flatPathMap = flattenWithLeafObject(slurpedItems, 0, separator)
        // todo -- revisit and refactor... some things work best with flattenWithLeafObject (leaf objects only), and some things (remove rules??) work better with additional paths pointing to non-leaf nodes
        flatPathMap = flattenWithObjects(slurpedItems, 0)           // trying out flattening with 'full' paths, including non-leaf objects
    }

    JsonObject(Object source, String separator = DEFAULT_SEPARATOR) {
//            log.info "We seem to have constructor with source arg (${source.getClass().simpleName}) pointing to source ($source), so use slurper.parse()... "
        JsonSlurper slurper = new JsonSlurper()
        log.info "We seem to have constructor with source arg (${source.getClass().simpleName}) pointing to source ($source), so use slurper.parse()... "
        slurpedItems = slurper.parse(source)
        flatPathMap = flattenWithLeafObject(slurpedItems, 0, separator)
    }

    /**
     * Find all the parsed/slurped items in this object matching path and value filters. If an arg is a pattern, then this will match via standard regex,
     * @param pathPattern string or pattern to match path
     * @param valuePattern string or pattern to match path, if valuePattern starts with equals '=' then it is an exact match, otherwise a string arg is value.contains(valuePattern) (case sensitive)
     * @return Map of items that match, same format as flatPathMap
     */
    Map<String, Object> findItems(def pathPattern, def valuePattern) {
        Map<String, Object> matchingPaths = findItemsByPath(pathPattern, flatPathMap)
        Map<String, Object> matchingItems = null
        if(matchingPaths) {
            if(valuePattern) {
                matchingItems = findItemsByValue(valuePattern, matchingPaths)
            } else {
                log.debug "\t\tNo value pattern given, skipping findItemsByValue... keeping findItemsByPath results: $matchingPaths"
                matchingItems = matchingPaths
            }
        } else {
            log.info "\t\tNo matching paths returned from findItemsByPath, so we are skipping the findItemsByValue() call"
        }

        return matchingItems
    }


    /**
     * get all JsonSlurped items by path
     * @param pathPattern if string object, do a basic string comparison, if Pattern then do regex compare
     * @param flatMapToSearch
     * @return
     */
    Map<String, Object> findItemsByPath(def pathPattern, Map<String, Object> flatMapToSearch) {
        Map<String, Object> matchingPaths = null
        if (!pathPattern || pathPattern == '.*') {
            log.info "\t\tpathPattern:($pathPattern) indicates using 'all' provided flatmap items in arg (${flatMapToSearch.keySet().size()})"
            matchingPaths = flatMapToSearch
        } else {
            String argType = pathPattern.getClass().simpleName
            log.info "${argType}:($pathPattern) used for regex matching against flatmap items in arg (${flatMapToSearch.keySet().size()})"
            matchingPaths = flatMapToSearch.findAll { String path, Object val ->
                boolean match = false
                if (pathPattern instanceof String) {
                    match = pathPattern == path
                    if(match) {
                        log.info "\t\t String path compare: $path == $pathPattern ?? ${match}"
                    } else {
                        log.debug "\t\t String path compare: $path == $pathPattern ?? ${match}"
                    }
                } else {
                    match = (path ==~ pathPattern)
                    if(match) {
                        log.info "\t\t Regex path compare: $path ==~ $pathPattern ?? ${match}"
                    } else {
                        log.debug "\t\t Regex pathcompare: $path ==~ $pathPattern ?? ${match}"
                    }
                }
                return match
            }
            if(!matchingPaths){
                def foo = getObjectNodeValue(pathPattern)
                matchingPaths = ["$pathPattern":foo]
                log.info "Tried getting matchingPaths via getObjectNodeValue($pathPattern): $matchingPaths"
            }
            if(matchingPaths) {
                log.info "\t\tfound (${matchingPaths.size()}) paths matching pathpattern: $pathPattern"
            } else {
                log.info "\t\tNo paths found matching path pattern: $pathPattern"
            }
        }
        return matchingPaths
    }

    Map<String, Object> findItemsByValue(def valuePattern, Map<String, Object> flatMapToSearch) {
        Map<String, Object> matchingItems = null
        if (!valuePattern || valuePattern == '.*') {
            log.info "\t\tvaluePattern:($valuePattern) indicates using 'all' provided flatmap items in arg (${flatMapToSearch.keySet().size()})"
            matchingItems = flatMapToSearch
        } else {
            log.debug "\t\tvaluePattern:($valuePattern) used for matching against flatmap items in arg (${flatMapToSearch.keySet().size()})"
            matchingItems = flatMapToSearch.findAll { def path, Object val ->
                boolean match = false
                if (valuePattern instanceof String) {
                    String v = val.toString()
                    if(valuePattern.startsWith('=')){
                        String vp=valuePattern[1..-1]
                        match = (vp==v)
                        if(match) {
                            log.info "\t\tvaluePattern ($valuePattern) started with an equals, so we removed that, and doing an exact match against value(${v}): $match"
                        } else {
                            log.debug "\t\tvaluePattern ($valuePattern) started with an equals, so we removed that, and doing an exact match against value(${v}): $match"
                        }
                    } else {
                        match = (v.contains(valuePattern))
                        if(match) {
                            log.info "\t\tValue:(${v}) contains valuePattern:($valuePattern)? -> $match"
                        } else {
                            log.debug "\t\tValue:(${v}) contains valuePattern:($valuePattern)? -> $match"
                        }
                    }

                } else {
                    match = (val ==~ valuePattern)
                    if(match) {
                        log.info "\t\t Regex value compare: $val ==~ $valuePattern ?? ${match}"
                    } else {
                        log.debug "\t\t Regex value compare: $val ==~ $valuePattern ?? ${match}"
                    }
                }
                return match
            }
        }
        return matchingItems
    }

    Object getObjectNodeValue(String path) {
        getObjectNodeValue(slurpedItems, path, separator)
    }


    /**
     * break the path into segments, walk the source object to get values,
     * try to handle maps AND collections(Lists)
     * @param srcMap
     * @param path
     * @param separator
     * @return
     */
    static Object getObjectNodeValue(Map srcMap, String path, String separator = DEFAULT_SEPARATOR) {
        log.debug "Get Node -- Process path: [$path] in src:$srcMap "
        List<String> segments = path.split(separator)
        if (!segments[0]) {
            segments.remove(0)
        }
        int numSegments = segments.size()
        def element = srcMap
        // loop through path segments, stop when reaching the last element, or a (parent) segment is null
        for (int depth = 0; depth < numSegments && element; depth++) {
            String seg = segments[depth]
            Object child = getChildElement(seg, element)
            log.debug "\t\t$depth - $seg) check if it exists... "
            if (!child) {
                log.info "\t\t${depth}) encountered MISSING segment: $seg -> return null, exit for loop..."
            } else {
                log.debug "\t\t$depth) Found segment:($seg) -> Child:$child   from parent element:($element)"
            }
            element = child

        }
        log.debug "\t\treturn Path ($path) element (value): $element "
        return element
    }


    /**
     * walk path segments and set the leaf value
     * if createIfMissing is true (default) will create element and any missing parents where necessary)
     * <p> the challenge comes in creating missing path segments, and setting the value in an List (especially a missing 'parent' list)
     * @param srcMap
     * @param path
     * @param valToSet
     * @param separator
     * @param createIfMissing
     * @return
     *
     * todo -- add syntax (parsing) to indicate "inserting" value into an array
     */
    static Map<String, Object> setObjectNodeValue(Map srcMap, String path, def valToSet, String separator = DEFAULT_SEPARATOR, boolean createIfMissing = true) {
        log.debug "Process SET path: [$path] to value ($valToSet) in src:$srcMap -- create if missing: $createIfMissing"
        List<String> segments = parsePath(path, separator)
        int numSegments = segments.size()
        Map<String, Object> result

        // start at the top, element will be set to each 'child' element as we walk the parsed segments
        def element = srcMap
        // loop through path segments, stop when reaching the last element, or a (parent) segment is null
        // todo -- confirm that we want parsePath(path) to yield a blank first element, e.g. parsePath('/id') -> ['','id'] -- this is awkward, but simplifies root slash handling
        for (int depth = 1; depth < numSegments && element != null; depth++) {
            String currentSegment = segments[depth]
            log.debug "\t\t$depth) process segment: $currentSegment in element:($element)..."
            Object child = getChildElement(currentSegment, element)
            if (child) {
                // child exists, if leaf node set the element in parent, otherwise just set the parent element's child value
                log.debug "\t\t\t\t$depth) we have a valid child ($child), and we are not at the leaf node (current segment: $currentSegment), keep iterating through path..."
//                element = child
                log.debug "shifted current element to child ($child) element we found"
                // if child existed (above) check if we are currently at the leaf node, if so: set the value
                if (isLeafTarget(depth, numSegments)) {
                    log.debug "\t\t$depth) found leaf node currentSegment:$currentSegment in path: $path, set to value: $valToSet"
                    def r = setLeafNodeValue(depth, currentSegment, path, valToSet, element)
                    result = ["${path}": r]
                    element = null      // redundant? should fail on for loop condition: depth < numSegments
                } else {
                    log.debug "\t\t$depth) still navigating path, currentSegment:($currentSegment) next segment: (${segments[depth + 1]})"
                    element = child
                }

            } else {
                if (isLeafTarget(depth, numSegments)) {
                    log.info "\t\tdepth:$depth) path: $path found  currentSegment:$currentSegment  SET VALUE: $valToSet"
                    def r = setLeafNodeValue(depth, currentSegment, path, valToSet, element)
                    result = ["${path}": r]

                } else {
                    log.debug "\t\tsegment depth $depth) found a missing 'child' for segment ($currentSegment), create it if createIfMissing($createIfMissing) is true..."
                    if (createIfMissing) {
                        String nextSegment = segments[depth + 1]
                        def newEmptyChild = [:]
                        if (nextSegment.isInteger()) {
                            newEmptyChild = []
                        }
                        child = createMissingNode(element, currentSegment, newEmptyChild)
//                        result = child      // remove me? should just be element = child??
                        element = child
                    } else {
                        log.warn "\t\t param: $createIfMissing is false, so we are leaving missing element ($currentSegment) as empty/null.."
                    }
                }
            }
        }

        log.debug "\t\treturn Path ( $path ) with updated element ($element) set to new value ($valToSet) -- result: $result"
        return result
    }


    /**
     * set the value of the given (JsonSlurped) 'node' -- this should handle differentiating list and/or map "parents"
     * @param depth
     * @param currentSegment
     * @param path the gpath-like path to dive into a JsonSlurped object (just maps/lists??)
     * @param valToSet intended for string/number, but perhaps also allows for more complex values to set???
     * @param element
     * @return the same valToSet param (as a success check), or null if failed???
     *
     * todo -- fix missing hierarchies -- currently returning each created segment flatly -- not correct
     */
    static String setLeafNodeValue(int depth, String currentSegment, String path, def valToSet, element) {
        def result
        log.debug "$depth) We are at the leafNode (seg: $currentSegment) in path($path) to set the value ($valToSet) in element ($element)..."
        if (element instanceof List) {
            if (currentSegment.isInteger()) {
                log.debug "Set LIST value ($valToSet) in element($element) with position: $currentSegment"
                Integer index = Integer.parseInt(currentSegment)
                List listElement = (List) element
                if (index > listElement.size() - 1) {
//                    String msg = "(List) element (size:${listElement.size()}) is smaller than index, bailing with error (for now... add more code to handle this!)"
                    String msg = "(List) element (size:${listElement.size()}) is smaller than index"
                    log.warn msg
//                    throw (new IllegalArgumentException(msg))
//                } else {
                }
                log.debug "set list element (${index}) to value: $valToSet"
                element[index] = valToSet
                result = valToSet
//                }
            } else {

                String msg = "We are in a List, but the segment ($currentSegment) is not an integer!! bailing!!!"
                log.warn msg
                throw (new IllegalArgumentException(msg))
            }

        } else if (element instanceof Map) {
            log.info "\t\t\t\tSet leafNode MAP key($currentSegment) to value ($valToSet) in parent map($element)"
            element[currentSegment] = valToSet
            // todo -- what makes sense for method return? testing approach of returning valToSet (srcMap should be updated in place)
            result = valToSet
        } else if (element instanceof String || element instanceof Integer) {
            element[currentSegment] = valToSet

        } else {
            String msg = "Unknown element type (${element.getClass().simpleName}), bailing!"
            log.warn msg
//            throw (new IllegalArgumentException(msg))
            element[currentSegment] = valToSet
        }
        return result
    }


    /**
     * simpple (override-able) method to parse the path (focused on slashy paths, but perhaps usable for dot notation??
     * @param path
     * @param separator
     * @return List of string segments to navigate (iterate through)
     */
    static List<String> parsePath(String path, String separator) {
        List<String> segments = path.split(separator)
        if (!segments[0]) {
            // todo -- decide on how to handle leading slash/separator...
            log.debug "Leading separator creates a blank for segment. This could be ok, as long as we are consistent: path:$path -> segments:$segments"
//            log.warn "Remove empty first segment because path ($path) starts with separator($separator)"
//            segments.remove(0)
        }
        segments
    }


    /**
     * simpple (override-able) method to check if we are at the 'goal' leaf node of the path, and just need to set the value
     * @param depth
     * @param numSegments
     * @return boolean (leaf node or no?)
     */
    static boolean isLeafTarget(int depth, int numSegments) {
        depth == (numSegments - 1)
    }


    /**
     * handle processing for map or list element
     * @param currentSegment the current <String> segment in the path
     * @param currentElement
     * @return JsonSurped 'node' (Map, List, leaf node...)
     */
    static def getChildElement(String currentSegment, Object currentElement) {
        def child
        if (currentSegment.isInteger()) {
            log.debug "segment ($currentSegment) looks like an int, converting to integer (assuming no Int-like map keys...): $currentSegment"
            child = currentElement[Integer.parseInt(currentSegment)]
        } else {
            child = currentElement[currentSegment]
            log.debug "segment ($currentSegment) looks like an map key, got child: $child"
        }
        child
    }


    /**
     * create a missing element in the path
     * @param parentElement
     * @param missingSegment
     * @param newEmptyChild defaults to empty map, send an empty list of the next element is list rather than map
     * @return new element (empty map or list)
     */
    static def createMissingNode(Object parentElement, String missingSegment, def newEmptyChild = [:]) {
        if (parentElement instanceof Collection) {
            if (missingSegment.isInteger()) {
                Integer index = Integer.parseInt(missingSegment)
//            List newList = new ArrayList(index)
//            newList[index] = newEmptyChild
                parentElement[index] = newEmptyChild
                log.info "\t\t\t\tset newEmptyChild to LIST for missing (Integer: $index) segment/index:($missingSegment) to parent 'element':($parentElement) with default empty child ($newEmptyChild)"
            } else {
                String msg = "invalid! parentelement($parentElement) is a collection, but missingSegment is not an integer!!!"
                log.warn msg
                throw new IllegalAccessException(msg)
            }
        } else {
//            log.debug "Leaving newEmptyChild as arg: $newEmptyChild"
            ((Map) parentElement).put(missingSegment, newEmptyChild)
            log.info "\t\tadd missing MAP entry for path segment($missingSegment) to parent 'element':($parentElement) with default empty child ($newEmptyChild)"

        }
        return newEmptyChild
    }


    /**
     * simple helper/wrapper to return 'human readable' version of escaped javascript
     * <br>Note: json does not like newlines in text, so it must be escaped (newlines become '\n', along with other items
     * @return human readable form ( real newlines and tabs)
     */
    static String unEscapeSource(String source) {
        StringEscapeUtils.unescapeEcmaScript(source)
    }

    /**
     * making this static, as we assume this Javascript object will deal with reading escaped source from json format,
     * this method
     * @param src
     * @return escaped string (ready for stuffing into a json output
     * <br>NOTE:
     */
    static String escapeSource(String src) {
        StringEscapeUtils.escapeEcmaScript(src)
    }

    /**
     * Flatten object, get path with object (reference?)
     * @param nested object (list and/or map) to flatten
     * @param level helper to track depth (is this helpful?)
     * @return Map with flattened path(string) as key, and the given object as value
     */
    static Map<String, Object> flattenWithLeafObject(def object, int level = 0, String prefix = DEFAULT_SEPARATOR, String separator = DEFAULT_SEPARATOR) {
        Map<String, Object> entries = [:]
        log.debug "$level) flattenPlusObject: $object..."
        if (object instanceof Map) {
//            def keyset = object.keySet()
            Map currentDepthMap = (Map) object
            currentDepthMap.each { String key, Object value ->
                log.debug "\t" * level + "$level)Key: $key"
                if (value instanceof Map || value instanceof List) {
                    level++
                    Map<String, Object> children = flattenWithLeafObject(value, level, '/', '/')
                    children.each { String child, Object childObject ->
                        String path = separator + key + child
                        if (childObject instanceof Map || childObject instanceof List) {
                            log.debug "skip this (${childObject.getClass().simpleName}"
                            entries[path] = null
                        } else {
                            entries[path] = childObject
                        }
                    }
                    log.debug "\t" * level + "submap keys: ${children}"
                } else {
                    entries[separator + key] = value
                    log.debug "\t\t$level) Leaf-node?? setting Map key($key) to value($value)"
//                    log.debug "\t" * level + "\t\tLeaf node: $key"
                }
            }
            log.debug "$level) after collect entries"

        } else if (object instanceof List) {
            log.debug "\t" * level + "$level) List! $object"
            List currentDepthList = (List) object
            currentDepthList.eachWithIndex { def val, int counter ->
                if (val instanceof Map) {
                    level++
                    def children = flattenWithLeafObject(val, level, prefix, separator)
                    children.each { String childName, Object childVal ->
                        String path = "${separator}${counter}${childName}"
                        log.debug "\t\tprocess child Path:($path) -- ChildVal:[$childVal] -- Skip??"
                        entries[path] = childVal
                    }
                    log.debug "\t" * level + "submap keys: ${children}"
                } else if (val instanceof List) {
                    String path = "${counter}${separator}${childName}"
                    log.warn "What do we do here? $path [$childVal] -- Skip??"
                    entries[path] = childVal

                } else {
                    log.debug "\t" * level + "$level:$counter) LIST value not a collection, leafNode? $val"
                    String path = "/${counter}"
                    entries[path] = val
                }
            }
            log.debug "done with list"
        } else {
            log.warn "$level) other?? $object"
        }
        return entries
    }


     Map<String, Object> flattenWithObjects(def object=slurpedItems, int level = 0) {
        Map<String, Object> entries = [:]
        log.info "\t" * level + "$level) flattenWithObjects: $object..."
        if (object instanceof Map) {
//            def keyset = object.keySet()
            Map currentDepthMap = (Map) object
            currentDepthMap.each { String key, Object value ->
                log.info "\t" * level + "$level)Key: $key"
                if (value instanceof Map || value instanceof List) {
                    level++
                    entries[separator + key] = value
                    Map<String, Object> children = flattenWithObjects(value, level)
                    children.each { String child, Object childObject ->
                        String path = separator + key + child
                        if (childObject instanceof Map || childObject instanceof List) {
                            log.info "\t" * level + "non-leaf entry:(${childObject.getClass().simpleName}: $childObject"
                            entries[path] = childObject
                        } else {
                            log.info "\t" * level+ "leaf entry:(${childObject.getClass().simpleName}: $childObject"
                            entries[path] = childObject
                        }
                    }
                    log.info "\t" * level + "submap keys: ${children}"
                } else {
                    entries[separator + key] = value
                    log.info "\t\t$level) Leaf-node?? setting Map key($key) to value($value)"
                }
            }
            log.info "$level) after collect entries: $entries"

        } else if (object instanceof List) {
            log.info "\t" * level + "$level) List! $object"
            List currentDepthList = (List) object
            currentDepthList.eachWithIndex { def val, int counter ->
                if (val instanceof Map) {
                    level++
                    def children = flattenWithObjects(val, level)
                    children.each { String childName, Object childVal ->
                        String path = "${separator}${counter}${childName}"
                        log.debug "\t\tprocess child Path:($path) -- ChildVal:[$childVal] -- Skip??"
                        entries[path] = childVal
                    }
                    log.debug "\t" * level + "submap keys: ${children}"
                } else if (val instanceof List) {
                    String path = "${counter}${separator}${childName}"
                    log.warn "What do we do here? $path [$childVal] -- Skip??"
                    entries[path] = childVal

                } else {
                    log.debug "\t" * level + "$level:$counter) LIST value not a collection, leafNode? $val"
                    String path = "/${counter}"
                    entries[path] = val
                }
            }
            log.info "done with list"
        } else {
            log.warn "$level) other?? $object"
        }
        return entries
    }


    /**
     * simple method to 'walk' Json Slurped object, and get element paths
     * @param object Json Slurped object (maps/lists)
     * @param level helper var to track depth in recursive calls
     * @return list of paths <String>s
     *
     * @deprecated use flattenWithLeafObject instead
     */
    static List<String> flatten(def object, int level = 0, String separator = '/') {
        log.warn "JS) Use flattenWithLeafObject in place of this outdated 'flatten' call..."
        List<String> entries = []
        log.debug "$level) flatten object: $object... with separator:$separator"
        if (object instanceof Map) {
            def keyset = object.keySet()
            Map map = (Map) object
            keyset.each { String key ->
                def value = map[key]
                log.debug "\t" * level + "$level)Key: $key"
                if (value instanceof Map || value instanceof List) {
                    level++
                    def children = flatten(value, level)
                    children.each { String child ->
                        entries << "${key}${separator}${child}".toString()
                    }
                    log.debug "\t" * level + "submap keys: ${children}"
                } else {
                    entries << "$key"
                    log.debug "\t" * level + "\t\tLeaf node: $key"
                }
                log.debug "next..."
            }
            log.debug "$level) after collect entries"

        } else if (object instanceof List) {
            log.debug "\t" * level + "$level) List! $object"
            List list = (List) object
            list.eachWithIndex { def val, int counter ->
                if (val instanceof Map || val instanceof List) {
                    level++
                    def children = flatten(val, level)
                    children.each { String child ->
                        entries << "[${counter}]${separator}${child}"
                    }
                    log.debug "\t" * level + "submap keys: ${children}"
                } else {
                    log.debug "\t" * level + "$level:$counter) List value not a collection, leafNode? $val"
                    entries << "[${counter}]"
                }
            }
            log.debug "done with list"
        } else {
            log.warn "$level) other?? $object"
        }
        return entries
    }


    /**
     * code to delete map entry or list item (based on provided path/object structure
     * @param path full path to remove
     * todo - consider (regex?) pattern matching here? -- or leave that to the parent code, and just do single item removals here...?
     */
    static def removeItem(String path, Map fromMap, String separator = DEFAULT_SEPARATOR) {
        List<String> segments = parsePath(path, separator)
        def itemToRemove = null
        String lastSeg = segments[-1]
        String parentPath = getParentPath(path)
        def parentObject = getObjectNodeValue(fromMap, parentPath, separator)
//        def parent = getParentItem(path, fromMap)
        if (parentObject instanceof Collection) {
            if (lastSeg.isInteger()) {
                List list = (List) parentObject
                Integer idx = Integer.parseInt(lastSeg)
                int origSize = list.size()
                if (idx < origSize) {
                    // todo -- revisit index removal...?
                    log.debug "\tBe careful removing list items by index ($lastSeg)... doing so likely to change the list, and bork any other removals based on index (i.e. no promise we process in descending order)"
                    itemToRemove = list.get(idx)
                    def foo = list.remove(idx)
                    int newSize = list.size()
                    log.info "List originally had ($origSize) items, removed item with index($idx): $itemToRemove -- new size: $newSize"
                } else {
                    log.warn "Last segment seems to be an array index, and it is not < array size (array out of bounds situation), this may be from a previous remoteItem in the same list which borked the index of the Collection "
                }
            } else {
                log.warn "We have a list as the parent($parentObject), and last segment ($lastSeg) is not an integer!!! cannot remove!!!"
            }
        } else if (parentObject instanceof Map) {
            Map map = (Map) parentObject
            itemToRemove = map.get(lastSeg)         // todo -- minor refactoring, duplicate/confusing use of itemToRemove...
            if (itemToRemove) {
                log.debug "\t\tremove map entry($lastSeg) with item ($itemToRemove) from parent map: $map"
                itemToRemove = map.remove(lastSeg)
                log.debug "\t\tremoved $itemToRemove"
            } else {
                log.warn "Could not get element ($lastSeg) from map with keyset: ${map.keySet()}!!! Nothing to remove...?"
            }
        } else {
            log.warn "Unknown parent ($parentObject) -- not collection nor map...? Cannot remove path: $path"
        }
        Map result = [:]
        result.put(path, itemToRemove)      // GString hassles building result map properly... trying this approach...
        return result
    }

    static String getParentPath(String path, String separator = DEFAULT_SEPARATOR) {
        List<String> segments = parsePath(path, separator)
        segments.removeLast()
        String parentPath = segments.join(separator)
    }

    static def getParentItem(String path, Map flatPaths, String separator = DEFAULT_SEPARATOR) {
        String parentPath = getParentPath(path, separator)
        def parent = flatPaths[parentPath]
    }

    /**
     * get array index leafs in decreasing order so things like removing an item don't step on similar operations in same list)
     * @param flatMapToSort
     * @param separator
     * @return sorted collection of map keys so that array elements occur in decreasing order
     */
    static Set<String> orderIndexKeysDecreasing(Map flatMapToSort, String separator = '/') {
        Set keys = flatMapToSort.keySet()
        Map<Integer, List<String>> arrayLeafsGrouped = [:].withDefault { [] }
        // group array leaf paths by integer index, the we can walk those groups in reverse order -- only relevant for 'remove' functionality???
        List<String> mapLeafList = []
        keys.each { String k ->
            log.debug "Process key: $k"
            List<String> parts = k.split(separator)
            String lastItem = parts[-1]
            if (lastItem.isInteger()) {
                Integer idx = Integer.parseInt(lastItem)
                log.debug "\t\torderIndexKeysDecreasing Index: $idx"
                arrayLeafsGrouped[idx] << k
            } else {
                log.debug "\t\torderIndexKeysDecreasing Map key (no sorting/grouping needed): $k"
                mapLeafList << k
            }
        }

        LinkedHashSet<String> myOrderSet = new LinkedHashSet<>()
        List orderedList = arrayLeafsGrouped.keySet().toList().reverse()
        orderedList.each { Integer i ->
            List groupedItems = arrayLeafsGrouped[i]
            log.debug "orderIndexKeysDecreasing $i) -> $groupedItems"
            groupedItems.each {
                log.debug "orderIndexKeysDecreasing $i) Add key: $it"
                myOrderSet << it
            }
        }
        mapLeafList.each {
            myOrderSet << it
        }

        return myOrderSet
    }


    /**
     * walk through all flattened entries, and look at the
     * @param pattern
     * @param flatpathItems
     * @return submap of matching flatpath entries
     *
     * todo -- consider returning match object??? premature optimization? just match regexes here, and potential subsequent transform action. (the latter is the current approach)
     */
/*    Map<String, Object> findAllItemsMatching(String pathPattern, def valuePattern, Map<String, Object> flatpathItems) {
        Map<String, Object> matchingFlatPaths = null
        def keys = flatpathItems.keySet()

        def matchingPaths
        if (pathPattern == '.*' || !pathPattern) {
            log.debug "\t\tShortcut: setting matching paths to all flat paths based on source pathPattern:$pathPattern (?'.*' or empty??)"
            matchingPaths = flatpathItems
        } else {
            matchingPaths = flatpathItems.findAll { String path, Object val ->
                path ==~ pathPattern
            }
            log.info "\t\tpathPattern($pathPattern) matched: $matchingPaths"
        }

        if (valuePattern) {
            log.debug "We have a valuePattern to further file"
            boolean tildeOperator = valuePattern.startsWith('~')
            if (tildeOperator) {
                valuePattern = valuePattern[1..-1]          //strip tilde and do a string contains search below
                log.debug "\t\tdoing String.contains() search (tildeOperator=true)"
            } else {
                log.debug "\t\tdoing pattern($pathPattern) search with groovy string regex (should handle regex and groups, likely more extended regex...)"
            }

            matchingFlatPaths = flatpathItems.subMap(matchingPaths.keySet()).findAll { String key, def val ->
                boolean valMatches = false
                if (tildeOperator) {
                    valMatches = ((String) val).contains(valuePattern)
                } else {
                    def valMatch = (val =~ valuePattern)
                    if (valMatch.matches()) {
                        log.debug "Add matcher to return thingie here? not unless things are slow with re-matching in doing the actual rule..."
                        valMatches = true
                    }
                }
                return valMatches
            }
            log.info "\t\tpathPattern:$pathPattern :: valuePattern: $valuePattern => Filtered ${matchingPaths.size()} matching paths to ${matchingFlatPaths.size()} matches by value matching, matches: $matchingFlatPaths"

        } else {
            Set matchingPathKeys = matchingPaths.keySet()
            matchingFlatPaths = flatpathItems.subMap(matchingPathKeys)
            assert matchingFlatPaths.size() == matchingPaths.size()
            log.debug "\t\tNo valuePattern given, we will return just the path matches..."
        }

        return matchingFlatPaths
    }*/

}

