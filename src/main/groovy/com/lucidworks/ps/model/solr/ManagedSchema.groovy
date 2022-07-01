package com.lucidworks.ps.model.solr

import groovy.json.JsonSlurper
import groovy.xml.XmlParser
import org.apache.log4j.Logger

import java.security.InvalidParameterException
import java.util.regex.Pattern

/**
 * wrapper class to help with solr schema parsing and operations
 */
class ManagedSchema {
    public static final Pattern OVERRIDE_FIELDNAMES = ~/id|_version_|_raw_content_|_root_/
    Logger log = Logger.getLogger(this.class.name);
    /** source of content, if string, these will be equal, if file, or url, that will be different */
    def source
    /** informal label of this schema (typically collection name? maybe with Fusion app name? */
    String label
    String content
    Map schemaMap = [:]
    Node xmlSchema
    List<Node> fieldTypes
    List<Node> schemaDynamicFieldDefinitions
    List<Node> schemaFields
    Map<String, Map> lukeMap
    Map<String, Map> lukeFields
    Map knownfields = [:].withDefault { [:] }
    // "withDefault so we can easily merge both schema-defined and luke-base details

    /**
     * simple constructor with just the managed schema source (file, url, string,...?)
     * @param src
     */
    ManagedSchema(def src, String label = 'unknown') {
        log.debug "Simple constructor WITHOUT luke param (missing luke means less helpful analysis on what is used and not used)"
        source = src
        parseSchema(src)
    }


    /**
     * Include luke output for more accurate snapshot of fields in use, including implicit dynamicFields
     * @param src
     * @param lukeOutput
     */
    ManagedSchema(def src, String label = 'unknown', def lukeOutput) {
        log.debug "Src(${src.getClass().simpleName}) AND luke output (${lukeOutput.getClass().simpleName})"
        if(src instanceof  String){
            if(src[0..20].contains(XML_START_TAG)){
                source = "XML Source string"
            } else {
                source = 'Unexpected Source string...'
            }
        } else {
            // file? url?....
            source = src
        }
        parseSchema(src)
        parseLukeOutput(lukeOutput)
    }


    /**
     * assume XML format, create the wrapper object, and return that (as something to check/review)
     * @param src
     * @return ManagedSchema -- parsed wrapper object to encapsulate some functionality and relevant structure
     * @throws InvalidParameterException
     */
    def parseSchema(def src) throws InvalidParameterException {
        content = getSchemaContent(src)
        List<String> lines = content.split('\n')
        // better way to determine json or xml? should we just have two different classes or methods?

        if (lines[0].contains('xml')) {
            log.debug "Source (${src} appears to be xml, parse with XMLParser (not xml slurper)"
            XmlParser parser = new XmlParser()
            xmlSchema = parser.parseText(content)
            schemaFields = collectSchemaFields()
            Map schemaFields = schemaFields.collectEntries {
                String name =it.attribute('name')
                [name: [schemaNode: it]]
            }
            knownfields = knownfields + schemaMap       // avoid using a reference, want a copy of the definedFields...
            schemaDynamicFieldDefinitions = collectDynamicFieldsDefinitions()
            fieldTypes = collectSchemaFieldTypes()
        } else if (lines[0].contains('{')) {
            log.warn "File (${src} appears to be JSON, parse with JsonSlurper (untested code: Json source...!!!)"
            JsonSlurper slurper = new JsonSlurper()
            schemaMap = slurper.parse(src)
        }
        return schemaMap
    }

    String getSchemaContent(Serializable src) {
        String content
        if (src instanceof File) {
            content = src.text
        } else if (src instanceof URL) {
            content = ((URL) src).text
        } else if (src instanceof String) {
            content = src
        } else {
            throw new InvalidParameterException("Src (${src.getClass().simpleName}) param was not expected type: (File, URL, String), bailing!")
        }
        content
    }

    /**
     * parse luke output and include parsed field information
     * this can help verify what (dynamic) fields are in the index, but not used (e.g. < n docs, like doc=0?)
     * @param lukeSource json export/file from solr admin luke request
     * @return the parsed list of fields from luke
     */
    def parseLukeOutput(def lukeSource) {
        log.debug "Parsing output of 'luke' request handler(JSON only!) : $lukeSource"
        JsonSlurper slurper = new JsonSlurper()
        lukeMap = slurper.parse(lukeSource)
        lukeFields = lukeMap.fields
        lukeFields.each { String fieldName, lukefieldDetails ->
            def map = knownfields[fieldName] ?: [:]
            if (map) {
                log.debug "Combine existing: $map (schema) with luke details $lukefieldDetails "
            } else {
                log.debug "Dynamic only? $fieldName"
            }
            knownfields[fieldName] = map + lukefieldDetails         // combine previous schema info (if exists) with luke info
        }
        return lukeMap      // return just boolean success? void?
    }


    /**
     * use luke output to see what fields are actually in use
     * @param mindDocs minimum number of docs to be considered a 'used' field
     * @param overrideAsUsed -- pattern to keep matching fields as 'used' regardless of luke information
     * @return
     */
    def findUsedFieldsLuke(int mindDocs = 1, Pattern overrideAsUsed = OVERRIDE_FIELDNAMES) {
        if (!lukeFields) {
            log.warn "No luke map/fields found!! Currently doesn't make sense to compare without luke, bailing..."
            return null
        }
        Map<String, Map<String, Object>> fieldNames = [:].withDefault { [:] }
        lukeFields.each { String fieldName, def lukeFieldInfo ->
            if (lukeFieldInfo.docs >= mindDocs) {
                log.debug "field ${lukeFieldInfo} had ${lukeFieldInfo.docs} which is >= min docs: $mindDocs"
                fieldNames[fieldName].luke = lukeFieldInfo
            } else if (fieldName ==~ overrideAsUsed) {
                log.debug "adding special field: $fieldName even though luke says there are no docs..."
                fieldNames[fieldName].luke = lukeFieldInfo
            } else {
                log.debug "\t\tLuke says field: $fieldName has no documents, consider it defined but not used"
            }
        }
        return fieldNames
    }


    /**
     *  return all docs (from luke output) the either have a minimum # of docs, or match some pattern signifying: consider 'used' by name match
     * @param minDocs some number (0+) test to consider a field "used" (helps find edge case fields that are technically used, but should be removed
     * @param overrideAsUsed
     * @return list of fields that can/should be considered unused (and potentially removed in other code??)
     */
    def findUnusedLukeFields(int minDocs = 0, Pattern overrideAsUsed = OVERRIDE_FIELDNAMES) {
        if (!lukeFields) {
            log.warn "No luke map/fields found!! Currently doesn't make sense to compare without luke, bailing..."
            return null
        }

        Map<String, Map<String, Object>> usedFields = findUsedFieldsLuke()
        def unused = lukeFields.findAll { String fieldName, def lukeFieldInfo ->
            if (lukeFieldInfo.docs > minDocs) {
                return false
            } else if (fieldName ==~ overrideAsUsed) {
                return false
            } else {
                return true
            }
        }
        return unused
    }


    def collectSchemaFieldTypes() {
        def fieldTypes = xmlSchema.'**'.findAll { Node node ->
            node.name() == 'fieldType'
        }
        return fieldTypes
    }

    def collectSchemaFields() {
        def fields = xmlSchema.'**'.findAll { Node node ->
            // slightly hackish combining the filtering and populating 'usedFields', but I'm bored and want to see if it works...
            String nodeName = node.name()
            if (nodeName == 'field') {
                String fieldName = node.attribute('name')
                String type = node.attribute('type')
                knownfields[fieldName] = [name: fieldName, type: type, schemaNode: node]
                return true
            } else {
                return false
            }
        }
        return fields
    }

    /**
     * get a list of dynamic field (definitions) -- often compared to usedFields so we can remove the cruft of unused dynamicFields
     * @return
     */
    List<Node> collectDynamicFieldsDefinitions() {
        schemaDynamicFieldDefinitions = xmlSchema.'**'.findAll { Node node ->
            node.name() == 'dynamicField'
        }
    }

    /**
     * get all the (default) dynamic field definitions, and return those that not actually used compared to the luke request handler output
     * @param lukeFields
     */
    def findUnusedDynamicfields() {
        if (!lukeFields) {
            log.warn "No luke map/fields found!! Currently doesn't make sense to compare without luke, bailing..."
            return null
        }

        def dynamicFieldNames = schemaDynamicFieldDefinitions.collect { Node n ->
            n.attributes()['name']
        }
        def lukeDynamicBases = lukeFields.findAll { String key, Object val ->
            def db = val.dynamicBase
            return db
        }.collect { String key, Object val ->
            val.dynamicBase
        }.groupBy { it }.keySet()
        def unused = dynamicFieldNames - lukeDynamicBases
        return unused
    }

    def removeUnusedDynamicFields(List<String> unusedDynamicFieldNames) {
        Map results = [removed: 0, problems: 0]
        unusedDynamicFieldNames.each { String name ->
            Node n = xmlSchema.'**'.find { it.name() == 'dynamicField' && it.attribute('name') == name }
            if (n) {
                log.debug "found: $n, go to parent, and remove the child"
                def success = n.parent().remove(n)
                if (success) {
                    results.removed++
                } else {
                    results.problems++
                }
            } else {
                log.warn "Could not find (unused dynamic field def) XML node: $name"
                results.problems++
            }
        }
        return results
    }

    def removeUnusedFieldsTypes(List<String> unusedFieldTypes) {
        Map results = [removed: 0, problems: 0]
        unusedFieldTypes.each { String name ->
            Node n = xmlSchema.'**'.find { it.name() == 'fieldType' && it.attribute('name') == name }
            if (n) {
                log.debug "found: $n, go to parent, and remove the child"
                def success = n.parent().remove(n)
                if (success) {
                    results.removed++
                } else {
                    results.problems++
                }
            } else {
                log.warn "Could not find (unused fieldType): $name"
                results.problems++
            }
        }
        return results
    }


    /**
     * get all the (default?) field types that are defined, but not actually used
     * Assumed 'used' if: luke shows field with > 0 docs, or explicitly defined in schema
     */
    def findUnusedFieldTypes() {
        List<String> fieldTypes = collectSchemaFieldTypes().collect { it.attribute('name') }
        Set<String> usedFieldTypes = knownfields.collect { String name, Map details ->
            details.type
        }.toSet()
        List<String> unusedFieldTypes = fieldTypes - usedFieldTypes
        return unusedFieldTypes
    }

    String toString() {
        String s = ""
    }
}
