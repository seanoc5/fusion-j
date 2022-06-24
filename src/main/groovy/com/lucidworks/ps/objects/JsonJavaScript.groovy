package com.lucidworks.ps.objects

import org.apache.commons.text.StringEscapeUtils

/**
 * @author :    sean
 * @mailto :    seanoc5@gmail.com
 * @created :   6/23/22, Thursday
 * @description: simple wrapper class to help convert from<->to json (json does not like (or allow) newlines in strings, so certain characters must be escaped
 * <br><a href='https://stackoverflow.com/questions/983451/where-can-i-find-a-list-of-escape-characters-required-for-my-json-ajax-return-ty/1021976#1021976'>Stack overflow discussion</a>
 *
 * todo - add a collection of `Statements` with some JS savvy parser that understands syntax (such as blocks, assignment statements, and multiline statements
 */

class JsonJavaScript {
    String source
    List<String> lines

    JsonJavaScript(String s) {
        source = s
        lines = s.split('\n')
    }

    /**
     * simple helper/wrapper to return 'human readable' version of escaped javascript
     * <br>Note: json does not like newlines in text, so it must be escaped (newlines become '\n', along with other items
     * @return human readable form ( real newlines and tabs)
     */
    String unEscapeSource() {
        StringEscapeUtils.unescapeEcmaScript(source)
    }

    /**
     * making this static, as we assume this Javascript object will deal with reading escaped source from json format,
     * this method
     * @param src
     * @return escaped string (ready for stuffing into a json output
     * <br>NOTE:
     */
    static String escapeSource(String src){
        StringEscapeUtils.escapeEcmaScript(src)
    }

}
