//package org.apache.http.client.test

import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.conn.ssl.TrustAllStrategy
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContextBuilder

// todo move to actual test case
/*HttpClient simpleClient = java.net.http.HttpClient.newBuilder()
               .cookieHandler(new CookieManager())
               .version(java.net.http.HttpClient.Version.HTTP_1_1)
               .build()*/
HttpClient simpleClient = HttpClients
        .custom()
//        .setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build())
//        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .build()

HttpClient trustingCliet = HttpClients
        .custom()
        .setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build())
        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .build()

//        executeRequestAndVerifyStatusIsOk("https://expired.badssl.com");
//        executeRequestAndVerifyStatusIsOk("https://wrong.host.badssl.com");
//        executeRequestAndVerifyStatusIsOk("https://self-signed.badssl.com");
//        executeRequestAndVerifyStatusIsOk("https://revoked.badssl.com");
//        executeRequestAndVerifyStatusIsOk("https://pinning-test.badssl.com");
//        executeRequestAndVerifyStatusIsOk("https://sha1-intermediate.badssl.com");

String url = "https://untrusted-root.badssl.com"
HttpUriRequest request = new HttpGet(url)



HttpResponse simpleResponse = simpleClient.execute(request)
int statusCode1 = simpleResponse.getStatusLine().getStatusCode()
assert statusCode1 == 405

HttpResponse trustingResponse = trustingCliet.execute(request)
int statusCode2 = trustingResponse.getStatusLine().getStatusCode()

assert statusCode == 200
