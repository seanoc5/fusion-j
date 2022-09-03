package com.lucidworks.ps.clients.experiment
//package org.apache.http.client.test



import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContextBuilder
import org.junit.Before
import org.junit.jupiter.api.Test

import java.security.KeyManagementException
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException
// todo move to actual test case

public class ApacheHttpClientTest {

    private HttpClient httpClient;

    @Before
    public void initClient() throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
        httpClient = HttpClients
                .custom()
                .setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build())
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .build();
    }

    @Test
    public void apacheHttpClient455Test() throws IOException {
        executeRequestAndVerifyStatusIsOk("https://expired.badssl.com");
        executeRequestAndVerifyStatusIsOk("https://wrong.host.badssl.com");
        executeRequestAndVerifyStatusIsOk("https://self-signed.badssl.com");
        executeRequestAndVerifyStatusIsOk("https://untrusted-root.badssl.com");
        executeRequestAndVerifyStatusIsOk("https://revoked.badssl.com");
        executeRequestAndVerifyStatusIsOk("https://pinning-test.badssl.com");
        executeRequestAndVerifyStatusIsOk("https://sha1-intermediate.badssl.com");
    }

    private void executeRequestAndVerifyStatusIsOk(String url) throws IOException {
        HttpUriRequest request = new HttpGet(url);

        HttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();

        assert statusCode == 200;
    }
}
