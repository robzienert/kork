/*
 * Copyright 2015 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.netflix.spinnaker.config

import com.netflix.spinnaker.security.AuthenticatedRequest
import com.squareup.okhttp.ConnectionSpec
import com.squareup.okhttp.OkHttpClient
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import retrofit.RequestInterceptor

import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.util.concurrent.TimeUnit

@Slf4j
@CompileStatic
@Configuration
@ConfigurationProperties(prefix="okHttpClient")
class OkHttpClientConfiguration {
  long connectTimoutMs = 15000
  long readTimeoutMs = 20000

  boolean propagateSpinnakerHeaders = true

  File keyStore
  String keyStoreType = 'PKCS12'
  String keyStorePassword = 'changeit'

  File trustStore
  String trustStoreType = 'PKCS12'
  String trustStorePassword = 'changeit'

  String secureRandomInstanceType = "NativePRNGNonBlocking"

  List<String> tlsVersions = ["TLSv1.2", "TLSv1.1", "TLSv1"]
  List<String> cipherSuites = [
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
    "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
    "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
    "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA"
  ]

  /**
   * @return RequestInterceptor that will propagate Spinnaker headers if <code>propagateSpinnakerHeaders</code> is true
   */
  @Bean
  RequestInterceptor spinnakerRequestInterceptor() {
    return new RequestInterceptor() {
      @Override
      void intercept(RequestInterceptor.RequestFacade request) {
        if (!propagateSpinnakerHeaders) {
          // noop
          return
        }

        AuthenticatedRequest.authenticationHeaders.each { String key, Optional<String> value ->
          if (value.present) {
            request.addHeader(key, value.get())
          }
        }
      }
    }
  }

  /**
   * @return OkHttpClient w/ <optional> key and trust stores
   */
  OkHttpClient create() {
    def okHttpClient = new OkHttpClient()
    okHttpClient.setConnectTimeout(connectTimoutMs, TimeUnit.MILLISECONDS)
    okHttpClient.setReadTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
    okHttpClient.setRetryOnConnectionFailure(false)

    if (!keyStore && !trustStore) {
      return okHttpClient
    }

    def sslContext = SSLContext.getInstance('TLS')

    def keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
    def ks = KeyStore.getInstance(keyStoreType)
    keyStore.withInputStream {
      ks.load(it as InputStream, keyStorePassword.toCharArray())
    }
    keyManagerFactory.init(ks, keyStorePassword.toCharArray())

    def trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
    def ts = KeyStore.getInstance(trustStoreType)
    trustStore.withInputStream {
      ts.load(it as InputStream, trustStorePassword.toCharArray())
    }
    trustManagerFactory.init(ts)

    def secureRandom = new SecureRandom()
    try {
      secureRandom = SecureRandom.getInstance(secureRandomInstanceType)
    } catch (NoSuchAlgorithmException e) {
      log.error("Unable to fetch secure random instance for ${secureRandomInstanceType}", e)
    }

    sslContext.init(keyManagerFactory.keyManagers, trustManagerFactory.trustManagers, secureRandom)
    okHttpClient.setSslSocketFactory(sslContext.socketFactory)

    return applyConnectionSpecs(okHttpClient)
  }

  @CompileDynamic
  private OkHttpClient applyConnectionSpecs(OkHttpClient okHttpClient) {
    def cipherSuites = (cipherSuites ?: ConnectionSpec.MODERN_TLS.cipherSuites()*.javaName) as String[]
    def tlsVersions = (tlsVersions ?: ConnectionSpec.MODERN_TLS.tlsVersions()*.javaName) as String[]

    def connectionSpec = new ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
      .cipherSuites(cipherSuites)
      .tlsVersions(tlsVersions)
      .build()

    return okHttpClient.setConnectionSpecs([connectionSpec, ConnectionSpec.CLEARTEXT] as List<ConnectionSpec>)
  }
}
