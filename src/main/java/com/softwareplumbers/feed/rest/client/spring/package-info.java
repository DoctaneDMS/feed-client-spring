/** Doctane client implemented over Spring REST API.
 * 
 * This package implements a simple Doctane client which uses the Spring REST
 * APIs to connect to a Doctane server.
 * 
 * The client will typically be configured as a Spring bean. A sample java configuration
 * for connecting to a local doctane server is included below:
 * 
 * ```
 * {@literal @}Bean
 *  public KeyManager keyManager() throws KeyStoreException { 
 *      KeyManager<SecretKeys, KeyPairs> keyManager = new KeyManager<>();
 *      keyManager.setLocation("/var/tmp/doctane-proxy.keystore");
 *      keyManager.setPassword(env.getProperty("doctane.keystore.password"));
 *      keyManager.setRequiredSecretKeys(SecretKeys.class);
 *      keyManager.setRequiredKeyPairs(KeyPairs.class);
 *      return keyManager;
 *  }
 *  
 * {@literal @}Bean 
 *  LoginHandler loginHandler() throws KeyStoreException {
 *      SignedRequestLoginHandler handler = new SignedRequestLoginHandler();
 *      handler.setKeyManager(keyManager());
 *      handler.setAuthURI("http://localhost:8080/rest-server-filenet/auth/test/service?request={request}&signature={signature}");
 *      handler.setRepository("test");
 *      return handler;
 *  }
 *  
 * {@literal @}Bean
 *  public RepositoryService testService() throws KeyStoreException {
 *      DocumentServiceImpl service = new DocumentServiceImpl();
 *      service.setDocumentAPIURL("http://localhost:8080/rest-server-filenet/docs/test/");
 *      service.setLoginHandler(loginHandler());
 *      return service;
 *  }
 * ```
 * 
 * 
 */
package com.softwareplumbers.feed.rest.client.spring;
