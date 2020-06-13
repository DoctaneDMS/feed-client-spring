package com.softwareplumbers.feed.rest.client.spring;

import com.softwareplumbers.feed.rest.client.spring.SignedRequestLoginHandler;
import com.softwareplumbers.feed.rest.client.spring.SecretKeys;
import com.softwareplumbers.feed.rest.client.spring.KeyPairs;
import com.softwareplumbers.feed.rest.client.spring.LoginHandler;
import com.softwareplumbers.feed.rest.client.spring.FeedServiceImpl;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.keymanager.KeyManager;
import java.security.KeyStoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author jonathan
 */
public class LocalConfig {
   
    @Autowired
	Environment env;    
    
    @Bean
    public KeyManager keyManager() throws KeyStoreException { 
        KeyManager<SecretKeys, KeyPairs> keyManager = new KeyManager<>();
        keyManager.setLocation("/var/tmp/doctane-proxy.keystore");
        keyManager.setPassword(env.getProperty("doctane.keystore.password"));
        keyManager.setRequiredSecretKeys(SecretKeys.class);
        keyManager.setRequiredKeyPairs(KeyPairs.class);
        return keyManager;
    }
    
    @Bean LoginHandler loginHandler(KeyManager keyManager) throws KeyStoreException {
        SignedRequestLoginHandler handler = new SignedRequestLoginHandler();
        handler.setKeyManager(keyManager);
        handler.setAuthURI("http://localhost:8080/auth/tmp/service?request={request}&signature={signature}");
        handler.setRepository("tmp");
        return handler;
    }
    
    @Bean
    FeedService testService(LoginHandler loginHandler) {
        return new FeedServiceImpl(
            "http://localhost:8080/feed/tmp/",
            loginHandler
        );
    }    
}
