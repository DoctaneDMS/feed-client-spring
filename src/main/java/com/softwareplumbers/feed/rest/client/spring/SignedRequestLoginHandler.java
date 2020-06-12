package com.softwareplumbers.feed.rest.client.spring;

import java.net.HttpCookie;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Optional;
import java.util.List;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import com.softwareplumbers.keymanager.KeyManager;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/** Handle login to the doctane proxy.
 * 
 * This implementation of LoginHandler uses a token signed by a private key to
 * authenticate itself to the Doctane server. The private key is store in a JCEKS
 * format keystore (which is managed via the KeyManager component).
 * 
 * @author SWPNET\jonessex
 *
 */
public class SignedRequestLoginHandler implements LoginHandler {

    //------ private static variables -------//
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(SignedRequestLoginHandler.class);

    private static String BASE_COOKIE_NAME="DoctaneUserToken";

    //------ private variables -------//

    private HttpCookie authCookie;
    private KeyManager<SecretKeys,KeyPairs> keyManager;
    private UriTemplate authURI;
    private String cookieName;

    //------ private static methods -------//
    
    private static byte[] formatAuthRequest(String serviceAccount) {
        LOG.entry(serviceAccount);
        JsonObjectBuilder authRequest = Json.createObjectBuilder();
        authRequest.add("instant", System.currentTimeMillis());
        authRequest.add("account", serviceAccount);
        return authRequest.build().toString().getBytes();
    }
    
    private static Optional<HttpCookie> getCookieFromResponse(String cookieName, ResponseEntity<?> response) {
        return response.getHeaders().get("Set-Cookie").stream()
            .map(HttpCookie::parse)
            .flatMap(List::stream)
            .filter(cookie -> cookieName.equals(cookie.getName()))
            .findAny();
    }

    //------ private methods ------///
    
    private byte[] signAuthRequest(byte[] request, KeyPairs serviceAccount) {
        LOG.entry(request, serviceAccount);
        PrivateKey key = keyManager.getKeyPair(serviceAccount).getPrivate();
        Signature sig;
        try {
            sig = Signature.getInstance("SHA1withDSA", "SUN");
            sig.initSign(key);
            sig.update(request);
            byte[] result = sig.sign();
            LOG.exit("<redacted>");
            return result;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException e) {
            LOG.catching(e);
            throw LOG.throwing(new IllegalArgumentException("cannot find an appropriate key pair for " + serviceAccount));
        } catch (SignatureException e) {
            LOG.catching(e);
            throw LOG.throwing(new RuntimeException(e));
        }
    }
    
    private static String extractName(X509Certificate cert) {
        String dn = cert.getSubjectDN().getName();
        return (dn.startsWith("CN=") || dn.startsWith("cn=")) ? dn.substring(3) : dn; 
    }
        
    private Optional<HttpCookie> getCookieFromServer() {
        LOG.entry();
        RestTemplate restTemplate = new RestTemplate();
        X509Certificate cert = keyManager.getCertificate(KeyPairs.DEFAULT_SERVICE_ACCOUNT);
        byte[] authRequestBytes = formatAuthRequest(extractName(cert));
        byte[] signature = signAuthRequest(authRequestBytes, KeyPairs.DEFAULT_SERVICE_ACCOUNT);
        Encoder base64 = Base64.getUrlEncoder();
        String authRequestBase64 = base64.encodeToString(authRequestBytes);
        String sigBase64 = base64.encodeToString(signature);
        URI authRequest = authURI.expand(authRequestBase64, sigBase64); 
        ResponseEntity<String> response = restTemplate.exchange(authRequest, HttpMethod.GET, null, String.class);
        Optional<HttpCookie> result = getCookieFromResponse(cookieName, response);
        return LOG.exit(result);
    }
    
    //------- public methods ------//
    
    /** Create a new LoginHandler
     * 
     * @param keyManager A key manager that contains a key for DEFAULT_SERVICE_ACCOUNT
     * @param authURI The URI for the authorization service
     * @param repository The repository which we are accessing
     */
    public SignedRequestLoginHandler(KeyManager<SecretKeys,KeyPairs> keyManager, UriTemplate authURI, String repository) {
        LOG.entry(keyManager, authURI, repository);
        this.keyManager = keyManager;
        this.authURI = authURI;
        this.cookieName = "DoctaneUserToken/"+repository;
        LOG.exit();
    }
    
    /** Null-arg constructor for Spring */
    public SignedRequestLoginHandler() {
        LOG.entry();
        this.keyManager = null;
        this.authURI = null;
        this.cookieName = null;
        LOG.exit();
    }
    
    /** Set the key manager.
     * 
     * Mainly for Spring configuration. Java code should use the three-argument constructor.
     * 
     * @param keyManager Key manager for this Doctane client.
     */
    @Required
    public void setKeyManager(KeyManager<SecretKeys, KeyPairs> keyManager) { 
        LOG.entry(keyManager);
        this.keyManager = keyManager;
        LOG.exit();
    }

    /** Set the authentication URI.
     * 
     * Mainly for Spring configuration. Java code should use the three-argument constructor.
     * 
     * @param authURI the URI on the Doctane server responsible for handling this authentication protocol.
     */
    @Required
    public void setAuthURI(String authURI) { 
        LOG.entry(authURI);
        this.authURI = new UriTemplate(authURI);
        LOG.exit();
    }
    
    /** Set the repository for which we are authenticating.
     * 
     * Mainly for Spring configuration. Java code should use the three-argument constructor.
     * @param repository The Doctane repository for which we are authenticating
     */
    @Required
    public void setRepository(String repository) { 
        LOG.entry(repository);
        this.cookieName = "DoctaneUserToken/"+repository;
        LOG.exit();
    }
    
    /** Apply credentials to a request.
     * 
     * Function will perform a login if necessary and apply the resulting credentials to the given request.
     * 
     * @param mainRequest The request that requires authentication information.
     */
    @Override
    public void applyCredentials(HttpHeaders mainRequest) {
        LOG.entry();
        if (authCookie == null || authCookie.hasExpired()) {
            Optional<HttpCookie> cookie = getCookieFromServer();
            if (cookie.isPresent()) authCookie = cookie.get();
        }
        if (authCookie != null)
            mainRequest.add("Cookie", authCookie.toString());
        LOG.exit();
    }
    
    /** Get credentials 
     * 
     * @return Credentials cookie as a string.
     */
    @Override
    public String getCredentials() {
        LOG.entry();
        if (authCookie == null || authCookie.hasExpired()) {
            Optional<HttpCookie> cookie = getCookieFromServer();
            if (cookie.isPresent()) authCookie = cookie.get();
        }
        return LOG.exit(authCookie.toString());
    }
}
