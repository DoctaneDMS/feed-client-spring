/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feeds.rest.client.spring;

import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedExceptions.RemoteException;
import com.softwareplumbers.feed.FeedExceptions.InvalidJson;
import com.softwareplumbers.feed.FeedExceptions.ServerError;
import com.softwareplumbers.feed.FeedExceptions.StreamingException;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.MessageFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import javax.json.Json;
import javax.json.JsonObject;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/** Implements the Doctane RepositoryService interface on top of a Spring REST client.
 *
 * @author SWPNET\jonessex
 */
public class FeedServiceImpl implements FeedService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(FeedServiceImpl.class);
 
    private String feedsUrl;

    private LoginHandler loginHandler;
    
    /** Set the URL for the Doctane web service to be called.
     * 
     * @param feedsUrl URL for Doctane feed operations
     */
    public void setFeedsAPIURL(String feedsUrl) { 
        this.feedsUrl = feedsUrl;
    }
    
    /** Set the class that will handle authentication with the Doctane web service.
     * 
     * @param loginHandler Login handler which handle authentication process
     */
    public void setLoginHandler(LoginHandler loginHandler) {
        this.loginHandler = loginHandler;
    }
    
    /** Construct a service using URL and login handler.
     * 
     * @param docsUrl
     * @param loginHandler 
     */
    public FeedServiceImpl(String feedsUrl, LoginHandler loginHandler) {
        this.feedsUrl = feedsUrl;
        this.loginHandler = loginHandler;
    }
    
    /** Construct an uninitialized service.
     * 
     * The FeedsAPIURL and LoginHandler properties must be set before using the service.
     */
    public FeedServiceImpl() {
        this(null, null);
    }
    
    private final MessageFactory factory = new MessageFactory();
    
    /** Convert a stream and mime type into an HttpEntity we can send to the server.
     * 
     * @param is Input stream to send
     * @param mimeType Type of data encoded in the stream
     * @return An HttpEntity that can be sent to the server.
     */
    private static HttpEntity<InputStreamResource> toEntity(InputStream is, String mimeType) {
	    InputStreamResource resource = new InputStreamResource(is);
		HttpHeaders fileHeader = new HttpHeaders();
		fileHeader.set("Content-Type", mimeType);
		return new HttpEntity<>(resource, fileHeader);
	}
	
        
    
    /** Send a JSON object to the server.
     * 
     * @param uri URI to which we will send the data
     * @param method HTTP method used to send the data (POST or PUT)
     * @param jo JSON object to send
     * @return Parsed JSON object send by server as response.
     * @throws IOException 
     */
    protected JsonObject sendJson(URI uri, HttpMethod method, JsonObject jo) throws IOException {
        LOG.entry(uri, method, jo);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", org.springframework.http.MediaType.APPLICATION_JSON_VALUE);
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);
        HttpEntity<String> requestEntity = new HttpEntity<>(jo.toString(), headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, method, requestEntity,
                String.class);

        return LOG.exit(Json.createReader(new StringReader(response.getBody())).readObject());
    }
    
    /** Get JSON from the server.
     * 
     * @param uri URI from which we will request JSON data
     * @return Parsed JSON object send by server as response.
     */
    protected MessageIterator getMessages(URI uri) throws InvalidJson, StreamingException {
        LOG.entry(uri);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<InputStream> response = restTemplate.exchange(
                uri, HttpMethod.GET, new HttpEntity<>(headers), InputStream.class);
        
        return LOG.exit(factory.buildIterator(response.getBody(), Optional.empty()));
    }
    
    /** Send a DELETE operation to the server.
     * 
     * @param uri URI on which we will invoke DELETE.
     */
    protected void delete(URI uri) {
        LOG.entry(uri);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(org.springframework.http.MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, HttpMethod.DELETE, new HttpEntity<>(headers), String.class);

        LOG.exit();
    }
    
    /** Convert a raw ClientHttpResponse into a ServerError exception 
     * 
     * @param response Response to convert
     * @return A ServerError including any reason phrase from the response.
     */
    protected static ServerError rawError(ClientHttpResponse response) {
        try {
            return new ServerError(response.getStatusCode().getReasonPhrase());
        } catch (IOException ioe) {
            // Oh, FFS
            return new ServerError("unknown server error");
        }
    }
   
    private static void writeBytes(ClientHttpResponse response, OutputStream out) throws IOException {
        
        if (response.getStatusCode() != HttpStatus.OK) {
            throw getDefaultError(response.getBody()).orElseGet(()->new RemoteException(rawError(response)));
        }
        OutputStreamConsumer.of(()->response.getBody()).consume(out);
    } 
    
     
    /** Parse a remote exception from a text stream.
     * 
     * @param body Text stream with JSON encoded remote error
     * @return An error, if successfully parsed.
     */
    protected static Optional<RemoteException> getDefaultError(InputStream body) {
        JsonObject message = null;
        try {
            message = Json.createReader(body).readObject();
        } catch (RuntimeException je) {
            // suppress
        }
        if (message != null)
            return Optional.of(new RemoteException(FeedExceptions.build(message)));
        else
            return Optional.empty();
    }
    
    /** Default error handler.
     * 
     * Where we have a server error message that can be parsed into a locally understood
     * exception type, do this, and then wrap it in a RemoteException.
     * 
     * @see Exceptions#buildException(javax.json.JsonObject) 
     * 
     * @param e an HttpStatusCodeException originating from a REST call to a Doctane server.
     * @return An appropriate RemoteException.
     */
    protected static RemoteException getDefaultError(HttpStatusCodeException e) {
        LOG.entry(e);
        InputStream body = new ByteArrayInputStream(e.getResponseBodyAsByteArray());
        Optional<RemoteException> re = getDefaultError(body);
        return re.orElseGet(() -> {
            LOG.warn("Unexplained error {} : {}", e.getRawStatusCode(), e.getResponseBodyAsString());
            return new RemoteException(new ServerError(e.getStatusText()));
        });
    }
    

  
    
    @Override
    public void listen(FeedPath fp, Instant instnt, Consumer<MessageIterator> cnsmr) throws FeedExceptions.InvalidPath {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void cancelCallback(FeedPath fp, Consumer<MessageIterator> cnsmr) throws FeedExceptions.InvalidPath {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public MessageIterator sync(FeedPath path, Instant from) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
            builder.path("{path}");
            builder.queryParam("from", from);
            return LOG.exit(getMessages(builder.build().toUri()));
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND:
                    throw new FeedExceptions.InvalidPath(path);
                default:
                    throw getDefaultError(e);
            }
        } catch (InvalidJson | StreamingException e) {
            throw FeedExceptions.runtime(e);
        }   
    }

    @Override
    public Message post(FeedPath path, Message msg) throws FeedExceptions.InvalidPath {
        return null;
    }

    @Override
    public void dumpState() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
