/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.rest.client.spring;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedExceptions.RemoteException;
import com.softwareplumbers.feed.FeedExceptions.ServerError;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.MessageFactory;
import com.softwareplumbers.feed.impl.MessageImpl;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.client.AsyncRestTemplate;
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
    private Map<Consumer<MessageIterator>, ListenableFuture<MessageIterator>> callbacks = new ConcurrentHashMap<>();
    
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
    
    /** Send a JSON object to the server.
     * 
     * @param uri URI to which we will send the data
     * @param method HTTP method used to send the data (POST or PUT)
     * @param stream Stream to send
     * @return Parsed JSON object send by server as response.
     * @throws IOException 
     */
    protected JsonObject sendStream(URI uri, HttpMethod method, InputStream stream) throws IOException {
        LOG.entry(uri, method, stream);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", MediaType.APPLICATION_OCTET_STREAM_VALUE);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);
        InputStreamResource resource = new InputStreamResource(stream);
        HttpEntity<InputStreamResource> requestEntity = new HttpEntity<>(resource, headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.exchange(
                uri, method, requestEntity,
                String.class);

        return LOG.exit(Json.createReader(new StringReader(response.getBody())).readObject());
    }
    
    /** Get Messages from the server.
     * 
     * @param uri URI from which we will request JSON data
     * @return Parsed JSON object send by server as response.
     */
    protected MessageIterator getMessages(URI uri) {
        LOG.entry(uri);

        RestTemplate restTemplate = new RestTemplate();

        return LOG.exit(restTemplate.execute(
            uri, 
            HttpMethod.GET, 
            request -> loginHandler.applyCredentials(request.getHeaders()), 
            response -> extractMessages(response, true)
        ));        
    }
    
    /** Get Messages from the server.
     * 
     * @param uri URI from which we will request messages
     * @param callback Callback function to receive messages
     */
    protected void getMessages(URI uri, Consumer<MessageIterator> callback) {
        LOG.entry(uri, callback);

        AsyncRestTemplate restTemplate = new AsyncRestTemplate();

        ListenableFuture<MessageIterator> result = restTemplate.execute(
                    uri, 
                    HttpMethod.GET, 
                    request -> loginHandler.applyCredentials(request.getHeaders()), 
                    response -> extractMessages(response, true)
            );
        
        callbacks.put(callback, result);
        result.addCallback(messages->receiveMessagesOn(callback, messages), error->handleErrorOn(error,callback));
        LOG.exit();
    }
    
    public MessageIterator extractMessages(ClientHttpResponse response, boolean copyBuffer) {
        LOG.entry(response, copyBuffer);
        try {
            if (response.getStatusCode() != HttpStatus.OK) {
                FeedExceptions.BaseRuntimeException error = getDefaultError(response.getBody())
                    .orElseGet(()->new RemoteException(rawError(response)));
                return LOG.exit(MessageIterator.defer(error));
            }
            if (copyBuffer) {
                // I hate it that we have to do this. 
                InputStreamSupplier iss = InputStreamSupplier.copy(response::getBody);
                return LOG.exit(factory.buildIterator(iss.get(), Optional.empty()));
            } else {
                return LOG.exit(factory.buildIterator(response.getBody(), Optional.empty()));
            }
        } catch (IOException error) {
            return LOG.exit(MessageIterator.defer(FeedExceptions.runtime(error)));
        }
    }
    
    public void receiveMessagesOn(Consumer<MessageIterator> callback, MessageIterator messages) {
        LOG.entry(callback, messages);
        callbacks.remove(callback);
        callback.accept(messages);
        LOG.exit();
    }
    
    public void handleErrorOn(Throwable error, Consumer<MessageIterator> callback) {
        callbacks.remove(callback);
        if (error instanceof FeedExceptions.BaseRuntimeException) callback.accept(MessageIterator.defer((FeedExceptions.BaseRuntimeException)error));
        LOG.error("Unhandled exception", error);
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
    public void listen(FeedPath path, Instant from, Consumer<MessageIterator> callback) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from);
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
            builder.path("{path}");
            builder.queryParam("from", from);
            builder.queryParam("wait", "20");
            getMessages(builder.buildAndExpand(path.toString()).toUri(), callback);
        LOG.exit();
    }

    @Override
    public void cancelCallback(FeedPath fp, Consumer<MessageIterator> callback) throws FeedExceptions.InvalidPath {
        ListenableFuture<MessageIterator> future = callbacks.remove(callback);
        future.cancel(true);
    }

    @Override
    public MessageIterator sync(FeedPath path, Instant from) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
        builder.path("{path}");
        builder.queryParam("from", from);
        return LOG.exit(getMessages(builder.buildAndExpand(path.toString()).toUri()));
    }

    @Override
    public Message post(FeedPath path, Message message) throws FeedExceptions.InvalidPath {
        LOG.entry(path, message);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
            builder.path("{path}");
            JsonObject result = LOG.exit(sendStream(builder.buildAndExpand(path.toString()).toUri(), HttpMethod.POST, message.toStream()));
            FeedPath name = Message.getName(result)
                .orElseThrow(()->new ServerError("No name in message returned from server"));
            String id = name.part.getId()
                .orElseThrow(()->new ServerError("No id in message returned from server"));
            return LOG.exit(new MessageImpl(
                id,
                name,
                Message.getSender(result).orElse(null),
                Message.getTimestamp(result).orElse(Instant.now()),
                Message.getHeaders(result),
                Message.getLength(result),
                ()->message.getData()                 
            ));
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
                case NOT_FOUND:
                    throw LOG.throwing(new FeedExceptions.InvalidPath(path));
                default:
                    throw getDefaultError(e);
            }
        }  catch (IOException e) {
            throw FeedExceptions.runtime(e);
        } catch (ServerError e) {
            throw FeedExceptions.runtime(e);
        }
    }

    @Override
    public void dumpState() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
