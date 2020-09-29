/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.rest.client.spring;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.Cluster;
import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedExceptions.RemoteException;
import com.softwareplumbers.feed.FeedExceptions.ServerError;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.FeedServiceManager;
import com.softwareplumbers.feed.Filters;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.FeedImpl;
import com.softwareplumbers.feed.impl.MessageFactory;
import com.softwareplumbers.feed.impl.MessageImpl;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URI;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonWriter;
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
    private final MessageFactory factory = new MessageFactory();
    
    
    private static class ServiceInfo {
        public final Instant initTime;
        public final UUID serviceId;
        public ServiceInfo(JsonObject serviceInfo) {
            this.initTime = FeedService.getInitTime(serviceInfo);
            this.serviceId = FeedService.getServerId(serviceInfo);
        }
    }
    
    private ServiceInfo serviceInfo;
    
    private ServiceInfo getServiceInfo() {
        if (serviceInfo == null) {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
            builder.path("feeds");  
            try {
                serviceInfo = new ServiceInfo(getJson(builder.build().toUri()));
            } catch(IOException ioe) {
                throw FeedExceptions.runtime(ioe);
            }
        }
        return serviceInfo;
    }
    
    private <T> CompletableFuture<T> convert(ListenableFuture<T> listenableFuture) {
        
        CompletableFuture<T> completable = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
               boolean result = listenableFuture.cancel(mayInterruptIfRunning);
               super.cancel(mayInterruptIfRunning);
               return result;
            }
        };

        listenableFuture.addCallback(completable::complete, completable::completeExceptionally);
        return completable;
    }
    
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
    
    protected JsonObject getJson(URI uri) throws IOException {
        LOG.entry(uri);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        loginHandler.applyCredentials(headers);

        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);

        return LOG.exit(Json.createReader(new StringReader(response.getBody())).readObject());
    }
    
    /** Get Messages from the server.
     * 
     * @param uri URI from which we will request JSON data
     * @return Parsed JSON object send by server as response.
     */
    protected MessageIterator getMessages(URI uri, Predicate<Message>... filters) {
        LOG.entry(uri);

        RestTemplate restTemplate = new RestTemplate();
        Predicate<Message> filter = Stream.of(filters).reduce(m->true, Predicate::and); 

        return LOG.exit(restTemplate.execute(
            uri, 
            HttpMethod.GET, 
            request -> loginHandler.applyCredentials(request.getHeaders()), 
            response -> extractMessages(response, true).filter(filter)
        ));        
    }
    
    /** Get Messages from the server.
     * 
     * @param uri URI from which we will request messages
     * @return promise of a future message iterator
     */
    protected CompletableFuture<MessageIterator> getMessagesAsync(URI uri, Predicate<Message>... filters) {
        LOG.entry(uri);

        AsyncRestTemplate restTemplate = new AsyncRestTemplate();
        Predicate<Message> filter = Stream.of(filters).reduce(m->true, Predicate::and); 

        ListenableFuture<MessageIterator> result = restTemplate.execute(
                    uri, 
                    HttpMethod.GET, 
                    request -> loginHandler.applyCredentials(request.getHeaders()), 
                    response -> extractMessages(response, true).filter(filter)
            );
        
        return LOG.exit(convert(result));
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
    
    public static String encode(JsonArray array) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); JsonWriter writer = Json.createWriter(bos)) {
            writer.write(array);
            return Base64.getUrlEncoder().encodeToString(bos.toByteArray());
        } catch (IOException ioe) {
            throw FeedExceptions.runtime(ioe);
        }
    }
        
    @Override
    public CompletableFuture<MessageIterator> listen(FeedPath path, Instant from, UUID serverId, long timeoutMillis, Predicate<Message>... filters) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
        builder.path("feeds/{path}");
        builder.queryParam("from", from);
        builder.queryParam("wait", timeoutMillis);
        builder.queryParam("filters", encode(Filters.toJson(filters)));
        return LOG.exit(getMessagesAsync(builder.buildAndExpand(path.toString()).toUri(), Filters.local(filters)));
    }

    @Override
    public MessageIterator search(FeedPath path, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Optional<Boolean> relay, Predicate<Message>... filters) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
        builder.path("feeds/{path}");
        builder.queryParam("from", from);
        if (fromInclusive) builder.queryParam("fromInclusive", true);
        if (to.isPresent()) builder.queryParam("to", to.get());
        if (toInclusive.isPresent()) builder.queryParam("toInclusive", toInclusive.get());
        if (relay.isPresent()) builder.queryParam("relay", relay.get());
        if (filters.length > 0) builder.queryParam("filters", encode(Filters.toJson(filters)));
        return LOG.exit(getMessages(builder.buildAndExpand(path.toString()).toUri(), Filters.local(filters)));
    }

    private Message fromJson(JsonObject object) throws ServerError {
        FeedPath name = Message.getName(object)
            .orElseThrow(()->new ServerError("No name in message returned from server"));
        String id = name.part.getId()
            .orElseThrow(()->new ServerError("No id in message returned from server"));
        return LOG.exit(new MessageImpl(
            Message.getType(object),
            id,
            name,
            Message.getSender(object).orElse(null),
            Message.getTimestamp(object).orElse(Instant.now()),
            Message.getServerId(object),
            Message.getRemoteInfo(object),
            Message.getHeaders(object),
            Message.getLength(object),
            null                 
        ));
    }
    
    @Override
    public Message post(FeedPath path, Message message) throws FeedExceptions.InvalidPath {
        LOG.entry(path, message);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
            builder.path("{path}");
            JsonObject result = sendStream(builder.buildAndExpand(path.toString()).toUri(), HttpMethod.POST, message.toStream());
            return LOG.exit(fromJson(result));
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
    public CompletableFuture<MessageIterator> watch(UUID watcherServerId, Instant after, long timeoutMillis) {
        LOG.entry(watcherServerId, after);
        final String WATCH_FILTER = encode(Filters.toJson(new Predicate[] { Filters.POSTED_LOCALLY }));
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
        builder.path("feeds");
        builder.queryParam("from", after);
        builder.queryParam("wait", timeoutMillis);
        builder.queryParam("filters", WATCH_FILTER);
        return LOG.exit(getMessagesAsync(builder.build().toUri()));
    }

    @Override
    public void setManager(FeedServiceManager manager) {
        // Hmm...
    }

    @Override
    public Instant getInitTime() {
        return getServiceInfo().initTime;
    }

    @Override
    public MessageIterator search(FeedPath messageId, Predicate<Message>... filters) throws FeedExceptions.InvalidPath, FeedExceptions.InvalidId {
        LOG.entry(messageId, filters);
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
        builder.path("feeds/{path}");
        if (filters.length > 0) builder.queryParam("filters", encode(Filters.toJson(filters)));
        return LOG.exit(getMessages(builder.buildAndExpand(messageId.toString()).toUri(), Filters.local(filters)));
    }

    @Override
    public Message replicate(Message message) throws FeedExceptions.InvalidState {
        LOG.entry(message);
        try {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
            builder.path("{path}");
            JsonObject result = sendStream(builder.buildAndExpand(message.getFeedName().toString()).toUri(), HttpMethod.PUT, message.toStream());
            return LOG.exit(fromJson(result));
        } catch (HttpStatusCodeException e) {
            switch (e.getStatusCode()) {
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
    public UUID getServerId() {
        return getServiceInfo().serviceId;
    }

    @Override
    public Optional<Cluster> getCluster() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Feed getFeed(FeedPath path) throws FeedExceptions.InvalidPath {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
        builder.path("feeds");  
        try {
            return LOG.exit(FeedImpl.fromJson(getJson(builder.build().toUri())));
        } catch(IOException ioe) {
            throw FeedExceptions.runtime(ioe);
        }
    }

    @Override
    public Stream<Feed> getChildren(FeedPath path) throws FeedExceptions.InvalidPath {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(feedsUrl);
        builder.path("feeds");  
        try {
            JsonObject feed = getJson(builder.build().toUri());
            return LOG.exit(Feed.getChildren(feed, FeedImpl::fromJson));
        } catch(IOException ioe) {
            throw FeedExceptions.runtime(ioe);
        }
    }

    @Override
    public Stream<Feed> getFeeds() {
        try {
            Feed root = getFeed(FeedPath.ROOT);
            return Stream.of(root).flatMap(feed->feed.getChildren(this));
        } catch (InvalidPath e) {
            throw FeedExceptions.runtime(e);
        }
    }

    @Override
    public Optional<Instant> getLastTimestamp(FeedPath path) throws FeedExceptions.InvalidPath {
        return getFeed(path).getLastTimestamp();
    }

    @Override
    public void dumpState(PrintWriter out) {
        out.println(getServiceInfo());
    }

    @Override
    public void close() throws Exception {
        //hmm
    }
}
