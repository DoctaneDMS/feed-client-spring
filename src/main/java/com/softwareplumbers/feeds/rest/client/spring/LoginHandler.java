package com.softwareplumbers.feeds.rest.client.spring;

import org.springframework.http.HttpHeaders;

/** Generic interface class for implementing different authentication mechanisms.
 *
 * @author Jonathan Essex
 */
public interface LoginHandler {

    /** Apply credentials to a request.
     *
     * Function will perform a login if necessary and apply the resulting credentials to the given request.
     *
     * @param mainRequest The request that requires authentication information.
     */
    void applyCredentials(HttpHeaders mainRequest);

    /** Get credentials
     *
     * @return Credentials cookie as a string.
     */
    String getCredentials();
    
}
