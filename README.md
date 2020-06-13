#Simple Spring Client for Doctane Feeds

The REST interface is considered the 'primary' interface for developing Doctane clients. However,
this java implementation of the core Doctane feed service uses the REST interface to communicate
with a remote back-end. This means that unit tests built against the core Doctane interface can
be used to directly test the service module, and also (via this java client) test the REST server.