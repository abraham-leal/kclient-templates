package leal.abraham.interactiveQueries;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("kafka-layer")
public class StreamsRESTLayer {

    private KafkaStreams streams;
    private MetadataService metadataService;
    private HostInfo hostInfo;
    private Server jettyServer;

    StreamsRESTLayer(final KafkaStreams streams, final HostInfo host){
        MetadataService metadataService1;
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.hostInfo = host;
    }

    @GET
    @Path("/app/store/key/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getValue(@PathParam("key") final String key){

        final ReadOnlyKeyValueStore<String,String> store = streams.store(KStreamInteractiveQueriesRemoteExample.storeToQuery, QueryableStoreTypes.keyValueStore());

        return store.get(key);

    }

    void start() throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(hostInfo.port());
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

}
