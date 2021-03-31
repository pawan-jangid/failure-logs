package com.ofss.digx.infra.cache.jcache;

import com.ofss.fc.infra.config.ConfigurationFactory;
import com.ofss.fc.infra.log.impl.MultiEntityLogger;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.Cache;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.sql.DataSource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.prefs.Preferences;

public class ApacheIgniteCacheClient implements IJCacheProvider{

    private static final int DEF_SLOW_CLIENT_QUEUE_LIMIT = 1000;
    private static final long DEF_IDLE_CON_TIMEOUT = 10 * 60_000;
    private static final long DEF_CACHE_EXP_TIMEOUT = 30;
    private static final String CACHE_EXP_TIMEOUT = "CACHE_EXP_TIMEOUT";
    private static final String SLOW_CLIENT_QUEUE_LIMIT = "SLOW_CLIENT_QUEUE_LIMIT";
    private static final String IDLE_CON_TIMEOUT = "IDLE_CON_TIMEOUT";
    private static final String JOIN_TIMEOUT = "JOIN_TIMEOUT";
    private static final long DEF_JOIN_TIMEOUT = 5000;
    /**
     * Ignite instance
     **/
    private Ignite ignite;

    /**
     * The database URL.
     **/
    protected DataSource dataSource;

    private CacheConfiguration cacheConfig;

    /**
     *
     * Stores the name of the entity(class) represented by this {@code Class} object as a {@code String}
     */
    private static final String THIS_COMPONENT_NAME = ApacheIgniteCacheProvider.class.getName();

    /**
     * Static instance of com.ofss.fc.infra.log.impl.MultiEntityLogger.
     */
    private static final Logger LOGGER = MultiEntityLogger.getUniqueInstance()
            .getLogger(THIS_COMPONENT_NAME);

    private static final String EXITED_FROM = "Exiting from " + THIS_COMPONENT_NAME;
    private static final String ENTERED_INTO = "Entered into " + THIS_COMPONENT_NAME;
    private static final MultiEntityLogger FORMATTER = MultiEntityLogger.getUniqueInstance();

    private static final String REMOTE_CACHE_ADD = "REMOTE_CACHE_ADD";
    private static final String DEF_REMOTE_CACHE_ADD = "172.17.0.6:47500..47509";
    private static final long DEF_BLCKED_TIMEOUT = 60 * 60 * 100;

    public ApacheIgniteCacheClient(){
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.log(Level.FINE,
                    FORMATTER.formatMessage(ENTERED_INTO + " ApacheIgniteCacheClient() no args constructor "));
        }
        Preferences dayOneConfig = ConfigurationFactory.getInstance()
                .getConfigurations("DayOneConfig");
        String address = dayOneConfig.get(REMOTE_CACHE_ADD, DEF_REMOTE_CACHE_ADD);
        String[] addresses = null;
        if(address.contains(",")){
            addresses = address.split(",");
        }else{
            addresses = new String[1];
            addresses[0] = address;
        }
        // Preparing IgniteConfiguration using Java APIs
        IgniteConfiguration cfg = new IgniteConfiguration();

        // The node will be started as a client node.
        cfg.setClientMode(true);
        cfg.setSystemWorkerBlockedTimeout(DEF_BLCKED_TIMEOUT);

        cacheConfig = new CacheConfiguration();
        cacheConfig.setCopyOnRead(false);
        cacheConfig.setBackups(0);
        cacheConfig.setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES,dayOneConfig.getLong(CACHE_EXP_TIMEOUT,DEF_CACHE_EXP_TIMEOUT))));

        TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
        tcpCommunicationSpi.setSlowClientQueueLimit(dayOneConfig.getInt(SLOW_CLIENT_QUEUE_LIMIT,DEF_SLOW_CLIENT_QUEUE_LIMIT));
        tcpCommunicationSpi.setIdleConnectionTimeout(dayOneConfig.getLong(IDLE_CON_TIMEOUT,DEF_IDLE_CON_TIMEOUT));

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        // Set initial IP addresses.
        // Note that you can optionally specify a port or a port range.
        ipFinder.setAddresses(Arrays.asList(addresses));
        spi.setIpFinder(ipFinder);
        spi.setClientReconnectDisabled(true);
        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);
        cfg.setCommunicationSpi(tcpCommunicationSpi);
        cfg.setFailureHandler(new NoOpFailureHandler());
        // Starting the node
        ignite = Ignition.start(cfg);

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.log(Level.FINE,
                    FORMATTER.formatMessage(EXITED_FROM + "ApacheIgniteCacheClient() no args constructor "));
        }
    }

    private static class SingletonHolder {
        private static final ApacheIgniteCacheClient INSTANCE = new ApacheIgniteCacheClient();
    }

    /**
     * Instance provider of {@link ApacheIgniteCacheClient}
     *
     * @return singleton instance of {@link ApacheIgniteCacheClient}
     */
    public static IJCacheProvider getInstance() {
        return ApacheIgniteCacheClient.SingletonHolder.INSTANCE;
    }

    /**
     * Method provides cache instance of given name from caches store.If instance with given name not exists returns
     * null .
     */
    @Override
    public Cache getCache(String name) {
        LOGGER.log(Level.SEVERE, "Inside getCache(name) method where name is : %s", name);
        cacheConfig.setName(name);
        return ignite.getOrCreateCache(cacheConfig);
    }


    @Override
    public void destroyCache(String name) {
        ignite.destroyCache(name);
    }
}
