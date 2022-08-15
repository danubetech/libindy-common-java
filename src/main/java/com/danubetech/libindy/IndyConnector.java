package com.danubetech.libindy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class IndyConnector {

    private static Logger log = LoggerFactory.getLogger(IndyConnector.class);

    private String poolConfigs;
    private String poolVersions;
    private String walletNames;
    private String submitterDidSeeds;
    private String genesisTimestamps;

    private Map<String, IndyConnection> indyConnections;

    public IndyConnector(String poolConfigs, String poolVersions, String walletNames, String submitterDidSeeds, String genesisTimestamps) {
        this.poolConfigs = poolConfigs;
        this.poolVersions = poolVersions;
        this.walletNames = walletNames;
        this.submitterDidSeeds = submitterDidSeeds;
        this.genesisTimestamps = genesisTimestamps;
    }

    public IndyConnector() {
        this.indyConnections = null;
    }

    public boolean isOpened() {
        return this.getIndyConnections() != null;
    }

    public synchronized void closeIndyConnections() throws IndyConnectionException {

        if (this.getIndyConnections() == null) {
            if (log.isWarnEnabled()) log.warn("Indy connections have not been opened and therefore cannot be closed.");
            return;
        }

        for (IndyConnection indyConnection : this.getIndyConnections().values()) {
            indyConnection.close();
        }
        this.getIndyConnections().clear();
        this.setIndyConnections(null);
        System.gc();
    }

    public synchronized void openIndyConnections(boolean createSubmitterDid, boolean retrieveTaa) throws IndyConnectionException {

        if (this.getPoolConfigs() == null || this.getPoolConfigs().isEmpty()) throw new IllegalStateException("No configuration found for Indy connections.");

        if (this.getIndyConnections() != null) {
            if (log.isWarnEnabled()) log.warn("Indy connections have already been opened.");
            return;
        }

        // parse pool configs

        String[] poolConfigStrings = this.getPoolConfigs() == null ? new String[0] : this.getPoolConfigs().split(";");
        Map<String, String> poolConfigNames = new LinkedHashMap<>();
        Map<String, String> poolConfigFiles = new LinkedHashMap<>();
        for (int i=0; i<poolConfigStrings.length; i+=2) {
            String network = poolConfigStrings[i];
            String poolConfigName = network;
            String poolConfigFile = poolConfigStrings[i+1];
            poolConfigNames.put(network, poolConfigName);
            poolConfigFiles.put(network, poolConfigFile);
        }

        if (log.isInfoEnabled()) log.info("Pool configs: " + poolConfigFiles);

        // parse pool versions

        String[] poolVersionStrings = this.getPoolVersions() == null ? new String[0] : this.getPoolVersions().split(";");
        Map<String, Integer> poolVersions = new LinkedHashMap<>();
        Map<String, Boolean> nativeDidIndys = new LinkedHashMap<>();
        for (int i=0; i<poolVersionStrings.length; i+=2) {
            String network = poolVersionStrings[i];
            Integer poolVersion = poolVersionStrings[i+1].endsWith("i") ? Integer.parseInt(poolVersionStrings[i+1].substring(0, poolVersionStrings[i+1].length()-1)) : Integer.parseInt(poolVersionStrings[i+1]);
            Boolean nativeDidIndy = poolVersionStrings[i+1].endsWith("i");
            poolVersions.put(network, poolVersion);
            nativeDidIndys.put(network, nativeDidIndy);
        }

        if (log.isInfoEnabled()) log.info("Pool versions: " + poolVersions);

        // parse wallet names

        String[] walletNameStrings = this.getWalletNames() == null ? new String[0] : this.getWalletNames().split(";");
        Map<String, String> walletNames = new LinkedHashMap<>();
        for (int i=0; i<walletNameStrings.length; i+=2) {
            String network = walletNameStrings[i];
            String walletName = walletNameStrings[i+1];
            walletNames.put(network, walletName);
        }

        if (log.isInfoEnabled()) log.info("Wallet names: " + walletNames);

        // parse submitter DID seeds

        String[] submitterDidSeedStrings = this.getSubmitterDidSeeds() == null ? new String[0] : this.getSubmitterDidSeeds().split(";");
        Map<String, String> submitterDidSeeds = new LinkedHashMap<>();
        for (int i=0; i<submitterDidSeedStrings.length; i+=2) {
            String network = submitterDidSeedStrings[i];
            String submitterDidSeed = submitterDidSeedStrings[i+1];
            submitterDidSeeds.put(network, submitterDidSeed);
        }

        if (log.isInfoEnabled()) log.info("Submitter DID seeds: " + submitterDidSeeds);

        // parse genesis timestamps

        String[] genesisTimestampStrings = this.getGenesisTimestamps() == null ? new String[0] : this.getGenesisTimestamps().split(";");
        Map<String, Long> genesisTimestamps = new LinkedHashMap<>();
        for (int i=0; i<genesisTimestampStrings.length; i+=2) {
            String network = genesisTimestampStrings[i];
            Long genesisTimestamp = Long.parseLong(genesisTimestampStrings[i+1]);
            genesisTimestamps.put(network, genesisTimestamp);
        }

        if (log.isInfoEnabled()) log.info("Genesis timestamps: " + genesisTimestamps);

        // create indy connections

        Map<String, IndyConnection> indyConnections = new LinkedHashMap<>();

        for (String network : poolConfigFiles.keySet()) {

            String poolConfigName = poolConfigNames.get(network);
            String poolConfigFile = poolConfigFiles.get(network);
            Integer poolVersion = poolVersions.get(network);
            Boolean nativeDidIndy = nativeDidIndys.get(network);
            String walletName = walletNames.get(network);
            String submitterDidSeed = submitterDidSeeds.get(network);
            Long genesisTimestamp = genesisTimestamps.get(network);

            if (poolConfigName == null) throw new IndyConnectionException("No 'poolConfigName' for network: " + network);
            if (poolConfigFile == null) throw new IndyConnectionException("No 'poolConfigFile' for network: " + network);
            if (poolVersion == null) throw new IndyConnectionException("No 'poolVersion' for network: " + network);
            if (nativeDidIndy == null) throw new IndyConnectionException("No 'nativeDidIndy' for network: " + network);
            if (walletName == null) throw new IndyConnectionException("No 'walletName' for network: " + network);
            if (submitterDidSeed == null) throw new IndyConnectionException("No 'submitterDidSeed' for network: " + network);

            IndyConnection indyConnection = new IndyConnection(network, poolConfigName, poolConfigFile, poolVersion, nativeDidIndy, walletName, submitterDidSeed, genesisTimestamp);
            indyConnection.open(createSubmitterDid, retrieveTaa);

            indyConnections.put(network, indyConnection);
        }

        if (log.isInfoEnabled()) log.info("Opened " + indyConnections.size() + " Indy connections: " + indyConnections.keySet());
        this.indyConnections = indyConnections;
    }

    public synchronized IndyConnection getIndyConnection(String network, boolean autoReopen, boolean createSubmitterDid, boolean retrieveTaa) throws IndyConnectionException {

        IndyConnection indyConnection = this.getIndyConnections().get(network);
        if (indyConnection == null) return null;

        if (autoReopen && (! indyConnection.isOpen())) {
            if (log.isInfoEnabled()) log.info("Auto re-opening Indy connection for network " + network + ": " + indyConnection);
            indyConnection.close();
            indyConnection.open(createSubmitterDid, retrieveTaa);
        }

        return indyConnection;
    }

    /*
     * Getters and setters
     */

    public String getPoolConfigs() {
        return poolConfigs;
    }

    public void setPoolConfigs(String poolConfigs) {
        this.poolConfigs = poolConfigs;
    }

    public String getPoolVersions() {
        return poolVersions;
    }

    public void setPoolVersions(String poolVersions) {
        this.poolVersions = poolVersions;
    }

    public String getWalletNames() {
        return walletNames;
    }

    public void setWalletNames(String walletNames) {
        this.walletNames = walletNames;
    }

    public String getSubmitterDidSeeds() {
        return submitterDidSeeds;
    }

    public void setSubmitterDidSeeds(String submitterDidSeeds) {
        this.submitterDidSeeds = submitterDidSeeds;
    }

    public String getGenesisTimestamps() {
        return genesisTimestamps;
    }

    public void setGenesisTimestamps(String genesisTimestamps) {
        this.genesisTimestamps = genesisTimestamps;
    }

    public Map<String, IndyConnection> getIndyConnections() {
        return indyConnections;
    }

    public void setIndyConnections(Map<String, IndyConnection> indyConnections) {
        this.indyConnections = indyConnections;
    }
}
