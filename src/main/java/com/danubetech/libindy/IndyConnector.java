package com.danubetech.libindy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

public class IndyConnector {

    private static final Logger log = LoggerFactory.getLogger(IndyConnector.class);

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

    /**
     * This opens the Indy pools and wallets for every configured network.
     * @param createSubmitterDid Whether to create a local DID in the wallet that will be used for submitting queries to the ledger.
     * @param retrieveTaa Whether to retrieve the Transaction Author Agreement from the ledger. This is needed for certain operations such as writing DIDs to the ledger.
     * @param openParallel Whether to open pools and wallets in parallel threads. This speeds up the process if multiple networks are configured, but could also cause higher memory consumption.
     * @throws IndyConnectionException
     */
    public synchronized void openIndyConnections(boolean createSubmitterDid, boolean retrieveTaa, boolean openParallel) throws IndyConnectionException {

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

        if (log.isInfoEnabled()) log.info("poolConfigNames: " + poolConfigNames);
        if (log.isInfoEnabled()) log.info("poolConfigFiles: " + poolConfigFiles);

        // parse pool versions

        String[] poolVersionStrings = this.getPoolVersions() == null ? new String[0] : this.getPoolVersions().split(";");
        Map<String, Integer> poolVersions = new LinkedHashMap<>();
        Map<String, Boolean> nativeDidIndys = new LinkedHashMap<>();
        Map<String, Boolean> nymAddSignMultis = new LinkedHashMap<>();
        Map<String, Boolean> nymEditSignMultis = new LinkedHashMap<>();
        Map<String, Boolean> attribAddSignMultis = new LinkedHashMap<>();
        Map<String, Boolean> attribEditSignMultis = new LinkedHashMap<>();
        for (int i=0; i<poolVersionStrings.length; i+=2) {
            String network = poolVersionStrings[i];
            String poolVersionString = poolVersionStrings[i+1];
            Integer poolVersion = Integer.parseInt(poolVersionString.substring(0, 1));
            Boolean nativeDidIndy = poolVersionString.contains("i");
            Boolean nymAddSignMulti = poolVersionString.contains("N");
            Boolean nymEditSignMulti = poolVersionString.contains("n");
            Boolean attribAddSignMulti = poolVersionString.contains("A");
            Boolean attribEditSignMulti = poolVersionString.contains("a");
            poolVersions.put(network, poolVersion);
            nativeDidIndys.put(network, nativeDidIndy);
            nymAddSignMultis.put(network, nymAddSignMulti);
            nymEditSignMultis.put(network, nymEditSignMulti);
            attribAddSignMultis.put(network, attribAddSignMulti);
            attribEditSignMultis.put(network, attribEditSignMulti);
        }

        if (log.isInfoEnabled()) log.info("poolVersions: " + poolVersions);
        if (log.isInfoEnabled()) log.info("nativeDidIndys: " + nativeDidIndys);
        if (log.isInfoEnabled()) log.info("nymAddSignMultis: " + nymAddSignMultis);
        if (log.isInfoEnabled()) log.info("nymEditSignMultis: " + nymEditSignMultis);
        if (log.isInfoEnabled()) log.info("attribAddSignMultis: " + attribAddSignMultis);
        if (log.isInfoEnabled()) log.info("attribEditSignMultis: " + attribEditSignMultis);

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

        Map<String, IndyConnection> indyConnections = openParallel ? Collections.synchronizedMap(new LinkedHashMap<>()) : new LinkedHashMap<>();
        List<IndyConnectionException> exceptions = openParallel ? Collections.synchronizedList(new ArrayList<>()) : new ArrayList<>();
        Stream<String> networks = openParallel ? poolConfigFiles.keySet().parallelStream() : poolConfigFiles.keySet().stream();

        networks.forEach(network -> {
            String poolConfigName = poolConfigNames.get(network);
            String poolConfigFile = poolConfigFiles.get(network);
            Integer poolVersion = poolVersions.get(network);
            Boolean nativeDidIndy = nativeDidIndys.get(network);
            Boolean nymAddSignMulti = nymAddSignMultis.get(network);
            Boolean nymEditSignMulti = nymEditSignMultis.get(network);
            Boolean attribAddSignMulti = attribAddSignMultis.get(network);
            Boolean attribEditSignMulti = attribEditSignMultis.get(network);
            String walletName = walletNames.get(network);
            String submitterDidSeed = submitterDidSeeds.get(network);
            Long genesisTimestamp = genesisTimestamps.get(network);

            if (poolConfigName == null) exceptions.add(new IndyConnectionException("No 'poolConfigName' for network: " + network));
            if (poolConfigFile == null) exceptions.add(new IndyConnectionException("No 'poolConfigFile' for network: " + network));
            if (poolVersion == null) exceptions.add(new IndyConnectionException("No 'poolVersion' for network: " + network));
            if (nativeDidIndy == null) exceptions.add(new IndyConnectionException("No 'nativeDidIndy' for network: " + network));
            if (nymAddSignMulti == null) exceptions.add(new IndyConnectionException("No 'nymAddSignMulti' for network: " + network));
            if (nymEditSignMulti == null) exceptions.add(new IndyConnectionException("No 'nymEditSignMulti' for network: " + network));
            if (attribAddSignMulti == null) exceptions.add(new IndyConnectionException("No 'attribAddSignMulti' for network: " + network));
            if (attribEditSignMulti == null) exceptions.add(new IndyConnectionException("No 'attribEditSignMulti' for network: " + network));
            if (walletName == null) exceptions.add(new IndyConnectionException("No 'walletName' for network: " + network));
            if (submitterDidSeed == null) exceptions.add(new IndyConnectionException("No 'submitterDidSeed' for network: " + network));
            if (poolVersion == null || nativeDidIndy == null || nymAddSignMulti == null || nymEditSignMulti == null || attribAddSignMulti == null || attribEditSignMulti == null || walletName == null || submitterDidSeed == null) return;
            IndyConnection indyConnection = new IndyConnection(network, poolConfigName, poolConfigFile, poolVersion, nativeDidIndy, nymAddSignMulti, nymEditSignMulti, attribAddSignMulti, attribEditSignMulti, walletName, submitterDidSeed, genesisTimestamp);
            try {
                indyConnection.open(createSubmitterDid, retrieveTaa);
            } catch (IndyConnectionException ex) {
                if (log.isWarnEnabled()) log.warn("Exception while opening Indy connection for network " + network);
                exceptions.add(ex);
            }

            if (log.isInfoEnabled()) log.info("Adding Indy connection for network " + network + ": " + indyConnection);
            indyConnections.put(network, indyConnection);
        });

        if (! exceptions.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder();
            for (IndyConnectionException e: exceptions) {
                errorMessage.append(e.getMessage()).append("; ");
            }
            throw new IndyConnectionException(errorMessage.toString());
        }

        if (log.isInfoEnabled()) log.info("Opened " + indyConnections.size() + " Indy connections: " + indyConnections.keySet());
        this.indyConnections = new LinkedHashMap<>(indyConnections);
    }

    public synchronized void openIndyConnections(boolean createSubmitterDid, boolean retrieveTaa) throws IndyConnectionException {

        this.openIndyConnections(createSubmitterDid, createSubmitterDid, false);
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
