/*
 * Copyright (C) 2008-2009 Institute for Computational Biomedicine,
 *                         Weill Medical College of Cornell University
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.cornell.med.icb.R;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Class used to start and stop instances of the R server from within the JVM.
 * @see <a href="http://www.rforge.net/Rserve/">Rserve - Binary R server</a>
 */
public final class RUtils {
    /**
     * Used to log debug and informational messages.
     */
    private static final Log LOG = LogFactory.getLog(RUtils.class);

    /**
     * Default executable name for reserve platforms.
     */
    private static final String DEFAULT_RSERVE_EXECUTABLE = "Rserve";

    /**
     * Default executable name for reserve on windows platforms.
     */
    private static final String DEFAULT_RSERVE_EXECUTABLE_WINDOWS = "Rserve.exe";

    /**
     * Default executable name for reserve.  The default assumes that the command is already
     * on the path.
     */
    static final String DEFAULT_RSERVE_COMMAND =
            System.getProperty("RSERVE_COMMAND", SystemUtils.IS_OS_WINDOWS
                    ? DEFAULT_RSERVE_EXECUTABLE_WINDOWS : DEFAULT_RSERVE_EXECUTABLE);

    /**
     * Name of Rserve executable.  This should be a fully qualified path unless it
     * is located on the execution path.
     */
    private String rServeCommand =
            "C:\\Program Files\\R\\R-2.6.0\\library\\Rserve\\" + DEFAULT_RSERVE_COMMAND;

    /**
     * This class is for internal use by the {@link edu.cornell.med.icb.R.RConnectionPool}
     * and also from the command line via the {@link #main(String[])} method.  Normally,
     * other classes should not be using this directly.
     */
    private RUtils() {
        super();
    }

    /**
     * Can be used to start a rserve instance.
     * @param threadPool The ExecutorService used to start the Rserve process
     * @param rServeCommand Full path to command used to start Rserve process
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     * @param username Username to send to the server if authentication is required
     * @param password Password to send to the server if authentication is required
     * @return The return value from the Rserve instance
     */
    static Future<Integer> startup(final ExecutorService threadPool, final String rServeCommand,
                                   final String host, final int port,
                                   final String username, final String password) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Attempting to start Rserve on " + host + ":" + port);
        }

        return threadPool.submit(new Callable<Integer>() {
            public Integer call() throws IOException {
                final List<String> commands = new ArrayList<String>();

                // if the host is not local, use ssh to exec the command
                if (!"localhost".equals(host) && !"127.0.0.1".equals(host)
                        && !InetAddress.getLocalHost().equals(InetAddress.getByName(host))) {
                    commands.add("ssh");
                    commands.add(host);
                }

                // TODO - this will fail when spaces are in the the path to the executable
                CollectionUtils.addAll(commands, rServeCommand.split(" "));
                commands.add("--RS-port");
                commands.add(Integer.toString(port));

                final String[] command = commands.toArray(new String[commands.size()]);
                LOG.debug(ArrayUtils.toString(commands));

                final ProcessBuilder builder = new ProcessBuilder(command);
                builder.redirectErrorStream(true);
                final Process process = builder.start();
                BufferedReader br = null;
                try {
                    final InputStream is = process.getInputStream();
                    final InputStreamReader isr = new InputStreamReader(is);
                    br = new BufferedReader(isr);
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(host + ":" + port + "> " + line);
                        }
                    }

                    process.waitFor();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Rserve on " + host + ":" + port + " terminated");
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted!", e);
                    process.destroy();
                    Thread.currentThread().interrupt();
                } finally {
                    IOUtils.closeQuietly(br);
                }

                final int exitValue = process.exitValue();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Rserve on " + host + ":" + port + " returned " + exitValue);
                }
                return exitValue;
            }
        });
    }

    /**
     * Can be used to shut down a running rserve instance.
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     * @param username Username to send to the server if authentication is required
     * @param password Password to send to the server if authentication is required
     * @throws RserveException if the process cannot be shutdown properly
     */
    static void shutdown(final String host, final int port,
                         final String username, final String password) throws RserveException {
        if (LOG.isInfoEnabled()) {
            LOG.info("Attempting to shutdown Rserve on " + host + ":" + port);
        }

        final RConnection rConnection = new RConnection(host, port);
        if (rConnection.needLogin()) {
            rConnection.login(username, password);
        }
        rConnection.shutdown();

        if (LOG.isInfoEnabled()) {
            LOG.info("Shutdown message sent");
        }
    }

    /**
     * Can be used to determine if an rserve instance is running or not.
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     * @param username Username to send to the server if authentication is required
     * @param password Password to send to the server if authentication is required
     * @return true if the connection to the Rserve instance was sucessful, false otherwise
     */
    static boolean validate(final String host, final int port,
                            final String username, final String password) {
        boolean connected;

        if (LOG.isInfoEnabled()) {
            LOG.info("Attempting to connect to Rserve on " + host + ":" + port);
        }

        try {
            final RConnection rConnection = new RConnection(host, port);
            if (rConnection.needLogin()) {
                rConnection.login(username, password);
            }
            connected = rConnection.isConnected();
            LOG.debug("Server version is: " + rConnection.getServerVersion());
            rConnection.close();
        } catch (RserveException e) {
            connected = false;
            LOG.error("", e);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Was" + (connected ? " " : " NOT ") + "able to connect to Rserve on "
                    + host + ":" + port);
        }

        return connected;
    }

    /**
     * Print usage message for main method.
     *
     * @param options Options used to determine usage
     */
    private static void usage(final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(RUtils.class.getName(), options, true);
    }

    /**
     * Current "mode" of operation for the main method.
     */
    private enum Mode {
        /** Mode used to start Rserve instances. */
        startup,
        /** Mode used to shutdown Rserve instances. */
        shutdown,
        /** Mode used to check whether or not Rserve instances are running. */
        validate
    }

    public static void main(final String[] args)
            throws ParseException, RserveException, ConfigurationException {
        final Options options = new Options();

        final Option helpOption = new Option("h", "help", false, "Print this message");
        options.addOption(helpOption);

        final Option startupOption = new Option(Mode.startup.name(), Mode.startup.name(), false,
                "Start Rserve process");
        final Option shutdownOption = new Option(Mode.shutdown.name(), Mode.shutdown.name(), false,
                "Shutdown Rserve process");
        final Option validateOption = new Option(Mode.validate.name(), Mode.validate.name(), false,
                "Validate that Rserve processes are running");

        final OptionGroup optionGroup = new OptionGroup();
        optionGroup.addOption(startupOption);
        optionGroup.addOption(shutdownOption);
        optionGroup.addOption(validateOption);
        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        final Option portOption = new Option("port", "port", true,
                "Use specified port to communicate with the Rserve process");
        portOption.setArgName("port");
        portOption.setType(int.class);
        options.addOption(portOption);

        final Option hostOption = new Option("host", "host", true,
                "Communicate with the Rserve process on the given host");
        hostOption.setArgName("hostname");
        hostOption.setType(String.class);
        options.addOption(hostOption);

        final Option userOption =
                new Option("u", "username", true, "Username to send to the Rserve process");
        userOption.setArgName("username");
        userOption.setType(String.class);
        options.addOption(userOption);

        final Option passwordOption =
                new Option("p", "password", true, "Password to send to the Rserve process");
        passwordOption.setArgName("password");
        passwordOption.setType(String.class);
        options.addOption(passwordOption);

        final Option configurationOption =
                new Option("c", "configuration", true, "Configuration file or url to read from");
        configurationOption.setArgName("configuration");
        configurationOption.setType(String.class);
        options.addOption(configurationOption);

        final Parser parser = new BasicParser();
        final CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            usage(options);
            throw e;
        }

        int exitStatus = 0;
        if (commandLine.hasOption("h")) {
            usage(options);
        } else {
            Mode mode = null;
            for (final Mode potentialMode : Mode.values()) {
                if (commandLine.hasOption(potentialMode.name())) {
                    mode = potentialMode;
                    break;
                }
            }

            final ExecutorService threadPool = Executors.newCachedThreadPool();

            if (commandLine.hasOption("configuration")) {
                final String configurationFile = commandLine.getOptionValue("configuration");
                LOG.info("Reading configuration from " + configurationFile);
                XMLConfiguration configuration;
                try {
                    final URL configurationURL = new URL(configurationFile);
                    configuration = new XMLConfiguration(configurationURL);
                } catch (MalformedURLException e) {
                    // resource is not a URL: attempt to get the resource from a file
                    LOG.debug("Configuration is not a valid url");
                    configuration = new XMLConfiguration(configurationFile);
                }

                configuration.setValidating(true);
                final int numberOfRServers =
                        configuration.getMaxIndex("RConfiguration.RServer") + 1;
                boolean failed = false;
                for (int i = 0; i < numberOfRServers; i++) {
                    final String server = "RConfiguration.RServer(" + i + ")";
                    final String host = configuration.getString(server + "[@host]");
                    final int port = configuration.getInt(server + "[@port]",
                            RConfigurationUtils.DEFAULT_RSERVE_PORT);
                    final String username = configuration.getString(server + "[@username]");
                    final String password = configuration.getString(server + "[@password]");
                    final String command = configuration.getString(server + "[@command]",
                            DEFAULT_RSERVE_COMMAND);

                    if (executeMode(mode, threadPool, host, port, username, password, command) != 0) {
                        failed = true;  // we have other hosts to check so keep a failed state
                    }
                }
                if (failed) {
                    exitStatus = 3;
                }
            } else {
                final String host = commandLine.getOptionValue("host", "localhost");
                final int port = Integer.valueOf(commandLine.getOptionValue("port", "6311"));
                final String username = commandLine.getOptionValue("username");
                final String password = commandLine.getOptionValue("password");

                exitStatus = executeMode(mode, threadPool, host, port, username, password, null);
            }
            threadPool.shutdown();
        }

        System.exit(exitStatus);
    }

    /**
     * @param mode Mode to execute
     * @param threadPool The ExecutorService used to start the Rserve process
     * @param command Full path to command used to start Rserve process
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     * @param username Username to send to the server if authentication is required
     * @param password Password to send to the server if authentication is required
     * @return 0 if the mode was executed successfully
     */
    private static int executeMode(final Mode mode, final ExecutorService threadPool,
                                   final String host, final int port,
                                   final String username, final String password,
                                   final String command) {
        int status = 0;

        switch (mode) {
            case shutdown:
                try {
                    shutdown(host, port, username, password);
                } catch (RserveException e) {
                    // just let the user know and try the other servers
                    LOG.warn("Couldn't shutdown Rserve on " + host + ":" + port, e);
                    status = 1;
                }
                break;
            case startup:
                final Future<Integer> future =
                        startup(threadPool, command, host, port, username, password);
                try {
                    status = future.get();
                } catch (ExecutionException e) {
                    LOG.warn("Couldn't shutdown Rserve on " + host + ":" + port, e);
                    status = 2;
                } catch (InterruptedException e) {
                    LOG.error("Interrupted!", e);
                    Thread.currentThread().interrupt();
                }
                break;
            case validate:
                final boolean connectionIsOk = validate(host, port, username, password);
                System.out.println("Rserve on " + host + ":" + port + " is "
                        + (connectionIsOk ? "UP" : "DOWN"));
                status = connectionIsOk ? 0 : 42;
                break;
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }

        return status;
    }

    /**
     * Make an an XMLConfiguration for RConnectionPool.
     * @param port the port on "localhost" to connect to
     * @return the XMLConfiguration
     * @throws ConfigurationException problem configuring with specified parameters
     */
    public static XMLConfiguration makeConfiguration(
            final int port) throws ConfigurationException {
        return makeConfiguration("localhost", port, null, null);
    }

    /**
     * Make an an XMLConfiguration fo RConnectionPool.
     * @param hostname the hostname to connect to
     * @param port the port on localhost to connect to
     * @return the XMLConfiguration
     * @throws ConfigurationException problem configuring with specified parameters
     */
    public static XMLConfiguration makeConfiguration(
            final String hostname, final int port) throws ConfigurationException {
        return makeConfiguration(hostname, port, null, null);
    }

    /**
     * Make an an XMLConfiguration fo RConnectionPool.
     * @param hostname the hostname to connect to
     * @param port the port on localhost to connect to
     * @param username the username to use for the connection
     * @param password the password to use for the connection
     * @return the XMLConfiguration
     * @throws ConfigurationException problem configuring with specified parameters
     */
    public static XMLConfiguration makeConfiguration(
            final String hostname, final int port,
            final String username, final String password) throws ConfigurationException {

        final StringBuilder xml = new StringBuilder();
        xml.append("<?xml version='1.0' encoding='UTF-8'?>");
        xml.append("<RConnectionPool xsi:noNamespaceSchemaLocation='RConnectionPool.xsd'");
        xml.append(" xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>");
        xml.append("<RConfiguration publish='false'>");
        xml.append("<RServer ");
        xml.append("host='").append(hostname).append("' ");
        xml.append("port='").append(port).append("' ");
        if (username != null) {
            xml.append("username='").append(username).append("' ");
        }
        if (password != null) {
            xml.append("password='").append(password).append("' ");
        }
        xml.append("/>");
        xml.append("</RConfiguration>");
        xml.append("</RConnectionPool>");

        final XMLConfiguration configuration = new XMLConfiguration();
        configuration.load(new StringReader(xml.toString()));
        return configuration;
    }
}
