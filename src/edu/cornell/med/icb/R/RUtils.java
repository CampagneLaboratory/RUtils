/*
 * Copyright (C) 2008 Institute for Computational Biomedicine,
 *                    Weill Medical College of Cornell University
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
import org.rosuda.REngine.Rserve.RSession;
import org.rosuda.REngine.Rserve.RserveException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Class used to start and stop instances of the R server from within the JVM.
 * @see <a href="http://www.rforge.net/Rserve/">Rserve - Binary R server</a>
 */
public class RUtils {
    /**
     * Used to log debug and informational messages.
     */
    private static final Log LOG = LogFactory.getLog(RUtils.class);

    /**
     * An {@link java.util.concurrent.ExecutorService} that can be used to start new threads.
     */
    ExecutorService threadPool = Executors.newCachedThreadPool();

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
    private static final String DEFAULT_RSERVE_COMMAND =
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
     * @param rServeCommand Full path to command used to start Rserve process
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     * @param username Username to send to the server if authentication is required
     * @param password Password to send to the server if authentication is required
     */
    void startup(final String rServeCommand,
                 final String host, final int port,
                 final String username, final String password) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Attempting to start Rserve on " + port);
        }

        final Future<Boolean> server =
                threadPool.submit(new Callable<Boolean>() {
                    public Boolean call() throws IOException {
                        final List<String> commands = new ArrayList<String>();
                        commands.add("ssh");
                        commands.add(host);
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
                                LOG.debug(line);
                            }

                            process.waitFor();
                            LOG.info("Program terminated!");
                        } catch (InterruptedException e) {
                            LOG.error("Interrupted!", e);
                            process.destroy();
                            Thread.currentThread().interrupt();
                        } finally {
                            IOUtils.closeQuietly(br);
                        }

                        return true;
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
    void shutdown(final String host, final int port,
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
     * Can be used to shut down a running rserve instance.
     * @param session the session associated with to the rserve process to shutdown
     * @throws RserveException if the process cannot be shutdown properly
     */
    void shutdown(final RSession session) throws RserveException {
        final RConnection connection = session.attach();
        connection.shutdown();
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

    public static void main(final String[] args)
            throws ParseException, RserveException, ConfigurationException {
        final Options options = new Options();

        final Option helpOption = new Option("h", "help", false, "Print this message");
        options.addOption(helpOption);

        final Option startupOption =
                new Option("startup", "startup", false, "Start Rserve process");
        final Option shutdownOption =
                new Option("shutdown", "shutdown", false, "Shutdown Rserve process");

        final OptionGroup optionGroup = new OptionGroup();
        optionGroup.addOption(startupOption);
        optionGroup.addOption(shutdownOption);
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
        final CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption("h")) {
            usage(options);
        } else {
            final boolean shutdown = commandLine.hasOption("shutdown");
            final RUtils rUtils = new RUtils();

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
                final int numberOfRServers = configuration.getMaxIndex("RServer") + 1;
                for (int i = 0; i < numberOfRServers; i++) {
                    final String server = "RServer(" + i + ")";
                    final String host = configuration.getString(server + "[@host]");
                    final int port = configuration.getInt(server + "[@port]",
                            RConfiguration.DEFAULT_RSERVE_PORT);
                    final String username = configuration.getString(server + "[@username]");
                    final String password = configuration.getString(server + "[@password]");
                    final String command = configuration.getString(server + "[@command]",
                            DEFAULT_RSERVE_COMMAND);

                    try {
                        if (shutdown) {
                            rUtils.shutdown(host, port, username, password);
                        } else {
                            rUtils.startup(command, host, port, username, password);
                        }
                    } catch (RserveException e) {
                        // just let the user know and try the other servers
                        LOG.warn("Couldn't shutdown Rserve on " + host + ":" + port, e);
                    }
                }
            } else {
                final String host = commandLine.getOptionValue("host", "localhost");
                final int port = Integer.valueOf(commandLine.getOptionValue("port", "6311"));
                final String username = commandLine.getOptionValue("username");
                final String password = commandLine.getOptionValue("password");

                if (shutdown) {
                    rUtils.shutdown(host, port, username, password);
                } else {
                    rUtils.startup(DEFAULT_RSERVE_COMMAND, host, port, username, password);
                }
            }
            rUtils.threadPool.shutdown();
        }
    }
}
