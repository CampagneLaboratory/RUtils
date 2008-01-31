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

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.ParseException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;
import org.rosuda.REngine.Rserve.RSession;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;

/**
 * Class used to start instances of the R server from within the JVM.
 * @see <a href="http://www.rforge.net/Rserve/">Rserve - Binary R server</a>
 */
class RUtils {
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
            System.getProperty("RSERVE_COMMAND",
                    SystemUtils.IS_OS_WINDOWS ? DEFAULT_RSERVE_EXECUTABLE_WINDOWS : DEFAULT_RSERVE_EXECUTABLE);

    /**
     * Name of Rserve executable.  This should be a fully qualified path unless it
     * is located on the execution path.
     */
    private String rServeCommand = "C:\\Program Files\\R\\R-2.6.0\\library\\Rserve\\" + DEFAULT_RSERVE_COMMAND;

    RUtils() {
        super();
    }

    void startup() throws IOException {
        final Future<Boolean> server =
                threadPool.submit(new Callable<Boolean>() {
                    public Boolean call() throws Exception {
                        final String[] command = {
                                rServeCommand
                        };
                        final ProcessBuilder builder = new ProcessBuilder(command);
                        builder.redirectErrorStream(true);
                        final Process process = builder.start();
                        try {
                            process.waitFor();
                            LOG.info("Program terminated!");
                        } catch (InterruptedException e) {
                            LOG.error("Interrupted!", e);
                            process.destroy();
                            Thread.currentThread().interrupt();
                        }

                        return true;
                    }
                });

    }

    /**
     * Can be used to shut down a running rserve instance.
     * @param host Host where the command should be sent
     * @param port Port number where the command should be sent
     * @throws RserveException if the process cannot be shutdown properly
     */
    void shutdown(final String host, final int port) throws RserveException {
        if (LOG.isInfoEnabled()) {
            LOG.info("Attempting to shutdown Rserve on " + host + ":" + port);
        }

        final RConnection rConnection = new RConnection(host, port);
        if (rConnection.needLogin()) {
            // TODO: send login info
            rConnection.login("", "");
        }
        rConnection.shutdown();

        if (LOG.isInfoEnabled()) {
            LOG.info("Shutdown message sent");
        }
    }

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

    public static void main(final String[] args) throws ParseException, RserveException {
        final Options options = new Options();

        final Option helpOption = new Option("h", "help", false, "Print this message");
        options.addOption(helpOption);

        final Option shutdownOption =
                new Option("s", "shutdown", false, "Shutdown a running Rserve process");
        options.addOption(shutdownOption);

        final Option portOption =
                new Option("port", "port", true, "Use specified port to communicate with the Rserve process");
        portOption.setArgName("port");
        portOption.setType(int.class);
        options.addOption(portOption);

        final Option hostOption =
                new Option("host", "host", true, "communicate with the Rserve process on the given host");
        hostOption.setArgName("hostname");
        hostOption.setType(String.class);
        options.addOption(hostOption);

        final Parser parser = new BasicParser();
        final CommandLine commandLine = parser.parse(options, args);

        final int port = Integer.valueOf(commandLine.getOptionValue("port", "6311"));
        final String host = commandLine.getOptionValue("host", "localhost");

        if (commandLine.hasOption("h")) {
            usage(options);
        } else if (commandLine.hasOption("shutdown")) {
            final RUtils rUtils = new RUtils();
            rUtils.shutdown(host, port);
        } else {
            usage(options);
        }
    }
}
