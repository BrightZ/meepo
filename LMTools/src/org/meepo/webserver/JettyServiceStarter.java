package org.meepo.webserver;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.meepo.config.Environment;

public class JettyServiceStarter {
	public boolean start() {
		try {
			Server server = new Server(Environment.getMeepoPort());

			ServletContextHandler context = new ServletContextHandler(
					ServletContextHandler.SESSIONS);
			context.setContextPath("/");
			server.setHandler(context);
			MeepoServlet servlet = new MeepoServlet();
			ServletHolder holder = new ServletHolder(servlet);
			holder.setInitParameter("enabledForExtensions", "true");
			context.addServlet(holder, "/*");

			StatisticServlet ss = new StatisticServlet();
			ServletHolder ssHolder = new ServletHolder(ss);
			context.addServlet(ssHolder, "/stat");

			server.setThreadPool(new QueuedThreadPool(1000));

			server.start();
			server.join();
			return true;
		} catch (Exception e) {
			logger.error("Failed to start jetty server.", e);
			return false;
		}
	}

	private Logger logger = Logger.getLogger(this.getClass());
}
