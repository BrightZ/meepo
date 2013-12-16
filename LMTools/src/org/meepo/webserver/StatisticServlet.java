package org.meepo.webserver;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.meepo.config.Environment;
import org.meepo.fs.PermissionFixer;
import org.meepo.monitor.Statistic;

public class StatisticServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html>");
		out.println("<head>");

		out.println("<title>MeePo Layout Manager Statistics</title>");
		out.println("</head>");
		out.println("<body>");
		out.println("<h1>MeePo Layout Manager Statistics</h1>");

		long total = Statistic.getInstance().getTotalRequestCount();
		long curr = Statistic.getInstance().getCurrentCount();
		long max = Statistic.getInstance().getMaxRequestCount();
		long boot = Statistic.getInstance().getBootTime();
		long gc = Statistic.getInstance().getTokenGCAliveTime();
		long tcgc = Statistic.getInstance().getTrashGCAliveTime();

		Date date = new Date(boot);

		out.println("<h2>Current requests per second(RPS): " + curr + "</h2>");
		out.println("<h2>Max requests per second(RPS): " + max + "</h2>");
		out.println("<h2>Total requests served: " + total + "</h2>");
		// out.println("<h2>Max request time: " +
		// Statistic.getInstance().getMaxRequestTime() + "ms</h2>");

		// Online User Stat
		out.println("<h2>Current online user count: "
				+ +Statistic.getInstance().getCurrentOnlineUserCount()
				+ "</h2>");
		out.println("<h2>Max online user count: "
				+ +Statistic.getInstance().getMaxOnlineUserCount() + "</h2>");

		out.println("<h2>Recycled token count:"
				+ Statistic.getInstance().getRecycledTokenCount() + "</h2>");

		out.println("<h2>System boot time: "
				+ DateFormat.getDateTimeInstance(DateFormat.FULL,
						DateFormat.FULL).format(date) + "</h2>");

		out.println("<h2>Token Collector last active time:"
				+ DateFormat.getDateTimeInstance(DateFormat.FULL,
						DateFormat.FULL).format(new Date(gc)) + "</h2>");
		out.println("<h2>Token cycle round: "
				+ Statistic.getInstance().getTokenCycleRound() + "</h2>");
		out.println("<h2>PermissionFixer task count: "
				+ PermissionFixer.getInstance().getTaskCount() + "</h2>");

		// TrashCleaner stat
		out.println("<h2>TrashCycleRound:"
				+ Statistic.getInstance().getTrashCycleRound() + "</h2>");
		out.println("<h2>TrashGCAliveTime:"
				+ DateFormat.getDateTimeInstance(DateFormat.FULL,
						DateFormat.FULL).format(new Date(tcgc)) + "</h2>");
		out.println("<h2>TrashCleaned (MB):"
				+ Statistic.getInstance().getTrashCleanedByte()
				/ (1000L * 1000L) + "</h2>");

		out.println("<h2>Version:" + Environment.version + "</h2>");

		out.println("</body>");
		out.println("</html>");
	}
}
