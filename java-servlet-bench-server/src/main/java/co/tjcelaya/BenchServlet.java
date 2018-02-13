package co.tjcelaya;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BenchServlet extends HttpServlet {

    private static final String getResponse = "{\"method\":\"GET\"}";
    private static final String postResponse = "{\"method\":\"POST\"}";

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentLength(getResponse.length());
        final PrintWriter writer = resp.getWriter();
        writer.write(getResponse);
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentLength(postResponse.length());
        final PrintWriter writer = resp.getWriter();
        writer.write(postResponse);
        writer.close();
    }
}
