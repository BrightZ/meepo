package org.meepo.client;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.client.XmlRpcCommonsTransportFactory;
import org.apache.xmlrpc.client.util.ClientFactory;
import org.meepo.xmlrpc.AdminRpcInterface;
import org.meepo.xmlrpc.RpcInterface;

public class RpcProxyFactory {
	private RpcProxyFactory() {

	}

	public AdminRpcInterface getMeepoAdminImpl(String urlString) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		config.setEnabledForExtensions(true);
		// config.setGzipCompressing(true);
		// config.setGzipRequesting(true);
		try {
			config.setServerURL(new URL(urlString));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		XmlRpcClient client = new XmlRpcClient();
		client.setTransportFactory(new XmlRpcCommonsTransportFactory(client));
		client.setConfig(config);
		ClientFactory factory = new ClientFactory(client);
		AdminRpcInterface m = (AdminRpcInterface) factory
				.newInstance(AdminRpcInterface.class);
		return m;
	}

	public RpcInterface getMeepoImpl(String urlString) {
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		config.setEnabledForExtensions(true);
		// config.setGzipCompressing(true);
		// config.setGzipRequesting(true);
		try {
			config.setServerURL(new URL(urlString));
		} catch (MalformedURLException e) {
			return null;
		}
		XmlRpcClient client = new XmlRpcClient();
		client.setTransportFactory(new XmlRpcCommonsTransportFactory(client));
		client.setConfig(config);
		ClientFactory factory = new ClientFactory(client);
		RpcInterface m = (RpcInterface) factory.newInstance(RpcInterface.class);
		return m;
	}

	private static RpcProxyFactory instance = new RpcProxyFactory();

	public static RpcProxyFactory getInstance() {
		return instance;
	}

}
