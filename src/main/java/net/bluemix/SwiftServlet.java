package net.bluemix;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.inject.Module;

import org.apache.commons.codec.binary.Base64;
import org.jclouds.ContextBuilder;
import org.jclouds.io.Payload;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.Container;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.openstack.swift.v1.options.CreateContainerOptions;
import org.jclouds.openstack.swift.v1.options.PutOptions;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import static com.google.common.io.ByteSource.wrap;
import static org.jclouds.io.Payloads.newByteSourcePayload;

/**
 * Servlet implementation class SwiftServlet
 */
public class SwiftServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public static final String CONTAINER_NAME = "bluemix-objectstorage-v2-example";
	public static final String OBJECT_NAME = "bluemix-objectstorage-v2-example.txt";
	
	private SwiftApi swiftApi;
	private Map<String, List<Map<String,Object>>> vcap_map;
	public String region;

    /**
     * Default constructor. 
     */
    public SwiftServlet() {
    	Iterable<Module> modules = ImmutableSet.<Module>of(
		            new SLF4JLoggingModule());

    	Map<String, String> env = System.getenv();
		Map<String, Object> cloudIntegration_map;

    	String vcap = env.get("VCAP_SERVICES");
    	ObjectMapper om = new ObjectMapper();

		try {
			vcap_map = om.readValue(vcap,Map.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
				
		List<Map<String,Object>> list = vcap_map.get("Object Storage");
		Map<String, Object> objectStorage = list.get(0);
		Map<String, Object> credentials = (Map<String, Object>)objectStorage.get("credentials");
		String url = (String)credentials.get("auth_url");
		String vcap_secret = "Basic " + 
				new String (Base64.encodeBase64( (credentials.get("username") + ":" + credentials.get("password")).getBytes()));
		URL obj;
		try {
			obj = new URL(url);		
			HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty ("Authorization", vcap_secret);
			
			InputStream in = con.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String cloudIntegration = reader.readLine();
			om = new ObjectMapper();
			cloudIntegration_map = om.readValue(cloudIntegration,Map.class);
			
			String provider = "openstack-swift";
			Map<String, Object> cloudIntegration_details = (Map<String, Object>)cloudIntegration_map.get("CloudIntegration");
			Map<String, Object> cloudIntegration_credentials = (Map<String, Object>)cloudIntegration_details.get("credentials");
			String endpoint   = (String) cloudIntegration_details.get("auth_url") + "/v2.0";
			String tenantName = (String) cloudIntegration_details.get("project");
			String userName   = (String) cloudIntegration_credentials.get("userid");
			String password   = (String) cloudIntegration_credentials.get("password");
			String identity = tenantName+":"+userName;
			region =  (String) cloudIntegration_details.get("region");

			swiftApi = ContextBuilder.newBuilder(provider)
			        .endpoint(endpoint)
			        .credentials(identity, password)
			        .modules(modules)
			        .buildApi(SwiftApi.class);
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ProtocolException e) {
			e.printStackTrace();
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		response.getWriter().println("In SwiftServlet");

	    SwiftServlet swiftservlet = new SwiftServlet();

	      try {
	    	 swiftservlet.createContainer();
	    	 swiftservlet.uploadObjectFromString();
	    	 swiftservlet.listContainers(response);
	    	 swiftservlet.retrieveObjects(response);
	    	 swiftservlet.close();
	      } catch (Exception e) {
	         e.printStackTrace();
	         response.getWriter().println(e);
	         for (int i = 0; i < e.getStackTrace().length; i++) {
	        	 response.getWriter().println(e.getStackTrace()[i]);
			} 
	      } finally {
	    	 swiftservlet.close();
	      }
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}


	private void createContainer() {
		System.out.println("Create Container");

		ContainerApi containerApi = swiftApi.getContainerApi(region);
		CreateContainerOptions options = CreateContainerOptions.Builder
				.metadata(ImmutableMap.of(
						"key1", "value1",
						"key2", "value2"));

		containerApi.create(CONTAINER_NAME, options);

		System.out.println("  " + CONTAINER_NAME);
	}

	private void uploadObjectFromString() {
		System.out.println("Upload Object From String");

		ObjectApi objectApi = swiftApi.getObjectApi(region, CONTAINER_NAME);
		Payload payload = newByteSourcePayload(wrap("Hello World".getBytes()));

		objectApi.put(OBJECT_NAME, payload, PutOptions.Builder.metadata(ImmutableMap.of("key1", "value1")));

		System.out.println("  " + OBJECT_NAME);
	}

	private void listContainers(HttpServletResponse response) throws IOException {
		System.out.println("List Containers");

		ContainerApi containerApi = swiftApi.getContainerApi(region);
		Set<Container> containers = containerApi.list().toSet();
		
		response.getWriter().println("Listing Containers: ");
		
		for (Container container : containers) {
			response.getWriter().println("  " + container);
		}
		response.getWriter().println(" ");
	}
	
	private void retrieveObjects(HttpServletResponse response) throws IOException {
		System.out.println("Upload Object From String");

		ObjectApi objectApi = swiftApi.getObjectApi(region, CONTAINER_NAME);

		SwiftObject swiftObject = objectApi.get(OBJECT_NAME);
		response.getWriter().println("Retrieved following object: ");
		response.getWriter().println("   Object: "+ swiftObject);
		response.getWriter().println("   Payload: "+ swiftObject.getPayload());
	}

	public void close() throws IOException {
		Closeables.close(swiftApi, true);
	}
}
