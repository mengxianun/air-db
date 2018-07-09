package com.mxy.air.db.es;

import java.io.IOException;
import java.util.HashMap;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.mxy.air.db.AirContext;
import com.mxy.air.json.JSON;
import com.mxy.air.json.JSONObject;

public class EsHandler {

	public JSON handle(String db, String table, JSONObject nativeQuery) throws IOException {
		Injector injector = AirContext.getInjector();
		RestClient client = injector.getInstance(Key.get(RestClient.class, Names.named(db)));

		//		if (!nativeQuery.containsKey("from")) {
		//			nativeQuery.put("from", 0);
		//		}
		//		if (!nativeQuery.containsKey("size")) {
		//			nativeQuery.put("size", 20);
		//		}

		NStringEntity nStringEntity = new NStringEntity(nativeQuery.toString(), ContentType.APPLICATION_JSON);
		Response response = client.performRequest("GET", "/" + table + "/_search", new HashMap<>(),
				nStringEntity);

		//		RequestLine requestLine = response.getRequestLine();
		//		HttpHost host = response.getHost();
		//		int statusCode = response.getStatusLine().getStatusCode();
		//		Header[] headers = response.getHeaders();
		String responseBody = EntityUtils.toString(response.getEntity());
		JSONObject responseObject = new JSONObject(responseBody);
		return responseObject;
		//		JSONObject hits = responseObject.getObject("hits");
		//		long total = hits.getLong("total");
		//		JSONArray objs = new JSONArray();
		//		if (responseObject.containsKey("aggregations")) {
		//			JSONObject aggs = (JSONObject) responseObject.getObject("aggregations").entrySet().iterator().next()
		//					.getValue();
		//			objs = aggs.getArray("buckets");
		//		} else {
		//			JSONArray hitsObjs = hits.getArray("hits");
		//			Iterator<Object> iterator = hitsObjs.iterator();
		//			while (iterator.hasNext()) {
		//				JSONObject entity = (JSONObject) iterator.next();
		//				objs.add(entity.getObject("_source"));
		//			}
		//		}
		//		if (nativeQuery.containsKey("from") && nativeQuery.containsKey("size")) {
		//			return PageResult.wrap(nativeQuery.getLong("from"), nativeQuery.getLong("size"), total, objs);
		//		}
		//		return objs;
		
	}

}
