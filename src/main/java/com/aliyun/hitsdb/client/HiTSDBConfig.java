package com.aliyun.hitsdb.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.Host;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HiTSDBConfig {

	public static final String BASICTYPE = "basic";
	public static final String ALITYPE = "alibaba-signature";
	
	public static class Builder {
		public static volatile boolean ProducerThreadSerializeSwitch = false;

		private int putRequestLimit = -1;
		private boolean putRequestLimitSwitch = true;

		private int batchPutBufferSize = 10000;
		private AbstractBatchPutCallback<?> batchPutCallback;
		private int batchPutConsumerThreadCount = 1;
		private int batchPutRetryCount = 0;
		private int batchPutSize = 500;
		private int batchPutTimeLimit = 300;
		private int maxTPS = -1;

		/**
		 * add by lizhiyang 2019-01-05
		 * 解决http chunked，增加开关类和每次发送内容大小设置
		 */
		private boolean batchPutByContentSizeSwitch = false;
		private int batchPutByContentSize = 4096;

		private String host;
		private int port = 8242;

		private boolean httpCompress = false;
		private int httpConnectionPool = 64; // 每个Host分配的连接数
		private int httpConnectTimeout = 90; // 单位：秒
		private int httpConnectionLiveTime = 0; // 单位：秒
		private int httpKeepaliveTime = -1; // 0 表示短连接。-1表示长连接。单位：秒。

		private int ioThreadCount = 1;
		private boolean backpressure = true;
		private boolean asyncPut = true;
		
		private boolean sslEnable = false;
		private String authType;
		private String instanceId = null;
		private String tsdbUser = null;
		private String basicPwd = null;
		private String certPath = null;
		private byte[] certContent = null;


		private List<Host> addresses = new ArrayList();

		private Set<String> uniqueHost = new HashSet();

		public Builder(String host) {
			this.host = host;
			this.uniqueHost.add(host);
		}

		public Builder(String host, int port) {
			this.host = host;
			this.port = port;
			this.uniqueHost.add(host);
		}

		public Builder() {

		}

		public Builder batchPutByContentSizeSwitch(boolean enable) {
			this.batchPutByContentSizeSwitch = enable;
			return this;
		}

		public Builder batchPutByContentSize(int maxSize) {
			this.batchPutByContentSize = maxSize;
			return this;
		}

		public Builder putRequestLimit(int limit) {
			this.putRequestLimit = limit;
			this.putRequestLimitSwitch = true;
			return this;
		}

		public Builder addAddress(String host,int port){
			String key = host + ":" + port;
			if(uniqueHost.contains(key)){
				return this;
			}
			this.addresses.add(new Host(host,port));
			this.uniqueHost.add(key);
			return this;
		}


		public Builder addAddress(String host) {
			return addAddress(host,port);
		}

		public Builder batchPutBufferSize(int batchPutBufferSize) {
			this.batchPutBufferSize = batchPutBufferSize;
			return this;
		}

		public Builder batchPutConsumerThreadCount(int batchPutConsumerThreadCount) {
			this.batchPutConsumerThreadCount = batchPutConsumerThreadCount;
			return this;
		}

		public Builder batchPutRetryCount(int batchPutRetryCount) {
			this.batchPutRetryCount = batchPutRetryCount;
			return this;
		}

		public Builder batchPutSize(int batchPutSize) {
			this.batchPutSize = batchPutSize;
			return this;
		}

		public Builder batchPutTimeLimit(int batchPutTimeLimit) {
			this.batchPutTimeLimit = batchPutTimeLimit;
			return this;
		}

		public Builder closePutRequestLimit() {
			this.putRequestLimitSwitch = false;
			return this;
		}

		public Builder closeBackpressure() {
			this.backpressure = false;
			return this;
		}

		public Builder backpressure(boolean backpressure) {
			this.backpressure = backpressure;
			return this;
		}

		public Builder httpConnectionLiveTime(int httpConnectionLiveTime) {
			this.httpConnectionLiveTime = httpConnectionLiveTime;
			return this;
		}

		public Builder httpKeepaliveTime(int httpKeepaliveTime) {
			this.httpKeepaliveTime = httpKeepaliveTime;
			return this;
		}

		public Builder readonly() {
			this.asyncPut = false;
			return this;
		}

		public Builder readonly(boolean readonly) {
			if (readonly) {
				this.asyncPut = false;
			}
			return this;
		}

		public Builder asyncPut(boolean asyncPut) {
			this.asyncPut = asyncPut;
			return this;
		}
		
		public Builder maxTPS(int maxTPS) {
			this.maxTPS = maxTPS;
			return this;
		}

		public HiTSDBConfig config() {
			HiTSDBConfig hiTSDBConfig = new HiTSDBConfig();

			hiTSDBConfig.host = this.host;
			hiTSDBConfig.port = this.port;
			hiTSDBConfig.batchPutCallback = this.batchPutCallback;
			hiTSDBConfig.batchPutSize = this.batchPutSize;
			hiTSDBConfig.batchPutTimeLimit = this.batchPutTimeLimit;
			hiTSDBConfig.batchPutBufferSize = this.batchPutBufferSize;
			hiTSDBConfig.batchPutRetryCount = this.batchPutRetryCount;
			hiTSDBConfig.httpConnectionPool = this.httpConnectionPool;
			hiTSDBConfig.httpConnectTimeout = this.httpConnectTimeout;
			hiTSDBConfig.putRequestLimitSwitch = this.putRequestLimitSwitch;
			hiTSDBConfig.putRequestLimit = this.putRequestLimit;
			hiTSDBConfig.batchPutConsumerThreadCount = this.batchPutConsumerThreadCount;
			hiTSDBConfig.httpCompress = this.httpCompress;
			hiTSDBConfig.ioThreadCount = this.ioThreadCount;
			hiTSDBConfig.backpressure = this.backpressure;
			hiTSDBConfig.httpConnectionLiveTime = this.httpConnectionLiveTime;
			hiTSDBConfig.httpKeepaliveTime = this.httpKeepaliveTime;
			hiTSDBConfig.maxTPS = this.maxTPS;
			hiTSDBConfig.asyncPut = this.asyncPut;
			hiTSDBConfig.batchPutByContentSizeSwitch = this.batchPutByContentSizeSwitch;
			hiTSDBConfig.batchPutByContentSize = this.batchPutByContentSize;

			hiTSDBConfig.addresses = this.addresses;
			if (this.putRequestLimitSwitch && this.putRequestLimit <= 0) {
				hiTSDBConfig.putRequestLimit = this.httpConnectionPool;
			}
			hiTSDBConfig.sslEnable = this.sslEnable;
			hiTSDBConfig.authType = this.authType;
			hiTSDBConfig.instanceId = this.instanceId;
			hiTSDBConfig.tsdbUser = this.tsdbUser;
			hiTSDBConfig.basicPwd = this.basicPwd;
			hiTSDBConfig.certContent = this.certContent;
			return hiTSDBConfig;
		}
		
		public Builder enableSSL(boolean sslEnable) {
			this.sslEnable = sslEnable;
			return this;
		}

		public Builder basicAuth(String instanceId, String tsdbUser, String basicPwd) {
			this.authType = HiTSDBConfig.BASICTYPE;
			this.instanceId = instanceId;
			this.tsdbUser = tsdbUser;
			this.basicPwd = basicPwd;
			return this;
		}
		
		public Builder aliAuth(String instanceId, String tsdbUser, String aliAuthPath) {
			this.authType = HiTSDBConfig.ALITYPE;
			this.instanceId = instanceId;
			this.tsdbUser = tsdbUser;
			this.certPath = aliAuthPath;
			File file = new File(certPath);
			if (!file.exists()) {
				throw new HttpClientInitException();
			}
			try {
				InputStream is = new FileInputStream(file);
				certContent = new byte[is.available()];
				int i = is.read(certContent);
				if (certContent.length == 0) {
					throw new HttpClientInitException();
				}
			} catch (FileNotFoundException e) {
				throw new HttpClientInitException();
			} catch (IOException e) {
				throw new HttpClientInitException();
			} catch (Exception e) {
				throw new HttpClientInitException();
			}
			return this;
		}

		public Builder httpCompress(boolean httpCompress) {
			this.httpCompress = httpCompress;
			return this;
		}

		public Builder httpConnectionPool(int connectionPool) {
			if (connectionPool <= 0) {
				throw new IllegalArgumentException("The ConnectionPool con't be less then 1");
			}
			httpConnectionPool = connectionPool;
			return this;
		}

		public Builder httpConnectTimeout(int httpConnectTimeout) {
			this.httpConnectTimeout = httpConnectTimeout;
			return this;
		}

		public Builder ioThreadCount(int ioThreadCount) {
			this.ioThreadCount = ioThreadCount;
			return this;
		}

		public Builder listenBatchPut(AbstractBatchPutCallback<?> cb) {
			this.batchPutCallback = cb;
			return this;
		}

		public Builder openHttpCompress() {
			this.httpCompress = true;
			return this;
		}

	}


	public static Builder address(String host) {
		return new Builder(host);
	}

	public static Builder address(String host, int port) {
		return new Builder(host, port);
	}

	/**
	 * 写入请求限制数
	 */
	public static Builder builder() {
		return new Builder();
	}

	private int putRequestLimit;
	/**
	 * 写入请求限制开关，true表示打开请求限制
	 */
	private boolean putRequestLimitSwitch;

	private int batchPutBufferSize;

    /**
     * 异步批量写回调接口
     */
	private AbstractBatchPutCallback<?> batchPutCallback;
	/**
	 *
	 */
	private int batchPutConsumerThreadCount;
	/**
	 *
	 */
	private int batchPutRetryCount;

	private int batchPutSize;
	/**
	 * 批量Put时从队列取数据时最长等待时间
	 */
	private int batchPutTimeLimit;

	private int maxTPS;

	private boolean batchPutByContentSizeSwitch;
	private int batchPutByContentSize;
	
	private String host;
	
	private boolean httpCompress;
	private int httpConnectionPool;
	private int httpConnectTimeout;
	private int httpConnectionLiveTime;
	private int httpKeepaliveTime;

	private int ioThreadCount;
	private boolean backpressure;
	private boolean asyncPut;

	private int port;
	
	/**
	 * is https enable
	 */
	private boolean sslEnable;
	
	private String authType;
	
	private String instanceId;
	
	private String tsdbUser;
	
	private String basicPwd;
	
	private byte[] certContent;
	
	public boolean isSslEnable() {
		return sslEnable;
	}

	public String getAuthType() {
		return authType;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public String getTsdbUser() {
		return tsdbUser;
	}

	public String getBasicPwd() {
		return basicPwd;
	}

	public byte[] getCertContent() {
		return certContent;
	}

	private List<Host> addresses = new ArrayList();

	public int getPutRequestLimit() {
		return putRequestLimit;
	}

	public int getBatchPutBufferSize() {
		return batchPutBufferSize;
	}

	public AbstractBatchPutCallback<?> getBatchPutCallback() {
		return batchPutCallback;
	}

	public int getBatchPutConsumerThreadCount() {
		return batchPutConsumerThreadCount;
	}

	public int getBatchPutRetryCount() {
		return batchPutRetryCount;
	}

	public int getBatchPutSize() {
		return batchPutSize;
	}

	public int getBatchPutTimeLimit() {
		return batchPutTimeLimit;
	}

	public String getHost() {
		return host;
	}


	public List<Host> getAddresses(){
		return this.addresses;
	}

	public int getHttpConnectionPool() {
		return httpConnectionPool;
	}

	public int getHttpConnectTimeout() {
		return httpConnectTimeout;
	}

	public int getIoThreadCount() {
		return ioThreadCount;
	}

	public int getPort() {
		return port;
	}

	public boolean isPutRequestLimitSwitch() {
		return putRequestLimitSwitch;
	}

	public boolean isHttpCompress() {
		return httpCompress;
	}

	public boolean isBackpressure() {
		return backpressure;
	}

	public int getHttpConnectionLiveTime() {
		return httpConnectionLiveTime;
	}

	public int getHttpKeepaliveTime() {
		return httpKeepaliveTime;
	}

	public boolean isAsyncPut() {
		return asyncPut;
	}

	public int getMaxTPS() {
		return maxTPS;
	}

	public void setBatchPutCallback(AbstractBatchPutCallback callback){
		this.batchPutCallback = callback;
	}

	public boolean isBatchPutByContentSizeSwitch() {
		return batchPutByContentSizeSwitch;
	}

	public int getBatchPutByContentSize() {
		return batchPutByContentSize;
	}

	public HiTSDBConfig copy(String host, int port){
		HiTSDBConfig hiTSDBConfig = new HiTSDBConfig();
		hiTSDBConfig.host = host;
		hiTSDBConfig.port = port;
		hiTSDBConfig.batchPutCallback = this.batchPutCallback;
		hiTSDBConfig.batchPutSize = this.batchPutSize;
		hiTSDBConfig.batchPutTimeLimit = this.batchPutTimeLimit;
		hiTSDBConfig.batchPutBufferSize = this.batchPutBufferSize;
		hiTSDBConfig.batchPutRetryCount = this.batchPutRetryCount;
		hiTSDBConfig.httpConnectionPool = this.httpConnectionPool;
		hiTSDBConfig.httpConnectTimeout = this.httpConnectTimeout;
		hiTSDBConfig.putRequestLimitSwitch = this.putRequestLimitSwitch;
		hiTSDBConfig.putRequestLimit = this.putRequestLimit;
		hiTSDBConfig.batchPutConsumerThreadCount = this.batchPutConsumerThreadCount;
		hiTSDBConfig.httpCompress = this.httpCompress;
		hiTSDBConfig.ioThreadCount = this.ioThreadCount;
		hiTSDBConfig.backpressure = this.backpressure;
		hiTSDBConfig.httpConnectionLiveTime = this.httpConnectionLiveTime;
		hiTSDBConfig.httpKeepaliveTime = this.httpKeepaliveTime;
		hiTSDBConfig.maxTPS = this.maxTPS;
		hiTSDBConfig.asyncPut = this.asyncPut;
		if (this.putRequestLimitSwitch && this.putRequestLimit <= 0) {
			hiTSDBConfig.putRequestLimit = this.httpConnectionPool;
		}
		hiTSDBConfig.batchPutByContentSizeSwitch = this.batchPutByContentSizeSwitch;
		hiTSDBConfig.batchPutByContentSize = this.batchPutByContentSize;
		return hiTSDBConfig;
	}

}