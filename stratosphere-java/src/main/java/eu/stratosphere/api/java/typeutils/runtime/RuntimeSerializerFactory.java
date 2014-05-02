/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.java.typeutils.runtime;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.InstantiationUtil;

public final class RuntimeSerializerFactory<T> implements TypeSerializerFactory<T> {

	private static final String CONFIG_KEY_SER = "SER_DATA";

	private static final String CONFIG_KEY_CLASS = "CLASS_DATA";

	private byte[] serializerData;
	
	private ClassLoader loader;
	
	private TypeSerializer<T> serializer;

	private Class<T> clazz;


	public RuntimeSerializerFactory() {}

	public RuntimeSerializerFactory(TypeSerializer<T> serializer, Class<T> clazz) {
		this.clazz = clazz;
		this.serializer = serializer;
		
		try {
			this.serializerData = InstantiationUtil.serializeObject(serializer);
		} catch (IOException e) {
			throw new RuntimeException("Cannt serialize the Serializer.", e);
		}
	}


	@Override
	public void writeParametersToConfig(Configuration config) {
		try {
			InstantiationUtil.writeObjectToConfig(clazz, config, CONFIG_KEY_CLASS);
			config.setBytes(CONFIG_KEY_SER, this.serializerData);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not serialize serializer into the configuration.", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
		if (config == null || cl == null) {
			throw new NullPointerException();
		}
		
		this.serializerData = config.getBytes(CONFIG_KEY_SER, null);
		if (this.serializerData == null) {
			throw new RuntimeException("Could not find deserializer in the configuration."); 
		}
		
		this.loader = cl;
		
		try {
			this.clazz = (Class<T>) InstantiationUtil.readObjectFromConfig(config, CONFIG_KEY_CLASS, cl);
			this.serializer = (TypeSerializer<T>) InstantiationUtil.deserializeObject(this.serializerData, cl);
		}
		catch (ClassNotFoundException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException("Could not load deserializer from the configuration.", e);
		}
		

	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeSerializer<T> getSerializer() {
		if (serializerData != null) {
			try {
				return (TypeSerializer<T>) InstantiationUtil.deserializeObject(this.serializerData, loader);
			} catch (Exception e) {
				throw new RuntimeException("Repeated instantiation of serializer failed.", e);
			}
		} else {
			throw new RuntimeException("SerializerFactory has not been initialized from configuration.");
		}
	}

	@Override
	public Class<T> getDataType() {
		return clazz;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if(obj == null){
			return false;
		}
		
		if(!(obj instanceof RuntimeSerializerFactory)){
			return false;
		}
		
		RuntimeSerializerFactory<T> otherRSF = (RuntimeSerializerFactory<T>) obj;
		return otherRSF.clazz == this.clazz && otherRSF.serializer.equals(serializer);
	}
}
