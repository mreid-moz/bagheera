/*
 * Copyright 2011 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.bagheera.hazelcast.persistence;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.core.MapStore;

/**
 * A repository that MapStore implementations can register with. 
 * Note**: Currently this is used for implementations that need to be closed 
 * via REST calls on a periodic basis.
 */
public class MapStoreRepository {

    private static Map<String,MapStore<String,String>> instances = new HashMap<String,MapStore<String,String>>();

    public static void addMapStore(String mapName, MapStore<String,String> mapStore) {
        instances.put(mapName, mapStore);
    }
    
    public static MapStore<String,String> getMapStore(String mapName) {
        return instances.get(mapName);
    }
    
}
