/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {ApplicationConfig, InjectionToken} from '@angular/core';
import {provideRouter} from '@angular/router';

import {routes} from './app.routes';
import {HttpClient, provideHttpClient, withFetch} from "@angular/common/http";
import {ResourceHandlerHttpClient} from "@ngx-resource/handler-ngx-http";
import {ResourceHandler} from "@ngx-resource/core";

export const RES_HANDLER = new InjectionToken<ResourceHandler>("RES_HANDLER");

export function createResourceHandler(httpClient:HttpClient): ResourceHandler {
  return  new ResourceHandlerHttpClient(httpClient);
}

export const appConfig: ApplicationConfig = {
  providers: [provideRouter(routes), provideHttpClient(withFetch()), { provide: RES_HANDLER, useFactory: createResourceHandler, deps:[HttpClient]}]
};
