/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import {Inject, Injectable} from "@angular/core";
import {
  IResourceMethodPromise,
  Resource,
  ResourceAction,
  ResourceActionReturnType,
  ResourceHandler,
  ResourceParams,
  ResourceRequestMethod
} from "@ngx-resource/core";
import {ISystemInfoResponse} from "../models/systeminfo/ISystemInfoResponse";
import {RES_HANDLER} from "../../../app.config";
import {SharedModule} from "../../common/shared.module";

@Injectable({
  providedIn: 'root'
})
@ResourceParams(
  SharedModule.getResourceSettings('api/node')
)
export class SystemInformationService extends Resource {

  constructor(@Inject(RES_HANDLER) handler: ResourceHandler) {
    super(handler);
  }

  @ResourceAction({
    path: '/system',
    method: ResourceRequestMethod.Get,
    returnAs: ResourceActionReturnType.Promise
  })

  getSystemInfo!: IResourceMethodPromise<void, ISystemInfoResponse>;

}
