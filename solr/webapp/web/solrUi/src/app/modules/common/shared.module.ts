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

import {NgModule} from '@angular/core';
import {SizeUnitConverterPipe} from "./pipes/sizeconverter/size-unit-converter-pipe.pipe";
import {TimeAgoPipe} from "./pipes/timeconverter/time-ago.pipe";
import {CommonModule, NgClass, NgFor, NgIf} from "@angular/common";
import {IResourceParams} from "@ngx-resource/core";
import {environment} from "../../../environments/environment";


@NgModule({
  declarations: [SizeUnitConverterPipe, TimeAgoPipe],
  imports: [CommonModule, NgClass, NgFor, NgIf],
  exports: [SizeUnitConverterPipe, TimeAgoPipe, NgClass, NgFor, NgIf],
})
export class SharedModule {

  static  isOdd(id: number):boolean {
    return (id & 1) !== 0
  }

  static getResourceSettings(path: string): IResourceParams{
    if(environment.production){
      return {
        url: environment.apiUrl,
        pathPrefix: path
      }
    }
    return {
      url: environment.apiUrl,
      pathPrefix: path
    }
  }
}
