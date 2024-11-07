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

import {Pipe, PipeTransform} from '@angular/core';


@Pipe({
  name: 'sizeUnitConverter'
})
export class SizeUnitConverterPipe implements PipeTransform {
  private readonly unitNames: {short:string, long:string}[]= [
    {short: "B", long: "Byte/s"},
    {short: "KB", long: "Kilobyte/s"},
    {short: "MB", long: "Megabyte/s"},
    {short: "GB", long: "Gigabyte/s"},
    {short: "TB", long: "Terabyte/s"},
    {short: "PB", long: "Petabyte/s"},
    {short: "EB", long: "Exabyte/s"},
    {short: "ZB", long: "Zettabyte/s"},
    {short: "YB", long: "Yottabyte/s"}
  ];

  private readonly decimalBase = 100;
  private readonly base = 1024;

  transform(sizeValue: number, isLong: boolean = false): string {

    ;
    let power = Math.round(Math.log(sizeValue) / Math.log(this.base));
    let unit = isLong ? this.unitNames[power].short : this.unitNames[power].long;
    let size = Math.round((sizeValue / Math.pow(this.base, power) ) * this.decimalBase) / this.decimalBase;

    return `${size} ${unit}`;
  }

}
